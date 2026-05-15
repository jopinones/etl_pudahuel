"""
Operaciones de base de datos MySQL para el ETL SII.

Flujo de carga diferencial con tablas staging:
  1. truncate_staging()      → limpia la tabla stg_* antes del archivo
  2. bulk_insert_staging()   → carga bruta de chunks al staging (sin validar duplicados)
  3. upsert_from_staging()   → compara staging vs producción por row_hash:
       - INSERT filas que no existen en producción
       - UPDATE filas cuyo hash cambió (= algún campo fue modificado)
       - SKIP filas idénticas (hash igual)
"""

from __future__ import annotations

import logging
from contextlib import contextmanager
from datetime import datetime
from typing import Optional

import mysql.connector
import pandas as pd

from config import DB_CONFIG

logger = logging.getLogger(__name__)


@contextmanager
def get_connection():
    conn = mysql.connector.connect(**DB_CONFIG)
    try:
        yield conn
    finally:
        conn.close()


def execute_sql_file(sql_path: str) -> None:
    """Ejecuta un archivo SQL completo (para setup del esquema)."""
    with open(sql_path, "r", encoding="utf-8") as f:
        sql = f.read()
    with get_connection() as conn:
        cursor = conn.cursor()
        for statement in sql.split(";"):
            stmt = statement.strip()
            if stmt:
                try:
                    cursor.execute(stmt)
                except mysql.connector.Error as e:
                    logger.warning("SQL ignorado: %s | %s", stmt[:60], e)
        conn.commit()
        cursor.close()


# ---------------------------------------------------------------------------
# Operaciones de staging
# ---------------------------------------------------------------------------

def truncate_staging(conn, stg_table: str) -> None:
    """Vacía la tabla staging antes de cargar un nuevo archivo."""
    cursor = conn.cursor()
    cursor.execute(f"TRUNCATE TABLE `{stg_table}`")
    conn.commit()
    cursor.close()
    logger.debug("TRUNCATE %s", stg_table)


def bulk_insert_staging(
    conn,
    stg_table: str,
    df: pd.DataFrame,
    batch_size: int = 10_000,
) -> None:
    """
    Inserta filas en la tabla staging sin verificar duplicados (carga bruta).
    Sin IGNORE: si algo falla se propaga inmediatamente para detectar errores de estructura.
    """
    if df.empty:
        return
    df = df.where(pd.notna(df), None)
    cols = list(df.columns)
    col_list = ", ".join(f"`{c}`" for c in cols)
    placeholders = ", ".join(["%s"] * len(cols))
    sql = f"INSERT INTO `{stg_table}` ({col_list}) VALUES ({placeholders})"

    cursor = conn.cursor()
    rows = [tuple(r) for r in df.itertuples(index=False, name=None)]
    for i in range(0, len(rows), batch_size):
        cursor.executemany(sql, rows[i : i + batch_size])
    conn.commit()
    cursor.close()


def upsert_from_staging(
    conn,
    table: str,
    stg_table: str,
    key_cols: list[str],
) -> tuple[int, int]:
    """
    Transfiere datos desde staging a producción de forma diferencial.

    Lógica:
      INSERT: filas en staging sin clave correspondiente en producción  (registros nuevos)
      UPDATE: filas con clave coincidente pero row_hash distinto          (registros modificados)
      SKIP:   filas con clave y hash idénticos                           (sin cambios)

    Retorna (insertados, actualizados).
    """
    # Obtener columnas desde DESCRIBE staging (orden canónico)
    cursor = conn.cursor()
    cursor.execute(f"DESCRIBE `{stg_table}`")
    stg_cols = [row[0] for row in cursor.fetchall()]
    cursor.close()

    if not stg_cols:
        return 0, 0

    join_cond = " AND ".join(f"s.`{c}` = p.`{c}`" for c in key_cols)

    # ── INSERT registros nuevos ─────────────────────────────────────────────
    col_list   = ", ".join(f"`{c}`" for c in stg_cols)
    select_stg = ", ".join(f"s.`{c}`" for c in stg_cols)

    insert_sql = f"""
        INSERT INTO `{table}` ({col_list})
        SELECT {select_stg}
        FROM `{stg_table}` s
        LEFT JOIN `{table}` p ON {join_cond}
        WHERE p.id IS NULL
    """

    # ── UPDATE registros modificados ────────────────────────────────────────
    update_cols = [c for c in stg_cols if c not in key_cols]
    set_clause  = ", ".join(f"p.`{c}` = s.`{c}`" for c in update_cols)

    update_sql = f"""
        UPDATE `{table}` p
        INNER JOIN `{stg_table}` s ON {join_cond}
        SET {set_clause}, p.fecha_carga = NOW()
        WHERE s.row_hash != p.row_hash
           OR p.row_hash IS NULL
    """

    cursor = conn.cursor()

    cursor.execute(insert_sql)
    inserted = cursor.rowcount
    conn.commit()

    cursor.execute(update_sql)
    updated = cursor.rowcount
    conn.commit()

    cursor.close()
    logger.debug("%s → %d insertados, %d actualizados", table, inserted, updated)
    return inserted, updated


# ---------------------------------------------------------------------------
# Log de cargas
# ---------------------------------------------------------------------------

def log_inicio(conn, nombre_archivo: str, tipo_archivo: str,
               codigo_comuna: Optional[str], anio: Optional[int],
               semestre: Optional[int], es_nacional: bool) -> int:
    sql = """
        INSERT INTO carga_log
            (nombre_archivo, tipo_archivo, codigo_comuna, anio, semestre,
             es_nacional, estado, inicio_carga)
        VALUES (%s, %s, %s, %s, %s, %s, 'INICIADO', %s)
    """
    cursor = conn.cursor()
    cursor.execute(sql, (
        nombre_archivo, tipo_archivo, codigo_comuna,
        anio, semestre, int(es_nacional), datetime.now(),
    ))
    conn.commit()
    log_id = cursor.lastrowid
    cursor.close()
    return log_id


def log_fin(conn, log_id: int,
            registros_leidos: int,
            registros_insertados: int,
            registros_actualizados: int,
            registros_ignorados: int,
            estado: str = "COMPLETADO",
            mensaje_error: Optional[str] = None) -> None:
    sql = """
        UPDATE carga_log SET
            registros_leidos        = %s,
            registros_insertados    = %s,
            registros_actualizados  = %s,
            registros_ignorados     = %s,
            estado                  = %s,
            mensaje_error           = %s,
            fin_carga               = %s
        WHERE id = %s
    """
    cursor = conn.cursor()
    cursor.execute(sql, (
        registros_leidos, registros_insertados, registros_actualizados,
        registros_ignorados, estado, mensaje_error, datetime.now(), log_id,
    ))
    conn.commit()
    cursor.close()


def archivo_ya_procesado(conn, nombre_archivo: str) -> bool:
    """True si el archivo fue cargado con éxito y sin cambios detectados."""
    sql = """
        SELECT COUNT(*) FROM carga_log
        WHERE nombre_archivo = %s AND estado = 'COMPLETADO'
    """
    cursor = conn.cursor()
    cursor.execute(sql, (nombre_archivo,))
    count = cursor.fetchone()[0]
    cursor.close()
    return count > 0
