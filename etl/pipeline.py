"""
Descubrimiento de archivos y orquestación del pipeline ETL.

Flujo por archivo:
  1. Descubrir archivos en DATA_BASE_PATH (automático por nombre)
  2. TRUNCATE tabla staging correspondiente
  3. Cargar todos los chunks del archivo → staging (INSERT bruto, rápido)
  4. upsert_from_staging → producción:
       INSERT nuevos  (clave no existe en producción)
       UPDATE cambiados (clave existe pero row_hash difiere)
       SKIP sin cambios (hash idéntico)
  5. Registrar resultado en carga_log
"""

from __future__ import annotations

import logging
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator, Optional

from tqdm import tqdm

from config import COMMUNES, DATA_BASE_PATH, TABLE_META
from etl import parsers
from etl.db import (
    archivo_ya_procesado,
    bulk_insert_staging,
    get_connection,
    log_fin,
    log_inicio,
    truncate_staging,
    upsert_from_staging,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Descripción de un archivo descubierto
# ---------------------------------------------------------------------------

@dataclass
class ArchivoSII:
    path: Path
    tipo_tabla: str
    anio: int
    semestre: int
    es_nacional: bool
    codigo_comuna: Optional[str] = None


# ---------------------------------------------------------------------------
# Patrones de nombre de archivo
# ---------------------------------------------------------------------------

_BRORGA_PATTERN = re.compile(
    r"BRORGA2441(A|AL|N|NL)_NAC_(\d{4})_(\d)", re.IGNORECASE
)
_NACROL_PATTERN = re.compile(
    r"BRTMPNACROL_NAC_(\d{4})_(\d)", re.IGNORECASE
)
_CATAS_PATTERN = re.compile(
    r"BRTMPCATAS(A|AL|N|NL)_(\d{4})_(\d)_(\d+)", re.IGNORECASE
)
_ROLSEM_PATTERN = re.compile(
    r"BRTMPROLSEM_(\d{4})_(\d)_(\d+)\.TXT", re.IGNORECASE
)

_SUFIJO_A_TABLA = {
    "A":  "roles_agricolas",
    "AL": "detalle_agricola",
    "N":  "roles_no_agricolas",
    "NL": "detalle_no_agricola",
}


def _descubrir_archivos(base_path: Path) -> Iterator[ArchivoSII]:
    for item in base_path.rglob("*"):
        if not item.is_file():
            continue
        name = item.name

        m = _BRORGA_PATTERN.match(name)
        if m:
            sufijo, anio, sem = m.group(1).upper(), int(m.group(2)), int(m.group(3))
            tabla = _SUFIJO_A_TABLA.get(sufijo)
            if tabla:
                yield ArchivoSII(item, tabla, anio, sem, es_nacional=True)
            continue

        m = _NACROL_PATTERN.match(name)
        if m:
            anio, sem = int(m.group(1)), int(m.group(2))
            yield ArchivoSII(item, "rol_cobro", anio, sem, es_nacional=True)
            continue

        m = _CATAS_PATTERN.match(name)
        if m:
            sufijo = m.group(1).upper()
            anio, sem, codigo = int(m.group(2)), int(m.group(3)), m.group(4)
            tabla = _SUFIJO_A_TABLA.get(sufijo)
            if tabla and codigo in COMMUNES:
                yield ArchivoSII(item, tabla, anio, sem, es_nacional=False,
                                 codigo_comuna=codigo)
            continue

        m = _ROLSEM_PATTERN.match(name)
        if m:
            anio, sem, codigo = int(m.group(1)), int(m.group(2)), m.group(3)
            if codigo in COMMUNES:
                yield ArchivoSII(item, "rol_cobro", anio, sem, es_nacional=False,
                                 codigo_comuna=codigo)


# ---------------------------------------------------------------------------
# Procesamiento de un archivo con staging
# ---------------------------------------------------------------------------

_PARSER_MAP = {
    "roles_agricolas":    parsers.parse_roles_agricolas,
    "detalle_agricola":   parsers.parse_detalle_agricola,
    "roles_no_agricolas": parsers.parse_roles_no_agricolas,
    "detalle_no_agricola": parsers.parse_detalle_no_agricola,
}


def _procesar_archivo(archivo: ArchivoSII, conn) -> tuple[int, int, int]:
    """
    Carga un archivo usando tablas staging.

    Returns:
        (registros_leidos, registros_insertados, registros_actualizados)
    """
    meta = TABLE_META[archivo.tipo_tabla]
    stg_table = meta["staging"]
    key_cols  = meta["key_cols"]

    filter_communes = set(COMMUNES.keys()) if archivo.es_nacional else None
    nombre = archivo.path.name

    # ── Paso 1: vaciar staging ──────────────────────────────────────────────
    truncate_staging(conn, stg_table)

    # ── Paso 2: cargar todos los chunks al staging ──────────────────────────
    total_leidos = 0

    if archivo.tipo_tabla == "rol_cobro":
        gen = parsers.parse_rol_cobro(archivo.path, filter_communes=filter_communes)
    else:
        gen = _PARSER_MAP[archivo.tipo_tabla](
            archivo.path,
            archivo.anio,
            archivo.semestre,
            filter_communes=filter_communes,
        )

    for chunk in gen:
        if chunk.empty:
            continue
        chunk["fuente_archivo"] = nombre
        bulk_insert_staging(conn, stg_table, chunk)
        total_leidos += len(chunk)

    if total_leidos == 0:
        logger.warning("  Sin registros para las comunas configuradas en: %s", nombre)
        return 0, 0, 0

    # ── Paso 3: upsert staging → producción ────────────────────────────────
    insertados, actualizados = upsert_from_staging(
        conn, archivo.tipo_tabla, stg_table, key_cols
    )

    return total_leidos, insertados, actualizados


# ---------------------------------------------------------------------------
# Entry point del pipeline
# ---------------------------------------------------------------------------

def ejecutar_pipeline(skip_procesados: bool = True) -> None:
    archivos = sorted(
        _descubrir_archivos(DATA_BASE_PATH),
        key=lambda a: (a.anio, a.semestre, a.tipo_tabla),
    )

    if not archivos:
        logger.error("No se encontraron archivos en %s", DATA_BASE_PATH)
        sys.exit(1)

    logger.info("Archivos descubiertos: %d", len(archivos))
    for a in archivos:
        tag = "NAC" if a.es_nacional else a.codigo_comuna
        logger.info("  [%s] %d-%d  %-20s  %s",
                    tag, a.anio, a.semestre, a.tipo_tabla, a.path.name)

    total_leidos = total_insertados = total_actualizados = 0

    with get_connection() as conn:
        for archivo in tqdm(archivos, desc="Archivos", unit="archivo"):
            nombre = archivo.path.name

            if skip_procesados and archivo_ya_procesado(conn, nombre):
                logger.info("SKIP (sin cambios previos): %s", nombre)
                continue

            logger.info("Procesando: %s", nombre)
            log_id = log_inicio(
                conn, nombre, archivo.tipo_tabla,
                archivo.codigo_comuna, archivo.anio, archivo.semestre,
                archivo.es_nacional,
            )

            try:
                leidos, insertados, actualizados = _procesar_archivo(archivo, conn)
                ignorados = leidos - insertados - actualizados
                log_fin(conn, log_id, leidos, insertados, actualizados, ignorados)
                total_leidos      += leidos
                total_insertados  += insertados
                total_actualizados += actualizados
                logger.info(
                    "  OK → leídos: %d | nuevos: %d | actualizados: %d | sin cambios: %d",
                    leidos, insertados, actualizados, ignorados,
                )
            except Exception as exc:
                logger.error("ERROR en %s: %s", nombre, exc, exc_info=True)
                log_fin(conn, log_id, 0, 0, 0, 0, estado="ERROR",
                        mensaje_error=str(exc))

    logger.info("=" * 60)
    logger.info("Pipeline completado.")
    logger.info("  Registros leídos      : %d", total_leidos)
    logger.info("  Registros nuevos      : %d", total_insertados)
    logger.info("  Registros actualizados: %d", total_actualizados)
    logger.info("  Registros sin cambios : %d",
                total_leidos - total_insertados - total_actualizados)
