"""
Validación y resumen de los datos cargados en MySQL.

Ejecutar después de load_data.py para verificar la integridad de la carga.

Uso:
    python scripts/validate_data.py
"""

import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import mysql.connector

from config import DB_CONFIG
from etl.db import get_connection

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)


def run_query(conn, sql: str, params: tuple = ()) -> list:
    cursor = conn.cursor()
    cursor.execute(sql, params)
    rows = cursor.fetchall()
    cursor.close()
    return rows


def seccion(titulo: str) -> None:
    print(f"\n{'='*60}")
    print(f"  {titulo}")
    print("="*60)


def main() -> None:
    with get_connection() as conn:

        # ------------------------------------------------------------------
        seccion("1. RESUMEN DE CARGA (carga_log)")
        # ------------------------------------------------------------------
        rows = run_query(conn, """
            SELECT tipo_archivo, codigo_comuna, anio, semestre,
                   estado,
                   registros_leidos,
                   registros_insertados,
                   registros_actualizados,
                   registros_ignorados,
                   TIMESTAMPDIFF(SECOND, inicio_carga, fin_carga) AS segundos
            FROM carga_log
            ORDER BY tipo_archivo, anio, semestre
        """)
        print(f"{'Tabla':<22} {'Cmna':<6} {'Año':<5} {'S':<2} "
              f"{'Estado':<12} {'Leídos':>9} {'Nuevos':>8} {'Actua':>7} {'Igno':>6} {'s':>5}")
        print("-" * 90)
        for r in rows:
            print(f"{r[0]:<22} {str(r[1] or ''):<6} {str(r[2] or ''):<5} {str(r[3] or ''):<2} "
                  f"{r[4]:<12} {r[5] or 0:>9,} {r[6] or 0:>8,} "
                  f"{r[7] or 0:>7,} {r[8] or 0:>6,} {r[9] or 0:>5}")

        # ------------------------------------------------------------------
        seccion("2. CONTEO POR TABLA Y PERÍODO")
        # ------------------------------------------------------------------
        for tabla in ["rol_cobro", "roles_agricolas", "detalle_agricola",
                      "roles_no_agricolas", "detalle_no_agricola"]:
            try:
                col_anio = "anio"
                rows = run_query(conn, f"""
                    SELECT codigo_comuna, anio, semestre, COUNT(*) AS total
                    FROM `{tabla}`
                    GROUP BY codigo_comuna, anio, semestre
                    ORDER BY codigo_comuna, anio, semestre
                """)
                if rows:
                    print(f"\n  {tabla}:")
                    for r in rows:
                        print(f"    Comuna {r[0]}  {r[1]}-S{r[2]}  →  {r[3]:>10,} registros")
                else:
                    print(f"\n  {tabla}: (vacía)")
            except mysql.connector.Error as e:
                print(f"\n  {tabla}: ERROR - {e}")

        # ------------------------------------------------------------------
        seccion("3. DISTRIBUCIÓN POR DESTINO (roles_no_agricolas)")
        # ------------------------------------------------------------------
        DESTINOS = {
            "A": "Agrícola", "B": "Agroindustrial", "C": "Comercio",
            "D": "Deporte/Recreación", "E": "Educación/Cultura",
            "F": "Forestal", "G": "Hotel/Motel", "H": "Habitacional",
            "I": "Industria", "L": "Bodega", "M": "Minería",
            "O": "Oficina", "P": "Adm. Pública", "Q": "Culto",
            "S": "Salud", "T": "Transporte/Telecom", "V": "Otros",
            "W": "Sitio Eriazo", "Y": "Gallineros/otros", "Z": "Estacionamiento",
        }
        rows = run_query(conn, """
            SELECT codigo_destino_principal, COUNT(*) AS total,
                   SUM(avaluo_fiscal_total) AS avaluo_total_sum,
                   AVG(avaluo_fiscal_total) AS avaluo_promedio
            FROM roles_no_agricolas
            GROUP BY codigo_destino_principal
            ORDER BY total DESC
            LIMIT 15
        """)
        print(f"\n  {'Cod':<4} {'Destino':<25} {'Predios':>10} "
              f"{'Avalúo Total (M$)':>20} {'Avalúo Prom ($)':>18}")
        print("  " + "-"*80)
        for r in rows:
            desc = DESTINOS.get(r[0] or "", "Desconocido")
            avaluo_sum = (r[2] or 0) / 1_000_000
            avaluo_prom = r[3] or 0
            print(f"  {r[0] or '?':<4} {desc:<25} {r[1]:>10,} "
                  f"{avaluo_sum:>20,.1f} {avaluo_prom:>18,.0f}")

        # ------------------------------------------------------------------
        seccion("4. ESTADÍSTICAS AVALÚOS (roles_no_agricolas, H=Habitacional)")
        # ------------------------------------------------------------------
        rows = run_query(conn, """
            SELECT anio, semestre,
                   COUNT(*) AS predios,
                   MIN(avaluo_fiscal_total) AS avaluo_min,
                   AVG(avaluo_fiscal_total) AS avaluo_prom,
                   MAX(avaluo_fiscal_total) AS avaluo_max,
                   SUM(contribucion_semestral) AS contrib_total
            FROM roles_no_agricolas
            WHERE codigo_destino_principal = 'H'
            GROUP BY anio, semestre
            ORDER BY anio, semestre
        """)
        if rows:
            print(f"\n  {'Año':<5} {'Sem':<4} {'Predios':>8} "
                  f"{'Avalúo Mín':>15} {'Avalúo Prom':>15} {'Avalúo Máx':>15} "
                  f"{'Contrib. Total':>18}")
            print("  " + "-"*85)
            for r in rows:
                print(f"  {r[0]:<5} {r[1]:<4} {r[2]:>8,} "
                      f"{r[3] or 0:>15,.0f} {r[4] or 0:>15,.0f} {r[5] or 0:>15,.0f} "
                      f"{r[6] or 0:>18,.0f}")
        else:
            print("  (sin datos habitacionales)")

        # ------------------------------------------------------------------
        seccion("5. VALIDACIONES DE INTEGRIDAD")
        # ------------------------------------------------------------------
        checks = [
            ("Roles no agrícolas sin dirección",
             "SELECT COUNT(*) FROM roles_no_agricolas WHERE direccion_predio IS NULL"),
            ("Roles no agrícolas con avalúo cero",
             "SELECT COUNT(*) FROM roles_no_agricolas WHERE avaluo_fiscal_total = 0"),
            ("Construcciones sin año",
             "SELECT COUNT(*) FROM detalle_no_agricola WHERE anio_construccion IS NULL"),
            ("Rol cobro con avalúo NULL",
             "SELECT COUNT(*) FROM rol_cobro WHERE avaluo_total IS NULL"),
        ]
        for desc, sql in checks:
            try:
                rows = run_query(conn, sql)
                valor = rows[0][0] if rows else "N/A"
                estado = "OK" if valor == 0 else f"ATENCIÓN: {valor:,} registros"
                print(f"  {desc:<45}  {estado}")
            except mysql.connector.Error as e:
                print(f"  {desc:<45}  ERROR: {e}")

        print()
        logger.info("Validación completada.")


if __name__ == "__main__":
    main()
