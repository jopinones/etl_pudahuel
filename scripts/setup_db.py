"""
Crea la base de datos y las tablas en MySQL (idempotente).

Uso:
    python scripts/setup_db.py
"""

import logging
import sys
from pathlib import Path

# Permitir imports desde la raíz del proyecto
sys.path.insert(0, str(Path(__file__).parent.parent))

import mysql.connector

from config import DB_CONFIG
from etl.db import execute_sql_file

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

SQL_FILE = Path(__file__).parent.parent / "sql" / "create_tables.sql"


def crear_base_de_datos() -> None:
    """Crea la base de datos si no existe (sin especificarla en la conexión)."""
    cfg = {k: v for k, v in DB_CONFIG.items() if k != "database"}
    db_name = DB_CONFIG["database"]
    conn = mysql.connector.connect(**cfg)
    cursor = conn.cursor()
    cursor.execute(
        f"CREATE DATABASE IF NOT EXISTS `{db_name}` "
        "CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"
    )
    conn.commit()
    cursor.close()
    conn.close()
    logger.info("Base de datos '%s' verificada/creada.", db_name)


def main() -> None:
    logger.info("Configurando esquema MySQL para SII Bienes Raíces...")

    # Paso 1: crear BD si no existe
    crear_base_de_datos()

    # Paso 2: ejecutar DDL completo
    logger.info("Ejecutando DDL: %s", SQL_FILE)
    execute_sql_file(str(SQL_FILE))
    logger.info("Tablas creadas/verificadas correctamente.")
    logger.info("")
    logger.info("Tablas disponibles en '%s':", DB_CONFIG["database"])
    logger.info("  - comunas")
    logger.info("  - rol_cobro")
    logger.info("  - roles_agricolas")
    logger.info("  - detalle_agricola")
    logger.info("  - roles_no_agricolas")
    logger.info("  - detalle_no_agricola")
    logger.info("  - carga_log")
    logger.info("")
    logger.info("Siguiente paso:  python scripts/load_data.py")


if __name__ == "__main__":
    main()
