"""
Punto de entrada principal del ETL SII → MySQL.

Uso:
    python scripts/load_data.py               # procesa todos los archivos
    python scripts/load_data.py --force       # re-procesa aunque ya estén cargados
    python scripts/load_data.py --dry-run     # solo lista archivos sin cargar

Agrega nuevas comunas en config.py → COMMUNES y vuelve a ejecutar.
"""

import argparse
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from config import COMMUNES, DATA_BASE_PATH
from etl.pipeline import ejecutar_pipeline, _descubrir_archivos

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


def main() -> None:
    parser = argparse.ArgumentParser(description="ETL SII Bienes Raíces → MySQL")
    parser.add_argument(
        "--force", action="store_true",
        help="Re-procesa archivos ya marcados como COMPLETADO en carga_log",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Solo lista los archivos que se procesarían, sin cargar nada",
    )
    args = parser.parse_args()

    logger.info("=" * 60)
    logger.info("ETL SII Bienes Raíces")
    logger.info("Ruta de datos : %s", DATA_BASE_PATH)
    logger.info("Comunas       : %s", ", ".join(
        f"{k}={v}" for k, v in COMMUNES.items()
    ))
    logger.info("=" * 60)

    if not DATA_BASE_PATH.exists():
        logger.error("La ruta de datos no existe: %s", DATA_BASE_PATH)
        sys.exit(1)

    if args.dry_run:
        archivos = list(_descubrir_archivos(DATA_BASE_PATH))
        logger.info("DRY RUN — archivos que se procesarían (%d):", len(archivos))
        for a in archivos:
            tipo = "NAC" if a.es_nacional else a.codigo_comuna
            logger.info("  [%s] %d-%d  %-20s  %s",
                        tipo, a.anio, a.semestre, a.tipo_tabla, a.path.name)
        return

    ejecutar_pipeline(skip_procesados=not args.force)


if __name__ == "__main__":
    main()
