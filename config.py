import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

DATA_BASE_PATH = Path(os.getenv("DATA_BASE_PATH", r"C:\Jonathan\PR_Rol_Contribuciones"))

DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": int(os.getenv("DB_PORT", "3306")),
    "user": os.getenv("DB_USER", "root"),
    "password": os.getenv("DB_PASSWORD", ""),
    "database": os.getenv("DB_NAME", "sii_bienes_raices"),
    "charset": "utf8mb4",
    "use_unicode": True,
    "autocommit": False,
}

# Comunas a procesar: código_SII → nombre
# Agregar nuevas comunas aquí para incluirlas en la carga
COMMUNES = {
    "14111": "Pudahuel",
}

# Filas procesadas por chunk en archivos nacionales (ajustar según RAM disponible)
CHUNK_SIZE = int(os.getenv("CHUNK_SIZE", "100000"))

# Encoding de todos los archivos SII
SII_ENCODING = "latin-1"

# Separador de campos en archivos pipe-delimited
SII_SEPARATOR = "|"

# Metadatos de tablas: staging name + columnas clave para el JOIN de upsert.
# key_cols identifica unívocamente un registro de negocio (sin id ni fecha_carga).
# Usados por etl/db.py::upsert_from_staging para generar INSERT new + UPDATE changed.
TABLE_META: dict[str, dict] = {
    "rol_cobro": {
        "staging": "stg_rol_cobro",
        "key_cols": ["codigo_comuna", "manzana", "predio", "anio", "semestre"],
    },
    "roles_agricolas": {
        "staging": "stg_roles_agricolas",
        "key_cols": ["codigo_comuna", "numero_manzana", "numero_predial", "anio", "semestre"],
    },
    "detalle_agricola": {
        "staging": "stg_detalle_agricola",
        "key_cols": ["codigo_comuna", "numero_manzana", "numero_predial",
                     "anio", "semestre", "codigo_suelo", "num_linea_construccion"],
    },
    "roles_no_agricolas": {
        "staging": "stg_roles_no_agricolas",
        "key_cols": ["codigo_comuna", "numero_manzana", "numero_predial", "anio", "semestre"],
    },
    "detalle_no_agricola": {
        "staging": "stg_detalle_no_agricola",
        "key_cols": ["codigo_comuna", "numero_manzana", "numero_predial",
                     "anio", "semestre", "num_linea_construccion"],
    },
}

# Columnas por tipo de archivo (confirmadas inspeccionando datos reales de Pudahuel)

COLS_ROLES_AGRICOLAS = [
    "codigo_comuna", "numero_manzana", "numero_predial",
    "direccion_predio", "avaluo_fiscal_total_raw", "contribucion_semestral_raw",
    "codigo_destino_principal", "avaluo_exento_raw", "codigo_ubicacion",
]

COLS_DETALLE_AGRICOLA = [
    "codigo_comuna", "numero_manzana", "numero_predial",
    "codigo_suelo", "superficie_suelo_raw", "num_linea_construccion",
    "codigo_material", "codigo_calidad", "superficie_construccion",
    "codigo_destino", "codigo_condicion_especial", "numero_pisos",
]

COLS_ROLES_NO_AGRICOLAS = [
    "codigo_comuna", "numero_manzana", "numero_predial",
    "direccion_predio", "avaluo_fiscal_total_raw", "contribucion_semestral_raw",
    "codigo_destino_principal", "avaluo_exento_raw",
    "codigo_comuna_bc1", "num_manzana_bc1", "num_predio_bc1",
    "codigo_comuna_bc2", "num_manzana_bc2", "num_predio_bc2",
    "superficie_total_terreno", "codigo_ubicacion",
    "codigo_comuna_padre", "num_manzana_padre", "num_predio_padre",
]

COLS_DETALLE_NO_AGRICOLA = [
    "codigo_comuna", "numero_manzana", "numero_predial",
    "num_linea_construccion", "codigo_material", "codigo_calidad",
    "anio_construccion", "superficie_construccion",
    "codigo_destino", "codigo_condicion_especial", "numero_pisos",
]
