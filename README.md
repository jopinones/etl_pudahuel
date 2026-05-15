# ETL SII Bienes Raíces → MySQL

Pipeline ETL que procesa archivos del **Servicio de Impuestos Internos (SII)** de Chile (Bienes Raíces) y los carga en una base de datos MySQL local para análisis. Soporta archivos nacionales (1–1.5 GB) y comunales, con carga diferencial por hash de fila para procesar solo los registros nuevos o modificados.

## Requisitos

- Python 3.9+
- MySQL Server 8.0+
- ~4 GB de RAM disponibles (para archivos nacionales en chunks)

## Instalación

```bash
# 1. Crear y activar entorno virtual
python -m venv .venv
.venv\Scripts\activate      # Windows
# source .venv/bin/activate # Linux/macOS

# 2. Instalar dependencias
pip install -r requirements.txt

# 3. Crear archivo de configuración
copy .env.example .env
```

Editar `.env` con las credenciales MySQL y la ruta a los archivos del SII:

```env
DATA_BASE_PATH=C:\Jonathan\PR_Rol_Contribuciones
DB_HOST=localhost
DB_PORT=3306
DB_USER=root
DB_PASSWORD=tu_password_aqui
DB_NAME=sii_bienes_raices
CHUNK_SIZE=100000
```

## Uso

```bash
# Crear el esquema en MySQL (idempotente, ejecutar una sola vez)
python scripts/setup_db.py

# Cargar datos (omite archivos ya procesados)
python scripts/load_data.py

# Forzar re-proceso de todos los archivos
python scripts/load_data.py --force

# Listar archivos que se procesarían sin cargar nada
python scripts/load_data.py --dry-run

# Verificar calidad de los datos cargados
python scripts/validate_data.py
```

## Tipos de archivos soportados

| Nombre de archivo | Tabla destino | Formato |
|---|---|---|
| `BRORGA2441A_NAC_YYYY_S` / `BRTMPCATASA_YYYY_S_COMUNA` | `roles_agricolas` | Pipe `\|` |
| `BRORGA2441AL_NAC_YYYY_S` / `BRTMPCATASAL_YYYY_S_COMUNA` | `detalle_agricola` | Pipe `\|` |
| `BRORGA2441N_NAC_YYYY_S` / `BRTMPCATASN_YYYY_S_COMUNA` | `roles_no_agricolas` | Pipe `\|` |
| `BRORGA2441NL_NAC_YYYY_S` / `BRTMPCATASNL_YYYY_S_COMUNA` | `detalle_no_agricola` | Pipe `\|` |
| `BRTMPNACROL_NAC_YYYY_S` / `BRTMPROLSEM_YYYY_S_COMUNA.TXT` | `rol_cobro` | Ancho fijo 117 chars |

Los archivos nacionales se filtran automáticamente por las comunas configuradas en `config.py`.

## Esquema de base de datos

```
comunas
rol_cobro           (clave: codigo_comuna, manzana, predio, anio, semestre)
roles_agricolas     (clave: codigo_comuna, manzana, predial, anio, semestre)
  └── detalle_agricola
roles_no_agricolas  (clave: codigo_comuna, manzana, predial, anio, semestre)
  └── detalle_no_agricola
carga_log           (trazabilidad de cada archivo procesado)
```

El DDL completo está en [sql/create_tables.sql](sql/create_tables.sql).

## Agregar una nueva comuna

1. Añadir el código en `config.py`:
   ```python
   COMMUNES = {
       "14111": "Pudahuel",
       "13101": "Santiago",  # nueva comuna
   }
   ```
2. Colocar los archivos SII en `DATA_BASE_PATH`
3. Ejecutar `python scripts/load_data.py`

## Arquitectura

El pipeline usa **tablas staging** para la carga diferencial:

1. `TRUNCATE` de la tabla `stg_*` correspondiente
2. `INSERT` masivo de todos los chunks del archivo → staging
3. Upsert staging → producción: inserta nuevos, actualiza modificados (por `row_hash`), omite sin cambios

Esto garantiza idempotencia: re-ejecutar el pipeline sobre el mismo archivo no duplica ni corrompe datos.

### Notas sobre los datos

- **Codificación**: todos los archivos SII usan Latin-1 (ISO-8859-1)
- **Valores monetarios** (`avaluo`, `contribucion`): enteros con 2 decimales implícitos → se dividen por 100 al cargar
- **Superficies de suelo agrícola**: igual, divididas por 100
- **Superficies de terreno/construcción**: sin decimales implícitos, se cargan como enteros

## Estructura del proyecto

```
etl_sii_pudahuel/
├── .env.example          ← plantilla de configuración
├── requirements.txt
├── config.py             ← comunas, rutas, columnas por tabla
├── PLAN.md               ← especificación técnica detallada
├── sql/
│   └── create_tables.sql ← DDL completo
├── etl/
│   ├── parsers.py        ← parsers por tipo de archivo
│   ├── db.py             ← operaciones MySQL (staging, upsert, log)
│   └── pipeline.py       ← descubrimiento y orquestación
└── scripts/
    ├── setup_db.py       ← creación del esquema
    ├── load_data.py      ← punto de entrada ETL
    └── validate_data.py  ← validación post-carga
```
