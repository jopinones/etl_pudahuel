# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
# Install dependencies (use the .venv or a fresh virtualenv)
pip install -r requirements.txt

# First-time: create .env from template and fill in credentials
copy .env.example .env

# Create/verify MySQL schema (idempotent)
python scripts/setup_db.py

# Run ETL (skips files already marked COMPLETADO in carga_log)
python scripts/load_data.py

# Force re-process all files regardless of carga_log status
python scripts/load_data.py --force

# List files that would be processed without loading
python scripts/load_data.py --dry-run

# Post-load data quality checks
python scripts/validate_data.py
```

## Architecture

### Three-stage differential load (per file)

1. `TRUNCATE` the corresponding `stg_*` staging table
2. Bulk `INSERT` all parsed chunks → staging (fast, no deduplication)
3. `upsert_from_staging()` → production:
   - **INSERT** rows whose business key doesn't exist in production
   - **UPDATE** rows with same key but different `row_hash` (content changed)
   - **SKIP** rows with identical hash (no changes, idempotent)

All load attempts (file name, row counts, errors) are recorded in `carga_log`.

### File auto-discovery (`etl/pipeline.py`)

Files are found by regex patterns on filenames. Four pattern types map to five destination tables:

| Pattern | Table | Format |
|---|---|---|
| `BRORGA2441A_` / `BRTMPCATASA_` | `roles_agricolas` | Pipe `\|` |
| `BRORGA2441AL_` / `BRTMPCATASAL_` | `detalle_agricola` | Pipe `\|` |
| `BRORGA2441N_` / `BRTMPCATASN_` | `roles_no_agricolas` | Pipe `\|` |
| `BRORGA2441NL_` / `BRTMPCATASNL_` | `detalle_no_agricola` | Pipe `\|` |
| `BRTMPNACROL_` / `BRTMPROLSEM_` | `rol_cobro` | Fixed-width 117 chars |

National files (`es_nacional=True`) are filtered by `COMMUNES` keys during parsing. Communal files include the commune code in their filename and are only loaded if that code is in `COMMUNES`.

### Chunked processing

National files (1–1.5 GB) are read via `pandas.read_csv(chunksize=CHUNK_SIZE)`. Each chunk is parsed, filtered by commune, hash-computed, and inserted to staging before the next chunk is read—RAM stays bounded regardless of file size.

## Key design decisions

- **Encoding**: All SII files are Latin-1 (ISO-8859-1). Never use UTF-8 when opening them.
- **Monetary fields** (`avaluo`, `contribucion`): raw integers with 2 implicit decimals → divide by 100 → stored as `DECIMAL(15,2)`.
- **Soil surfaces** (`superficie_suelo`): same rule, divide by 100 → `DECIMAL(12,2)`.
- **Land/building surfaces**: no implicit decimals → `INT`.
- **`row_hash`**: MD5 of all business fields (excludes `fuente_archivo`, `fecha_carga`). If the hash matches, the row is treated as unchanged.
- **Adding a new commune**: add its code to `COMMUNES` in `config.py`, place the files under `DATA_BASE_PATH`, and re-run `load_data.py`. No schema changes needed.

## Configuration

`config.py` is the single source of truth for:
- `COMMUNES` dict (commune code → name)
- `DATA_BASE_PATH` (resolved from `DATA_BASE_PATH` env var)
- `TABLE_META` (staging table names, key columns per table)
- Column definitions for each parser

All secrets and paths live in `.env` (never committed). See `.env.example` for the full list of variables.
