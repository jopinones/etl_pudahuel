# Plan ETL: Rol de Contribuciones SII → MySQL

## Objetivo

Leer archivos pesados del SII (Bienes Raíces) para la comuna de **Pudahuel (código 14111)**,
procesarlos y cargarlos en una base de datos MySQL local para análisis de datos.
El diseño debe soportar fácilmente la incorporación de nuevas comunas.

---

## Fuentes de Datos Disponibles

Ruta base: `C:\Jonathan\PR_Rol_Contribuciones\`

| Carpeta/Archivo | Tipo | Período | Descripción |
|---|---|---|---|
| `BRORGA2441_NAC_2024_1/` | Nacional | 2024 Sem 1 | Catastral completo país |
| `BRORGA2441_NAC_2025_1/` | Nacional | 2025 Sem 1 | Catastral completo país |
| `BRORGA2441_NAC_2025_2/` | Nacional | 2025 Sem 2 | Catastral completo país |
| `BRTMPCATAS_2026_1_14111/` | Comunal | 2026 Sem 1 | Catastral solo Pudahuel |
| `BRTMPNACROL_NAC_2024_1/` | Nacional | 2024 Sem 1 | Rol cobro (ancho fijo) |
| `BRTMPNACROL_NAC_2025_1/` | Nacional | 2025 Sem 1 | Rol cobro (ancho fijo) |
| `BRTMPNACROL_NAC_2025_2/` | Nacional | 2025 Sem 2 | Rol cobro (ancho fijo) |
| `BRTMPROLSEM_2026_1_14111.TXT` | Comunal | 2026 Sem 1 | Rol semestral Pudahuel |

### Tipos de Archivos Internos

| Prefijo | Destino BD | Formato | Columnas |
|---|---|---|---|
| `BRORGA2441A_` / `BRTMPCATASA_` | `roles_agricolas` | Pipe `\|` | 9 (+ trailing \|) |
| `BRORGA2441AL_` / `BRTMPCATASAL_` | `detalle_agricola` | Pipe `\|` | 12 |
| `BRORGA2441N_` / `BRTMPCATASN_` | `roles_no_agricolas` | Pipe `\|` | 19 (+ trailing \|) |
| `BRORGA2441NL_` / `BRTMPCATASNL_` | `detalle_no_agricola` | Pipe `\|` | 11 (+ trailing \|) |
| `BRTMPNACROL_` / `BRTMPROLSEM_` | `rol_cobro` | Ancho fijo 117 chars | 14 campos |

---

## Estructura de la Base de Datos MySQL

### Diagrama de Tablas

```
comunas ─────────────────────────────────────────────┐
                                                      │
rol_cobro (por manzana+predio+año+sem)               │
roles_agricolas (por manzana+predio+año+sem)    ←────┤ FK codigo_comuna
  └── detalle_agricola                                │
roles_no_agricolas (por manzana+predio+año+sem) ←────┘
  └── detalle_no_agricola

carga_log (registro de archivos procesados)
```

### Tablas

| Tabla | Descripción | Clave Única |
|---|---|---|
| `comunas` | Catálogo de comunas | `codigo_sii` |
| `rol_cobro` | Rol semestral ancho fijo | `(codigo_comuna, manzana, predio, anio, semestre)` |
| `roles_agricolas` | Roles agrícolas básicos | `(codigo_comuna, manzana, predial, anio, semestre)` |
| `detalle_agricola` | Suelos y construcciones agrícolas | — |
| `roles_no_agricolas` | Roles no agrícolas básicos | `(codigo_comuna, manzana, predial, anio, semestre)` |
| `detalle_no_agricola` | Construcciones no agrícolas | — |
| `carga_log` | Trazabilidad de cargas | — |

---

## Estructura del Proyecto

```
etl_sii_pudahuel/
├── PLAN.md                    ← este archivo
├── .env.example               ← variables de entorno (copiar a .env)
├── requirements.txt           ← dependencias Python
├── config.py                  ← configuración central
│
├── sql/
│   └── create_tables.sql      ← DDL MySQL completo
│
├── etl/
│   ├── __init__.py
│   ├── parsers.py             ← parsers por tipo de archivo
│   ├── db.py                  ← conexión MySQL y carga masiva
│   └── pipeline.py            ← descubrimiento y orquestación
│
└── scripts/
    ├── setup_db.py            ← crea/verifica esquema en MySQL
    ├── load_data.py           ← punto de entrada principal ETL
    └── validate_data.py       ← verificación de calidad de datos
```

---

## Flujo ETL

```
1. setup_db.py
   └── Crea base de datos y tablas en MySQL (idempotente)

2. load_data.py
   ├── Descubre todos los archivos en DATA_BASE_PATH
   ├── Para cada archivo:
   │   ├── Detecta tipo (nacional vs comunal, agrícola/no agrícola, etc.)
   │   ├── Lee en chunks de 100k filas
   │   ├── Filtra por código de comuna (solo para archivos nacionales)
   │   ├── Limpia y convierte tipos de datos
   │   ├── Inserta con INSERT IGNORE (idempotente)
   │   └── Registra resultado en carga_log
   └── Muestra resumen final

3. validate_data.py
   └── Ejecuta queries de validación sobre los datos cargados
```

---

## Decisiones de Diseño

### Conversión de Valores Monetarios
Los archivos del SII almacenan montos como enteros con 2 decimales implícitos.
Ejemplo: `000001727332030` → $17,273,320.30 CLP
**Se dividen por 100** al cargar → `DECIMAL(15,2)` en MySQL.

### Superficies
- Construcciones no agrícolas (`superficie_construccion`): sin decimales → `INT`
- Terrenos (`superficie_total_terreno`): sin decimales → `INT`
- Suelos agrícolas (`superficie_suelo`): 2 últimas cifras son decimales → dividir por 100 → `DECIMAL(12,2)`

### Codificación
Todos los archivos del SII usan **Latin-1 (ISO-8859-1)**.

### Archivos Nacionales (pesados, 1-1.5GB)
- Procesados en chunks de 100,000 filas con `pandas.read_csv(chunksize=...)`
- Cada chunk se filtra por código(s) de comuna antes de insertar
- Esto evita cargar millones de filas innecesarias en memoria

### Idempotencia
- Cada tabla tiene una clave `UNIQUE` por periodo (año + semestre) y rol
- Se usa `INSERT IGNORE` → re-ejecutar no duplica datos
- `carga_log` registra si el archivo ya fue procesado

### Escalabilidad (nuevas comunas)
Agregar una nueva comuna requiere solo:
1. Añadir el código en `config.py` → `COMMUNES = {"14111": "Pudahuel", "13101": "Santiago", ...}`
2. Colocar los archivos en la carpeta de datos
3. Volver a ejecutar `load_data.py`

---

## Requisitos del Sistema

- Python 3.9+
- MySQL Server 8.0+ (local)
- ~4GB RAM libre (para chunks de archivos nacionales)
- mysql-connector-python, pandas, tqdm, python-dotenv

---

## Uso Rápido

```bash
# 1. Instalar dependencias
pip install -r requirements.txt

# 2. Configurar conexión
cp .env.example .env
# Editar .env con credenciales MySQL

# 3. Crear esquema en MySQL
python scripts/setup_db.py

# 4. Cargar datos
python scripts/load_data.py

# 5. Validar carga
python scripts/validate_data.py
```
