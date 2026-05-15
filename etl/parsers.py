"""
Parsers para cada tipo de archivo SII Bienes Raíces.

Tipos soportados:
- Pipe-delimited: roles_agricolas, detalle_agricola,
                  roles_no_agricolas, detalle_no_agricola
- Ancho fijo 117 chars: rol_cobro
"""

import hashlib
import re
from pathlib import Path
from typing import Iterator, Optional

import pandas as pd

from config import (
    CHUNK_SIZE, SII_ENCODING, SII_SEPARATOR,
    COLS_ROLES_AGRICOLAS, COLS_DETALLE_AGRICOLA,
    COLS_ROLES_NO_AGRICOLAS, COLS_DETALLE_NO_AGRICOLA,
    TABLE_META,
)


# ---------------------------------------------------------------------------
# Cálculo de hash por fila para detección de cambios
# ---------------------------------------------------------------------------

def compute_row_hash(df: pd.DataFrame, exclude_cols: list[str]) -> pd.Series:
    """
    Calcula MD5 fila por fila sobre todos los campos excepto los excluidos.
    Excluir: fuente_archivo (metadato del ETL) y row_hash mismo.
    La clave de negocio (key_cols) SÍ se incluye para que el hash sea
    global al registro; el JOIN de upsert se basa en key_cols, no en el hash.
    """
    skip = set(exclude_cols) | {"row_hash"}
    hash_cols = [c for c in df.columns if c not in skip]
    # Concatenar todos los valores como string con separador poco frecuente
    combined = df[hash_cols].fillna("").astype(str).agg("§".join, axis=1)
    return combined.apply(
        lambda s: hashlib.md5(s.encode("utf-8")).hexdigest()
    )


# ---------------------------------------------------------------------------
# Helpers de limpieza y conversión
# ---------------------------------------------------------------------------

def _to_decimal(series: pd.Series, divisor: int = 100) -> pd.Series:
    """Convierte serie numérica raw (entero con decimales implícitos) a float."""
    return pd.to_numeric(series.str.strip(), errors="coerce") / divisor


def _to_int(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series.str.strip(), errors="coerce").astype("Int64")


def _strip(series: pd.Series) -> pd.Series:
    return series.str.strip().replace("", None)


def _normalizar_lineas(path: Path) -> None:
    """Normaliza saltos de línea Unix a Windows en el archivo (in-place)."""
    content = path.read_bytes()
    # Reemplazar \n sueltos (sin \r previo) por \r\n
    content = re.sub(b"(?<!\r)\n", b"\r\n", content)
    path.write_bytes(content)


# ---------------------------------------------------------------------------
# Parser: archivos pipe-delimited
# ---------------------------------------------------------------------------

def _read_pipe_chunks(
    filepath: Path,
    columns: list[str],
    usecols_count: int,
    filter_col: Optional[str] = None,
    filter_values: Optional[set] = None,
    chunk_size: int = CHUNK_SIZE,
) -> Iterator[pd.DataFrame]:
    """
    Lee un archivo pipe-delimited del SII en chunks.

    Si el archivo tiene más columnas que las definidas (trailing |),
    las columnas extra se descartan usando usecols.
    """
    reader = pd.read_csv(
        filepath,
        sep=re.escape(SII_SEPARATOR),
        header=None,
        names=columns + [f"_extra_{i}" for i in range(10)],  # buffer para trailing |
        usecols=range(usecols_count),
        encoding=SII_ENCODING,
        dtype=str,
        on_bad_lines="skip",
        engine="python",
        chunksize=chunk_size,
    )

    # Asignar nombres correctos solo a las columnas usadas
    for chunk in reader:
        chunk.columns = columns
        if filter_col and filter_values:
            mask = chunk[filter_col].str.strip().isin(filter_values)
            chunk = chunk[mask]
        if not chunk.empty:
            yield chunk


# ---------------------------------------------------------------------------
# Parsers específicos por tipo de tabla
# ---------------------------------------------------------------------------

def parse_roles_agricolas(
    filepath: Path,
    anio: int,
    semestre: int,
    filter_communes: Optional[set] = None,
) -> Iterator[pd.DataFrame]:
    """Genera chunks de roles_agricolas limpios."""
    for chunk in _read_pipe_chunks(
        filepath,
        columns=COLS_ROLES_AGRICOLAS,
        usecols_count=len(COLS_ROLES_AGRICOLAS),
        filter_col="codigo_comuna",
        filter_values=filter_communes,
    ):
        chunk["anio"] = anio
        chunk["semestre"] = semestre
        chunk["codigo_comuna"] = _strip(chunk["codigo_comuna"])
        chunk["numero_manzana"] = _strip(chunk["numero_manzana"])
        chunk["numero_predial"] = _strip(chunk["numero_predial"])
        chunk["direccion_predio"] = _strip(chunk["direccion_predio"])
        chunk["avaluo_fiscal_total"] = _to_decimal(chunk["avaluo_fiscal_total_raw"])
        chunk["contribucion_semestral"] = _to_decimal(chunk["contribucion_semestral_raw"])
        chunk["avaluo_exento"] = _to_decimal(chunk["avaluo_exento_raw"])
        chunk["codigo_destino_principal"] = _strip(chunk["codigo_destino_principal"])
        chunk["codigo_ubicacion"] = _strip(chunk["codigo_ubicacion"])
        chunk.drop(columns=["avaluo_fiscal_total_raw", "contribucion_semestral_raw",
                             "avaluo_exento_raw"], inplace=True)
        chunk["row_hash"] = compute_row_hash(chunk, exclude_cols=["fuente_archivo"])
        yield chunk


def parse_detalle_agricola(
    filepath: Path,
    anio: int,
    semestre: int,
    filter_communes: Optional[set] = None,
) -> Iterator[pd.DataFrame]:
    """Genera chunks de detalle_agricola limpios."""
    for chunk in _read_pipe_chunks(
        filepath,
        columns=COLS_DETALLE_AGRICOLA,
        usecols_count=len(COLS_DETALLE_AGRICOLA),
        filter_col="codigo_comuna",
        filter_values=filter_communes,
    ):
        chunk["anio"] = anio
        chunk["semestre"] = semestre
        chunk["codigo_comuna"] = _strip(chunk["codigo_comuna"])
        chunk["numero_manzana"] = _strip(chunk["numero_manzana"])
        chunk["numero_predial"] = _strip(chunk["numero_predial"])
        chunk["codigo_suelo"] = _strip(chunk["codigo_suelo"])
        # Últimas 2 cifras de superficie_suelo son decimales
        chunk["superficie_suelo"] = _to_decimal(chunk["superficie_suelo_raw"], divisor=100)
        chunk["num_linea_construccion"] = _to_int(chunk["num_linea_construccion"])
        chunk["codigo_material"] = _strip(chunk["codigo_material"])
        chunk["codigo_calidad"] = _strip(chunk["codigo_calidad"])
        chunk["superficie_construccion"] = _to_int(chunk["superficie_construccion"])
        chunk["codigo_destino"] = _strip(chunk["codigo_destino"])
        chunk["codigo_condicion_especial"] = _strip(chunk["codigo_condicion_especial"])
        chunk["numero_pisos"] = _to_int(chunk["numero_pisos"])
        chunk.drop(columns=["superficie_suelo_raw"], inplace=True)
        chunk["row_hash"] = compute_row_hash(chunk, exclude_cols=["fuente_archivo"])
        yield chunk


def parse_roles_no_agricolas(
    filepath: Path,
    anio: int,
    semestre: int,
    filter_communes: Optional[set] = None,
) -> Iterator[pd.DataFrame]:
    """Genera chunks de roles_no_agricolas limpios."""
    for chunk in _read_pipe_chunks(
        filepath,
        columns=COLS_ROLES_NO_AGRICOLAS,
        usecols_count=len(COLS_ROLES_NO_AGRICOLAS),
        filter_col="codigo_comuna",
        filter_values=filter_communes,
    ):
        chunk["anio"] = anio
        chunk["semestre"] = semestre
        chunk["codigo_comuna"] = _strip(chunk["codigo_comuna"])
        chunk["numero_manzana"] = _strip(chunk["numero_manzana"])
        chunk["numero_predial"] = _strip(chunk["numero_predial"])
        chunk["direccion_predio"] = _strip(chunk["direccion_predio"])
        chunk["avaluo_fiscal_total"] = _to_decimal(chunk["avaluo_fiscal_total_raw"])
        chunk["contribucion_semestral"] = _to_decimal(chunk["contribucion_semestral_raw"])
        chunk["avaluo_exento"] = _to_decimal(chunk["avaluo_exento_raw"])
        chunk["codigo_destino_principal"] = _strip(chunk["codigo_destino_principal"])
        chunk["codigo_ubicacion"] = _strip(chunk["codigo_ubicacion"])
        chunk["superficie_total_terreno"] = _to_int(chunk["superficie_total_terreno"])
        for col in ["codigo_comuna_bc1", "num_manzana_bc1", "num_predio_bc1",
                    "codigo_comuna_bc2", "num_manzana_bc2", "num_predio_bc2",
                    "codigo_comuna_padre", "num_manzana_padre", "num_predio_padre"]:
            chunk[col] = _strip(chunk[col])
        chunk.drop(columns=["avaluo_fiscal_total_raw", "contribucion_semestral_raw",
                             "avaluo_exento_raw"], inplace=True)
        chunk["row_hash"] = compute_row_hash(chunk, exclude_cols=["fuente_archivo"])
        yield chunk


def parse_detalle_no_agricola(
    filepath: Path,
    anio: int,
    semestre: int,
    filter_communes: Optional[set] = None,
) -> Iterator[pd.DataFrame]:
    """Genera chunks de detalle_no_agricola limpios."""
    for chunk in _read_pipe_chunks(
        filepath,
        columns=COLS_DETALLE_NO_AGRICOLA,
        usecols_count=len(COLS_DETALLE_NO_AGRICOLA),
        filter_col="codigo_comuna",
        filter_values=filter_communes,
    ):
        chunk["anio"] = anio
        chunk["semestre"] = semestre
        chunk["codigo_comuna"] = _strip(chunk["codigo_comuna"])
        chunk["numero_manzana"] = _strip(chunk["numero_manzana"])
        chunk["numero_predial"] = _strip(chunk["numero_predial"])
        chunk["num_linea_construccion"] = _to_int(chunk["num_linea_construccion"])
        chunk["codigo_material"] = _strip(chunk["codigo_material"])
        chunk["codigo_calidad"] = _strip(chunk["codigo_calidad"])
        chunk["anio_construccion"] = _to_int(chunk["anio_construccion"])
        chunk["superficie_construccion"] = _to_int(chunk["superficie_construccion"])
        chunk["codigo_destino"] = _strip(chunk["codigo_destino"])
        chunk["codigo_condicion_especial"] = _strip(chunk["codigo_condicion_especial"])
        chunk["numero_pisos"] = _to_int(chunk["numero_pisos"])
        chunk["row_hash"] = compute_row_hash(chunk, exclude_cols=["fuente_archivo"])
        yield chunk


# ---------------------------------------------------------------------------
# Parser: Rol de cobro (ancho fijo 117 chars)
# ---------------------------------------------------------------------------

def _parse_rol_cobro_line(line: str) -> Optional[dict]:
    """Parsea una línea de 117 chars del archivo de rol de cobro."""
    line = line.rstrip("\r\n")
    if len(line) < 117:
        return None
    try:
        return {
            "codigo_comuna":         line[0:5].strip(),
            "anio":                  int(line[5:9]) if line[5:9].strip() else None,
            "semestre":              int(line[9:10]) if line[9:10].strip() else None,
            "indicador_aseo":        line[10:11].strip() or None,
            "direccion_predial":     line[17:57].strip() or None,
            "manzana":               line[57:62].strip(),
            "predio":                line[62:67].strip(),
            "codigo_serie":          line[67:68].strip() or None,
            "cuota_trimestral":      _safe_decimal(line[68:81]),
            "avaluo_total":          _safe_decimal(line[81:96]),
            "avaluo_exento":         _safe_decimal(line[96:111]),
            "anio_termino_exencion": int(line[111:115]) if line[111:115].strip() else None,
            "codigo_ubicacion":      line[115:116].strip() or None,
            "codigo_destino":        line[116:117].strip() or None,
        }
    except (ValueError, IndexError):
        return None


def _safe_decimal(raw: str, divisor: float = 100.0) -> Optional[float]:
    try:
        return int(raw) / divisor
    except (ValueError, TypeError):
        return None


def parse_rol_cobro(
    filepath: Path,
    filter_communes: Optional[set] = None,
    chunk_size: int = CHUNK_SIZE,
) -> Iterator[pd.DataFrame]:
    """Genera chunks de rol_cobro desde archivo de ancho fijo."""
    buffer = []
    with open(filepath, "r", encoding=SII_ENCODING) as f:
        for line in f:
            record = _parse_rol_cobro_line(line)
            if record is None:
                continue
            if filter_communes and record["codigo_comuna"] not in filter_communes:
                continue
            buffer.append(record)
            if len(buffer) >= chunk_size:
                chunk = pd.DataFrame(buffer)
                chunk["row_hash"] = compute_row_hash(chunk, exclude_cols=["fuente_archivo"])
                yield chunk
                buffer = []
    if buffer:
        chunk = pd.DataFrame(buffer)
        chunk["row_hash"] = compute_row_hash(chunk, exclude_cols=["fuente_archivo"])
        yield chunk
