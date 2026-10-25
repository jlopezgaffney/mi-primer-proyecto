"""BRONZE — SEPA.

Bronze conserva el dato recibido: no corrige nada, sólo hace legible el contenedor
(zip diario -> zip por comercio -> CSV) y agrega trazabilidad. Los problemas se
miden y se registran acá; se corrigen recién en Silver.

Tablas que publica:
  bronze_zip_files   manifiesto: un registro por zip diario recibido (sin el binario)
  bronze_sepa_lines  una fila por línea de CSV, sin parsear
  bronze_sepa_audit  diagnóstico por CSV (cuántas líneas, qué encabezado, caracteres inválidos)
  bronze_sepa_errors zips o CSVs que no se pudieron leer, con el motivo
"""

import io
import zipfile

import dlt
import pandas as pd
from pyspark.sql import functions as F

# ---------------------------------------------------------------------------
# Configuración (se puede cambiar desde la configuración del pipeline, sin tocar el código)
#   sepa.landing     carpeta del volumen donde caen los zips diarios
#   sepa.batch_rows  filas por lote que emite el generador (controla la memoria)
# ---------------------------------------------------------------------------
LANDING = spark.conf.get("sepa.landing", "/Volumes/workspace/sepa_tp/landing/incoming")
BATCH = int(spark.conf.get("sepa.batch_rows", "50000"))
SOURCE_NAME = "sepa_zip_diario"

COLS = ["source_zip", "inner_zip", "file_name", "line_no", "raw_line", "ingest_status", "ingest_error"]
OUT_SCHEMA = ("source_zip string, inner_zip string, file_name string, line_no long, "
              "raw_line string, ingest_status string, ingest_error string")

# Nombre interno: sepa_1_comercio-sepa-62_2026-10-22_09-05-10.zip
RE_FECHA = r"(\d{4}-\d{2}-\d{2})"
RE_EXPORT = r"(\d{4}-\d{2}-\d{2})_(\d{2})-(\d{2})-(\d{2})"
RE_COMERCIO = r"comercio-sepa-(\d+)_"
RE_PREFIJO = r"(sepa_\d+)_comercio"


# ---------------------------------------------------------------------------
# Lectura de los zips (Python puro: se puede probar sin Spark)
# ---------------------------------------------------------------------------
def local_path(path: str) -> str:
    """Auto Loader entrega 'dbfs:/Volumes/...'; zipfile necesita la ruta local."""
    return path[len("dbfs:"):] if path.startswith("dbfs:") else path


def _frame(rows):
    return pd.DataFrame(rows, columns=COLS).astype({"line_no": "int64"})


def _error(path, inner_name, file_name, status, exc):
    """Una fila que conserva el motivo del fallo en lugar de tirar abajo el pipeline."""
    return _frame([(path, inner_name, file_name, -1, None, status, f"{type(exc).__name__}: {exc}"[:500])])


def iter_zip_rows(path: str, batch: int = BATCH):
    """Recorre un zip diario y emite DataFrames de hasta `batch` líneas.

    El zip externo se abre desde el archivo (sin cargarlo entero en memoria); cada zip
    interno de comercio sí se carga, porque zipfile necesita poder hacer seek.
    Un zip o CSV ilegible genera una fila con ingest_status != 'ok' y el proceso sigue.
    """
    try:
        outer = zipfile.ZipFile(local_path(path))
    except Exception as exc:  # noqa: BLE001
        yield _error(path, "", "", "zip_externo_ilegible", exc)
        return

    with outer:
        for inner_name in outer.namelist():
            if not inner_name.lower().endswith(".zip"):
                continue
            try:
                with outer.open(inner_name) as handle:
                    inner = zipfile.ZipFile(io.BytesIO(handle.read()))
            except Exception as exc:  # noqa: BLE001
                yield _error(path, inner_name, "", "zip_interno_ilegible", exc)
                continue

            with inner:
                for member in inner.namelist():
                    file_name = member.split("/")[-1]
                    if not file_name.lower().endswith(".csv"):
                        continue
                    rows = []
                    try:
                        with inner.open(member) as handle:
                            # utf-8-sig quita el BOM; newline=None unifica CRLF/LF
                            text = io.TextIOWrapper(handle, encoding="utf-8-sig",
                                                    errors="replace", newline=None)
                            for i, line in enumerate(text):
                                rows.append((path, inner_name, file_name, i, line.rstrip("\n"), "ok", None))
                                if len(rows) >= batch:
                                    yield _frame(rows)
                                    rows = []
                        if rows:
                            yield _frame(rows)
                    except Exception as exc:  # noqa: BLE001
                        if rows:
                            yield _frame(rows)
                        yield _error(path, inner_name, file_name, "csv_ilegible", exc)


def expand(batches):
    """mapInPandas: recibe lotes con la columna `path` y emite las líneas de cada zip."""
    for pdf in batches:
        for path in pdf["path"]:
            yield from iter_zip_rows(path)


def _extract_or_null(column, pattern, group=1):
    """regexp_extract devuelve '' cuando no hay match; en Bronze preferimos NULL."""
    value = F.regexp_extract(column, pattern, group)
    return F.when(F.length(value) > 0, value)


# ---------------------------------------------------------------------------
# 1. Manifiesto de zips recibidos
# ---------------------------------------------------------------------------
@dlt.table(
    name="bronze_zip_files",
    comment="Manifiesto: un registro por zip diario recibido. No guarda el contenido binario.",
    table_properties={"quality": "bronze"},
)
@dlt.expect("zip_no_vacio", "length > 0")
def bronze_zip_files():
    return (spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "binaryFile")
            .option("cloudFiles.maxFilesPerTrigger", 1)      # un zip diario por microbatch
            .option("pathGlobFilter", "*.zip")
            .load(LANDING)
            .select("path", "modificationTime", "length")    # sin `content`: no se lee ni se guarda el binario
            .withColumn("_source", F.lit(SOURCE_NAME))
            .withColumn("ingested_at", F.current_timestamp()))


# ---------------------------------------------------------------------------
# 2. Una fila por línea de CSV, sin parsear
# ---------------------------------------------------------------------------
@dlt.table(
    name="bronze_sepa_lines",
    comment="Una fila por línea de CSV, tal como llegó, con su origen y metadatos de ingesta.",
    table_properties={"quality": "bronze"},
)
@dlt.expect("zip_legible", "ingest_status = 'ok'")           # expect sin DROP: Bronze no descarta
@dlt.expect("fecha_presente", "fecha IS NOT NULL")
@dlt.expect("id_comercio_presente", "id_comercio IS NOT NULL")
def bronze_sepa_lines():
    export_ts = F.when(
        F.col("inner_zip").rlike(RE_EXPORT),
        F.concat_ws(" ",
                    F.regexp_extract("inner_zip", RE_EXPORT, 1),
                    F.concat_ws(":",
                                F.regexp_extract("inner_zip", RE_EXPORT, 2),
                                F.regexp_extract("inner_zip", RE_EXPORT, 3),
                                F.regexp_extract("inner_zip", RE_EXPORT, 4))))
    return (dlt.read_stream("bronze_zip_files")
            .select("path")
            .mapInPandas(expand, schema=OUT_SCHEMA)
            .withColumn("fecha", _extract_or_null("inner_zip", RE_FECHA))
            .withColumn("id_comercio", _extract_or_null("inner_zip", RE_COMERCIO))
            .withColumn("prefijo_fuente", _extract_or_null("inner_zip", RE_PREFIJO))   # sepa_1 / sepa_2
            .withColumn("_export_str", export_ts)
            .withColumn("fecha_export", F.expr("try_to_timestamp(_export_str)"))       # cuándo se generó el export
            .drop("_export_str")
            .withColumn("_source", F.lit(SOURCE_NAME))
            .withColumn("ingested_at", F.current_timestamp()))


# ---------------------------------------------------------------------------
# 3. Diagnóstico inicial de calidad (se mide, no se corrige)
# ---------------------------------------------------------------------------
@dlt.table(
    name="bronze_sepa_audit",
    comment="Diagnóstico por CSV: líneas, encabezado, cantidad de columnas y caracteres inválidos.",
    table_properties={"quality": "bronze"},
)
def bronze_sepa_audit():
    b = dlt.read("bronze_sepa_lines").filter("ingest_status = 'ok'")
    audit = (b.groupBy("source_zip", "inner_zip", "file_name", "fecha", "id_comercio",
                       "prefijo_fuente", "fecha_export")
             .agg(F.count("*").alias("lineas_totales"),
                  F.sum(F.when(F.col("line_no") > 0, 1).otherwise(0)).alias("lineas_de_datos"),
                  F.max(F.when(F.col("line_no") == 0, F.col("raw_line"))).alias("encabezado"),
                  F.sum(F.col("raw_line").contains("\ufffd").cast("int")).alias("lineas_con_caracteres_invalidos")))
    return (audit
            .withColumn("columnas_encabezado", F.size(F.split("encabezado", r"\|")))
            .withColumn("schema_hash", F.md5("encabezado")))     # detecta cambios de esquema entre días/comercios


# ---------------------------------------------------------------------------
# 4. Lo que no se pudo leer
# ---------------------------------------------------------------------------
@dlt.table(
    name="bronze_sepa_errors",
    comment="Zips o CSVs ilegibles, con el motivo. Nada se pierde en silencio.",
    table_properties={"quality": "bronze"},
)
def bronze_sepa_errors():
    return (dlt.read("bronze_sepa_lines")
            .filter("ingest_status <> 'ok'")
            .select("source_zip", "inner_zip", "file_name", "ingest_status", "ingest_error", "ingested_at"))
