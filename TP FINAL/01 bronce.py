# Databricks notebook source
# MAGIC %md
# MAGIC # 1 · Bronce — SEPA
# MAGIC
# MAGIC Bronce conserva el dato recibido: no corrige nada. Sólo hace legible el contenedor
# MAGIC (zip diario → zip por comercio → CSV) y agrega trazabilidad.
# MAGIC
# MAGIC | Tabla | Qué es |
# MAGIC |---|---|
# MAGIC | `bronze_sepa_lines` | Una fila por línea de CSV, sin parsear |
# MAGIC | `bronze_sepa_audit` | Diagnóstico por CSV: líneas, encabezado, columnas, caracteres inválidos |
# MAGIC | `bronze_zip_files` (vista) | Un registro por zip diario recibido |
# MAGIC | `bronze_sepa_errors` (vista) | Zips o CSVs ilegibles, con el motivo |
# MAGIC
# MAGIC La ingesta es **incremental**: Auto Loader recuerda en su checkpoint qué zips ya leyó,
# MAGIC así que volver a ejecutar el notebook no duplica nada.

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace", "01 - Catálogo")
dbutils.widgets.text("schema", "sepa_tp", "02 - Esquema")

# COMMAND ----------

# MAGIC %run "./00 config"

# COMMAND ----------

import io
import zipfile

import pandas as pd
from pyspark.sql import functions as F

cfg = SepaConfig(dbutils.widgets.get("catalog"), dbutils.widgets.get("schema"))
create_namespace(spark, cfg)

T_LINES = cfg.table("bronze_sepa_lines")
T_AUDIT = cfg.table("bronze_sepa_audit")
T_ZIPS = cfg.table("bronze_zip_files")
T_ERRORS = cfg.table("bronze_sepa_errors")

print("Entrada :", cfg.incoming)
print("Destino :", cfg.namespace)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Lectura de los zips
# MAGIC
# MAGIC Un zip ilegible no tira abajo la ingesta: genera una fila con `ingest_status` distinto de `ok`
# MAGIC y el motivo, y la lectura sigue con el resto.

# COMMAND ----------

BATCH = 50_000
SOURCE_NAME = "sepa_zip_diario"

COLS = ["source_zip", "inner_zip", "file_name", "line_no", "raw_line", "ingest_status", "ingest_error"]
OUT_SCHEMA = ("source_zip string, inner_zip string, file_name string, line_no long, "
              "raw_line string, ingest_status string, ingest_error string")

# Nombre interno: sepa_1_comercio-sepa-62_2026-10-22_09-05-10.zip
RE_FECHA = r"(\d{4}-\d{2}-\d{2})"
RE_EXPORT = r"(\d{4}-\d{2}-\d{2})_(\d{2})-(\d{2})-(\d{2})"
RE_COMERCIO = r"comercio-sepa-(\d+)_"
RE_PREFIJO = r"(sepa_\d+)_comercio"


def _frame(rows):
    return pd.DataFrame(rows, columns=COLS).astype({"line_no": "int64"})


def _error(path, inner_name, file_name, status, exc):
    return _frame([(path, inner_name, file_name, -1, None, status, f"{type(exc).__name__}: {exc}"[:500])])


def iter_zip_rows(path, blob, batch=BATCH):
    """Recorre un zip diario (bytes) y emite DataFrames de hasta `batch` líneas."""
    try:
        outer = zipfile.ZipFile(io.BytesIO(bytes(blob)))
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
                            # utf-8-sig quita el BOM; newline=None unifica CRLF y LF
                            text = io.TextIOWrapper(handle, encoding="utf-8-sig", errors="replace", newline=None)
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
    """mapInPandas: recibe lotes con `path` y `content` y emite las líneas de cada zip."""
    for pdf in batches:
        for path, blob in zip(pdf["path"], pdf["content"]):
            yield from iter_zip_rows(path, blob)


def _extract_or_null(column, pattern, group=1):
    value = F.regexp_extract(column, pattern, group)
    return F.when(F.length(value) > 0, value)


def add_bronze_metadata(df):
    """Trazabilidad: se deriva del nombre del archivo, sin tocar el contenido."""
    export_ts = F.when(
        F.col("inner_zip").rlike(RE_EXPORT),
        F.concat_ws(" ",
                    F.regexp_extract("inner_zip", RE_EXPORT, 1),
                    F.concat_ws(":",
                                F.regexp_extract("inner_zip", RE_EXPORT, 2),
                                F.regexp_extract("inner_zip", RE_EXPORT, 3),
                                F.regexp_extract("inner_zip", RE_EXPORT, 4))))
    return (df
            .withColumn("fecha", _extract_or_null("inner_zip", RE_FECHA))
            .withColumn("id_comercio", _extract_or_null("inner_zip", RE_COMERCIO))
            .withColumn("prefijo_fuente", _extract_or_null("inner_zip", RE_PREFIJO))   # sepa_1 / sepa_2
            .withColumn("_export_str", export_ts)
            .withColumn("fecha_export", F.expr("try_to_timestamp(_export_str)"))       # cuándo se generó el export
            .drop("_export_str")
            .withColumn("_source", F.lit(SOURCE_NAME))
            .withColumn("ingested_at", F.current_timestamp()))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Ingesta incremental con Auto Loader
# MAGIC
# MAGIC `trigger(availableNow=True)`: procesa los zips pendientes y termina. El checkpoint guarda cuáles ya se leyeron.
# MAGIC Se procesa un zip por microbatch para acotar la memoria.

# COMMAND ----------

def run_bronze():
    stream = (spark.readStream.format("cloudFiles")
              .option("cloudFiles.format", "binaryFile")
              .option("cloudFiles.maxFilesPerTrigger", 1)
              .option("pathGlobFilter", "*.zip")
              .load(cfg.incoming)
              .select("path", "content")
              .mapInPandas(expand, schema=OUT_SCHEMA))
    query = (add_bronze_metadata(stream).writeStream
             .option("checkpointLocation", cfg.checkpoint("bronze_sepa_lines"))
             .trigger(availableNow=True)
             .toTable(T_LINES))
    query.awaitTermination()


before = table_count(spark, T_LINES)
run_bronze()
added = table_count(spark, T_LINES) - before
print(f"Líneas nuevas en Bronce: {added:,} · total: {table_count(spark, T_LINES):,}")
assert spark.catalog.tableExists(T_LINES), f"No se creó {T_LINES}: ¿hay zips en {cfg.incoming}?"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Prueba de idempotencia
# MAGIC
# MAGIC Sin archivos nuevos, una segunda ejecución no debe agregar nada.

# COMMAND ----------

before = table_count(spark, T_LINES)
run_bronze()
again = table_count(spark, T_LINES) - before
print(f"Reejecución sin archivos nuevos → líneas nuevas: {again}")
assert again == 0, "La reejecución agregó filas: revisá el checkpoint"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Diagnóstico de calidad (se mide, no se corrige)

# COMMAND ----------

lines = spark.table(T_LINES)

audit = (lines.filter("ingest_status = 'ok'")
         .groupBy("source_zip", "inner_zip", "file_name", "fecha", "id_comercio", "prefijo_fuente", "fecha_export")
         .agg(F.count("*").alias("lineas_totales"),
              F.sum(F.when(F.col("line_no") > 0, 1).otherwise(0)).alias("lineas_de_datos"),
              F.max(F.when(F.col("line_no") == 0, F.col("raw_line"))).alias("encabezado"),
              F.sum(F.col("raw_line").contains("\ufffd").cast("int")).alias("lineas_con_caracteres_invalidos"))
         .withColumn("columnas_encabezado", F.size(F.split("encabezado", r"\|")))
         .withColumn("schema_hash", F.md5("encabezado")))      # cambia si cambia el esquema del CSV
audit.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(T_AUDIT)

spark.sql(f"""CREATE OR REPLACE VIEW {T_ERRORS} AS
SELECT source_zip, inner_zip, file_name, ingest_status, ingest_error, ingested_at
FROM {T_LINES} WHERE ingest_status <> 'ok'""")

spark.sql(f"""CREATE OR REPLACE VIEW {T_ZIPS} AS
SELECT source_zip, min(fecha) AS fecha, count(DISTINCT inner_zip) AS zips_de_comercio,
       count(*) AS lineas, min(ingested_at) AS ingested_at
FROM {T_LINES} GROUP BY source_zip""")

display(spark.table(T_AUDIT).groupBy("file_name", "columnas_encabezado", "schema_hash")
        .agg(F.count("*").alias("archivos"), F.sum("lineas_de_datos").alias("lineas_de_datos"))
        .orderBy("file_name", "columnas_encabezado"))

# COMMAND ----------

# MAGIC %md
# MAGIC Si un mismo `file_name` aparece con más de un `schema_hash`, hay comercios que mandan otro esquema
# MAGIC (Silver los va a separar en cuarentena). Zips o CSVs que no se pudieron leer:

# COMMAND ----------

display(spark.table(T_ERRORS))
display(spark.table(T_ZIPS))

# COMMAND ----------

# Controles finales
errors = spark.table(T_ERRORS).count()
ok_files = spark.table(T_AUDIT).count()
print(f"CSV leídos: {ok_files} · errores de lectura: {errors}")
assert ok_files > 0, "No se leyó ningún CSV"
