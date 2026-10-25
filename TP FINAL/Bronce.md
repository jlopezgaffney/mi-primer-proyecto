import io, zipfile
import pandas as pd
import dlt
from pyspark.sql import functions as F

LANDING = "/Volumes/workspace/sepa_tp/landing/incoming"
BATCH = 20000

OUT_SCHEMA = "source_zip string, inner_zip string, file_name string, line_no long, raw_line string"

def expand(batches):
    # Generador: lee un CSV a la vez, en streaming, y emite lotes chicos
    for pdf in batches:
        for path, blob in zip(pdf["path"], pdf["content"]):
            with zipfile.ZipFile(io.BytesIO(blob)) as outer:
                for inner_name in outer.namelist():
                    if not inner_name.endswith(".zip"):
                        continue
                    with zipfile.ZipFile(io.BytesIO(outer.read(inner_name))) as inner:
                        for member in inner.namelist():
                            fname = member.split("/")[-1]
                            if not fname.endswith(".csv"):
                                continue
                            rows = []
                            with inner.open(member) as fh:
                                text = io.TextIOWrapper(fh, encoding="utf-8-sig",
                                                        errors="replace", newline=None)
                                for i, line in enumerate(text):
                                    rows.append((path, inner_name, fname, i, line.rstrip("\n")))
                                    if len(rows) >= BATCH:
                                        yield pd.DataFrame(rows, columns=["source_zip","inner_zip","file_name","line_no","raw_line"])
                                        rows = []
                            if rows:
                                yield pd.DataFrame(rows, columns=["source_zip","inner_zip","file_name","line_no","raw_line"])

@dlt.table(name="bronze_zip_files", comment="Zips crudos desde landing")
def bronze_zip_files():
    return (spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "binaryFile")
            .option("cloudFiles.maxFilesPerTrigger", 1)
            .load(LANDING)
            .select("path", "modificationTime", "length", "content"))

@dlt.table(name="bronze_sepa_lines", comment="Una fila por línea de CSV, sin parsear")
def bronze_sepa_lines():
    return (dlt.read_stream("bronze_zip_files")
            .select("path", "content")
            .mapInPandas(expand, schema=OUT_SCHEMA)
            .withColumn("line_no", F.col("line_no").cast("int"))
            .withColumn("fecha", F.regexp_extract("inner_zip", r"(\d{4}-\d{2}-\d{2})", 1))
            .withColumn("id_comercio", F.regexp_extract("inner_zip", r"comercio-sepa-(\d+)_", 1))
            .withColumn("ingested_at", F.current_timestamp()))
