# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 1b · Bronce de referencias — SEPA
# MAGIC
# MAGIC Igual que `01_bronce`: guarda lo recibido **sin corregir**. Acá entran dos fuentes de referencia:
# MAGIC
# MAGIC | Tabla | Fuente | Qué guarda |
# MAGIC |---|---|---|
# MAGIC | `bronze_ref_maestro_provincias` | `maestro-provincias.xlsx` (Volume) | Código ISO 3166-2 y nombre de cada provincia, tal cual el Excel |
# MAGIC | `bronze_georef_raw` | API Georef (`apis.datos.gob.ar`) | La respuesta JSON cruda de `provincias` y `departamentos`, página por página |
# MAGIC
# MAGIC **Idempotente:** el Excel se carga sólo si cambió su SHA-256; la API se guarda sólo si la respuesta cambió
# MAGIC respecto de la última exitosa. Si la API no responde, se registra el error y el pipeline sigue
# MAGIC (Silver funciona con el Excel solo).
# MAGIC
# MAGIC **Antes de ejecutar:** subir `maestro-provincias.xlsx` a la carpeta de referencias del Volume
# MAGIC (por defecto, `.../landing/referencias/`, al lado de `incoming/`).

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace", "01 - Catálogo")
dbutils.widgets.text("schema", "sepa_tp", "02 - Esquema")
dbutils.widgets.text("ref_dir", "", "03 - Carpeta de referencias (vacío = ../referencias)")

# COMMAND ----------

# MAGIC %run "./00 config"

# COMMAND ----------

import hashlib
import json
import os
import time
import uuid

import pandas as pd
import requests
from pyspark.sql import functions as F

cfg = SepaConfig(dbutils.widgets.get("catalog"), dbutils.widgets.get("schema"))
create_namespace(spark, cfg)

REF_DIR = dbutils.widgets.get("ref_dir").strip() or cfg.incoming.rstrip("/").rsplit("/", 1)[0] + "/referencias"
MAESTRO_FILE = "maestro-provincias.xlsx"
GEOREF_URL = "https://apis.datos.gob.ar/georef/api/v2.0"

T_MAESTRO = cfg.table("bronze_ref_maestro_provincias")
T_GEOREF = cfg.table("bronze_georef_raw")

print("Referencias:", REF_DIR)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Maestro de provincias (Excel)

# COMMAND ----------

def sha256_file(path):
    h = hashlib.sha256()
    with open(path, "rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def cargar_maestro():
    path = f"{REF_DIR}/{MAESTRO_FILE}"
    assert os.path.exists(path), f"Falta {path}. Subí el Excel a esa carpeta del Volume."
    sha = sha256_file(path)
    ya_cargado = (spark.catalog.tableExists(T_MAESTRO)
                  and spark.table(T_MAESTRO).filter(F.col("file_sha256") == sha).limit(1).count() > 0)
    if ya_cargado:
        print("Maestro sin cambios (mismo SHA-256): no se agrega nada.")
        return

    pdf = pd.read_excel(path, sheet_name=0, dtype=str)
    pdf.columns = [str(c).strip() for c in pdf.columns]
    faltan = {"sucursales_provincia", "provincia"} - set(pdf.columns)
    assert not faltan, f"Al Excel le faltan columnas: {sorted(faltan)}"

    filas = [(None if pd.isna(a) else a, None if pd.isna(b) else b)
             for a, b in zip(pdf["sucursales_provincia"], pdf["provincia"])]
    df = (spark.createDataFrame(filas, "sucursales_provincia string, provincia string")
          .withColumn("source_file", F.lit(MAESTRO_FILE))
          .withColumn("file_sha256", F.lit(sha))
          .withColumn("ingested_at", F.current_timestamp()))
    df.write.mode("append").saveAsTable(T_MAESTRO)
    print(f"Maestro cargado: {len(filas)} filas (sha {sha[:12]}…)")


cargar_maestro()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. API Georef (respuesta cruda)
# MAGIC
# MAGIC Se pide con `campos=completo` y, si la API lo rechaza, sin ese parámetro. La respuesta se guarda entera
# MAGIC como texto: Silver decide qué campos usar. Se pagina con `inicio` hasta traer el `total`.

# COMMAND ----------

GEOREF_SCHEMA = ("fetch_id string, resource string, params_json string, page_no int, http_status int, "
                 "response_json string, fetch_status string, fetch_hash string")


def http_get(resource, params, retries=3):
    """GET con reintentos. Devuelve (status, texto); status -1 = error de red."""
    ultimo = (-1, "sin intentos")
    for k in range(retries):
        try:
            r = requests.get(f"{GEOREF_URL}/{resource}", params=params, timeout=30)
            if r.status_code in (429, 500, 502, 503, 504):
                ultimo = (r.status_code, r.text[:2000])
                time.sleep(2 ** k)
                continue
            return r.status_code, r.text
        except requests.RequestException as exc:
            ultimo = (-1, f"{type(exc).__name__}: {exc}"[:2000])
            time.sleep(2 ** k)
    return ultimo


def fetch_all(resource, base_params, page_size=500, max_pages=50):
    pages, inicio = [], 0
    while len(pages) < max_pages:
        params = {**base_params, "max": page_size, "inicio": inicio}
        status, text = http_get(resource, params)
        pages.append((params, status, text))
        if status != 200:
            break
        body = json.loads(text)
        got, total = body.get("cantidad", 0), body.get("total", 0)
        inicio += got
        if got == 0 or inicio >= total:
            break
    return pages


def todas_ok(pages):
    return all(status == 200 for _, status, _ in pages)


def fetch_con_fallback(resource):
    pages = fetch_all(resource, {"campos": "completo"})
    if todas_ok(pages):
        return pages
    alternativa = fetch_all(resource, {})
    return alternativa if todas_ok(alternativa) else pages


def guardar_fetch(resource, pages):
    ok = todas_ok(pages)
    fetch_hash = hashlib.sha256("".join(t for _, _, t in pages).encode()).hexdigest() if ok else None
    if ok and spark.catalog.tableExists(T_GEOREF):
        ultimo = (spark.table(T_GEOREF)
                  .filter((F.col("resource") == resource) & (F.col("fetch_status") == "ok"))
                  .orderBy(F.desc("fetched_at")).select("fetch_hash").first())
        if ultimo is not None and ultimo["fetch_hash"] == fetch_hash:
            print(f"{resource}: sin cambios respecto de la última carga.")
            return "sin_cambios"
    fetch_id = str(uuid.uuid4())
    filas = [(fetch_id, resource, json.dumps(p), i, s, t, "ok" if ok else "error", fetch_hash)
             for i, (p, s, t) in enumerate(pages)]
    (spark.createDataFrame(filas, GEOREF_SCHEMA)
     .withColumn("fetched_at", F.current_timestamp())
     .write.mode("append").saveAsTable(T_GEOREF))
    print(f"{resource}: {'OK' if ok else 'ERROR'} · {len(pages)} página(s) guardadas")
    return "ok" if ok else "error"


resultados = {r: guardar_fetch(r, fetch_con_fallback(r)) for r in ("provincias", "departamentos")}
resultados

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Controles

# COMMAND ----------

mae_all = spark.table(T_MAESTRO)
ultimo_sha = mae_all.orderBy(F.desc("ingested_at")).select("file_sha256").first()["file_sha256"]
mae = mae_all.filter(F.col("file_sha256") == ultimo_sha)
n_prov, n_codigos = mae.count(), mae.select("sucursales_provincia").distinct().count()
print(f"Maestro vigente: {n_prov} filas · {n_codigos} códigos distintos")
assert n_prov == n_codigos == 24, "El maestro debería tener las 24 jurisdicciones, sin repetidas"

if spark.catalog.tableExists(T_GEOREF):
    display(spark.table(T_GEOREF).groupBy("resource", "fetch_status")
            .agg(F.max("fetched_at").alias("ultima_carga"), F.count("*").alias("paginas"))
            .orderBy("resource", "fetch_status"))
    sin_ok = [r for r in ("provincias", "departamentos")
              if spark.table(T_GEOREF).filter((F.col("resource") == r) & (F.col("fetch_status") == "ok")).count() == 0]
    if sin_ok:
        print(f"⚠ Sin respuesta OK de Georef para: {sin_ok}. Silver sigue con el Excel; reintentá más tarde "
              "o revisá que el cómputo tenga salida a internet.")
else:
    print("⚠ No hay datos de Georef todavía (la API no respondió).")
