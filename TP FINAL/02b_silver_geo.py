# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 2b · Silver geo — SEPA
# MAGIC
# MAGIC Enriquece las sucursales con **georreferencia inversa** de Georef (`/ubicacion`): dado lat/lon devuelve
# MAGIC provincia, departamento y municipio.
# MAGIC
# MAGIC | Tabla | Qué es |
# MAGIC |---|---|
# MAGIC | `bronze_georef_ubicacion` | Caché cruda: una respuesta por coordenada consultada (sólo se consultan las nuevas) |
# MAGIC | `silver_sucursales_geo` | Una fila por sucursal: provincia/departamento/municipio según Georef, comparados con la provincia declarada |
# MAGIC
# MAGIC **Diseño:** el pipeline no depende de la API en cada corrida. Lo ya consultado se reutiliza; si la API falla,
# MAGIC las sucursales quedan como `pendiente` y se reintentan en la próxima ejecución. Corre **después** de `02_silver`.
# MAGIC
# MAGIC `provincia_final` usa la provincia declarada si es válida y, si no, la de Georef (`provincia_final_fuente`
# MAGIC indica de dónde salió).

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace", "01 - Catálogo")
dbutils.widgets.text("schema", "sepa_tp", "02 - Esquema")
dbutils.widgets.text("max_consultas", "300", "03 - Máx. consultas a la API por corrida")

# COMMAND ----------

# MAGIC %run "./00 config"

# COMMAND ----------

import time

import requests
from pyspark.sql import functions as F, Window

cfg = SepaConfig(dbutils.widgets.get("catalog"), dbutils.widgets.get("schema"))
create_namespace(spark, cfg)
MAX_CONSULTAS = int(dbutils.widgets.get("max_consultas") or 300)
GEOREF_URL = "https://apis.datos.gob.ar/georef/api/v2.0"

T_SUC = cfg.table("silver_sucursales")
T_REF_PROV = cfg.table("silver_ref_provincias")
T_UBI = cfg.table("bronze_georef_ubicacion")
T_SUC_GEO = cfg.table("silver_sucursales_geo")

for t in (T_SUC, T_REF_PROV):
    assert spark.catalog.tableExists(t), f"Falta {t}. Ejecutá primero 02_silver."

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Consultar sólo coordenadas nuevas

# COMMAND ----------

UBI_SCHEMA = "latitud double, longitud double, http_status int, response_json string, fetch_status string"


def http_get(path, params, retries=3):
    """GET con reintentos. Devuelve (status, texto); status -1 = error de red."""
    ultimo = (-1, "sin intentos")
    for k in range(retries):
        try:
            r = requests.get(f"{GEOREF_URL}/{path}", params=params, timeout=30)
            if r.status_code in (429, 500, 502, 503, 504):
                ultimo = (r.status_code, r.text[:2000])
                time.sleep(2 ** k)
                continue
            return r.status_code, r.text
        except requests.RequestException as exc:
            ultimo = (-1, f"{type(exc).__name__}: {exc}"[:2000])
            time.sleep(2 ** k)
    return ultimo


def guardar(filas):
    if filas:
        (spark.createDataFrame(filas, UBI_SCHEMA).withColumn("fetched_at", F.current_timestamp())
         .write.mode("append").saveAsTable(T_UBI))


candidatas = spark.table(T_SUC).filter("geo_valida").select("latitud", "longitud").distinct()
if spark.catalog.tableExists(T_UBI):
    ya_ok = spark.table(T_UBI).filter("fetch_status = 'ok'").select("latitud", "longitud").distinct()
    candidatas = candidatas.join(ya_ok, ["latitud", "longitud"], "left_anti")

pendientes = candidatas.limit(MAX_CONSULTAS).collect()
print(f"Coordenadas a consultar: {len(pendientes)} (tope {MAX_CONSULTAS})")

filas, ok_n, err_n, errores_red = [], 0, 0, 0
for r in pendientes:
    status, texto = http_get("ubicacion", {"lat": r["latitud"], "lon": r["longitud"]})
    es_ok = status == 200
    ok_n, err_n = ok_n + es_ok, err_n + (not es_ok)
    errores_red = 0 if status != -1 else errores_red + 1
    filas.append((r["latitud"], r["longitud"], status, texto, "ok" if es_ok else "error"))
    if len(filas) >= 100:
        guardar(filas)
        filas = []
    if errores_red >= 3:
        print("⚠ 3 errores de red seguidos: se corta. Revisá que el cómputo tenga salida a internet.")
        break
    time.sleep(0.15)
guardar(filas)
print(f"Consultas OK: {ok_n} · con error: {err_n}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. `silver_sucursales_geo`

# COMMAND ----------

UBICACION_SCHEMA = ("ubicacion struct<provincia:struct<id:string,nombre:string>,"
                    "departamento:struct<id:string,nombre:string>,municipio:struct<id:string,nombre:string>>")

suc = spark.table(T_SUC).select("id_comercio", "id_sucursal", "latitud", "longitud", "geo_valida",
                                "provincia", "provincia_estado")
prov_indec = (spark.table(T_REF_PROV).filter("id_indec IS NOT NULL")
              .select(F.col("id_indec").alias("provincia_geo_id"), F.col("iso_id").alias("provincia_geo_iso")))

if spark.catalog.tableExists(T_UBI):
    w = Window.partitionBy("latitud", "longitud").orderBy(F.desc("fetched_at"))
    ubi = (spark.table(T_UBI).filter("fetch_status = 'ok'")
           .withColumn("rn", F.row_number().over(w)).filter("rn = 1")
           .withColumn("j", F.from_json("response_json", UBICACION_SCHEMA))
           .select("latitud", "longitud", F.lit(True).alias("_respondio"),
                   F.col("j.ubicacion.provincia.id").alias("provincia_geo_id"),
                   F.col("j.ubicacion.provincia.nombre").alias("provincia_geo_nombre"),
                   F.col("j.ubicacion.departamento.id").alias("departamento_id"),
                   F.col("j.ubicacion.departamento.nombre").alias("departamento_nombre"),
                   F.col("j.ubicacion.municipio.id").alias("municipio_id"),
                   F.col("j.ubicacion.municipio.nombre").alias("municipio_nombre")))
else:
    ubi = (spark.createDataFrame([], "latitud double, longitud double")
           .select("latitud", "longitud", F.lit(True).alias("_respondio"),
                   F.lit(None).cast("string").alias("provincia_geo_id"),
                   F.lit(None).cast("string").alias("provincia_geo_nombre"),
                   F.lit(None).cast("string").alias("departamento_id"),
                   F.lit(None).cast("string").alias("departamento_nombre"),
                   F.lit(None).cast("string").alias("municipio_id"),
                   F.lit(None).cast("string").alias("municipio_nombre")))

geo = (suc.join(ubi, ["latitud", "longitud"], "left")
          .join(prov_indec, "provincia_geo_id", "left")
          .withColumn("georef_status",
                      F.when(~F.col("geo_valida"), "sin_coordenadas_validas")
                       .when(F.col("_respondio").isNull(), "pendiente")
                       .when(F.col("provincia_geo_id").isNull(), "fuera_de_argentina")
                       .otherwise("ok"))
          .withColumn("provincia_coherente",
                      F.when(F.col("provincia").isNotNull() & F.col("provincia_geo_iso").isNotNull(),
                             F.col("provincia") == F.col("provincia_geo_iso")))
          .withColumn("provincia_final", F.coalesce("provincia", "provincia_geo_iso"))
          .withColumn("provincia_final_fuente",
                      F.when(F.col("provincia").isNotNull(), "declarada")
                       .when(F.col("provincia_geo_iso").isNotNull(), "georef"))
          .drop("_respondio"))
geo.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(T_SUC_GEO)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Controles

# COMMAND ----------

n_suc, n_geo = spark.table(T_SUC).count(), spark.table(T_SUC_GEO).count()
print(f"silver_sucursales: {n_suc} · silver_sucursales_geo: {n_geo}")
assert n_suc == n_geo, "silver_sucursales_geo debería tener una fila por sucursal"

display(spark.table(T_SUC_GEO).groupBy("georef_status").count().orderBy(F.desc("count")))
display(spark.table(T_SUC_GEO).groupBy("provincia_final_fuente").count())
print("Sucursales cuya provincia declarada NO coincide con la de sus coordenadas:")
display(spark.table(T_SUC_GEO).filter("provincia_coherente = false")
        .select("id_comercio", "id_sucursal", "provincia", "provincia_geo_iso", "provincia_geo_nombre",
                "departamento_nombre", "latitud", "longitud"))
