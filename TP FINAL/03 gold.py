# Databricks notebook source
# MAGIC %md
# MAGIC # 3 · Gold — SEPA
# MAGIC
# MAGIC Gold publica productos de datos listos para consumir, con el grano explícito. Se reconstruyen
# MAGIC completos desde Silver, así que reejecutar siempre da el mismo resultado.
# MAGIC
# MAGIC | Tabla | Grano | Para qué sirve |
# MAGIC |---|---|---|
# MAGIC | `gold_precios_producto_dia` | fecha + producto (EAN) | Comparar el precio de un mismo producto entre comercios |
# MAGIC | `gold_precios_provincia_dia` | fecha + provincia + producto (EAN) | Comparar precios entre provincias |
# MAGIC | `gold_resumen_comercio_dia` | fecha + comercio | Cobertura y calidad de lo que informa cada comercio |
# MAGIC
# MAGIC Sólo se comparan productos con código EAN (`es_ean = true`): son los que identifican el mismo
# MAGIC producto en distintos comercios. El precio es el unitario con IVA.

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace", "01 - Catálogo")
dbutils.widgets.text("schema", "sepa_tp", "02 - Esquema")

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

from pyspark.sql import functions as F

cfg = SepaConfig(dbutils.widgets.get("catalog"), dbutils.widgets.get("schema"))
create_namespace(spark, cfg)

T_PROD = cfg.table("silver_productos")
T_QUAR = cfg.table("silver_productos_quarantine")
T_SUC = cfg.table("silver_sucursales")
T_COM = cfg.table("silver_comercio")
G_PRODUCTO = cfg.table("gold_precios_producto_dia")
G_PROVINCIA = cfg.table("gold_precios_provincia_dia")
G_COMERCIO = cfg.table("gold_resumen_comercio_dia")

for table in [T_PROD, T_QUAR, T_SUC, T_COM]:
    assert spark.catalog.tableExists(table), f"Falta {table}. Ejecutá primero 02_silver."

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Definición de los productos Gold

# COMMAND ----------

PRECIO = "precio_unit_con_iva"


def gold_precios_producto_dia(prod):
    return (prod.filter("es_ean")
            .groupBy("fecha", "id_producto")
            .agg(F.max("descripcion").alias("descripcion"),
                 F.max("marca").alias("marca"),
                 F.countDistinct("id_comercio").alias("comercios"),
                 F.countDistinct("id_comercio", "id_sucursal").alias("sucursales"),
                 F.round(F.min(PRECIO), 2).alias("precio_min"),
                 F.round(F.max(PRECIO), 2).alias("precio_max"),
                 F.round(F.avg(PRECIO), 2).alias("precio_promedio"),
                 F.round(F.percentile_approx(PRECIO, 0.5), 2).alias("precio_mediana"))
            .withColumn("dispersion", F.round(F.col("precio_max") / F.col("precio_min") - 1, 4)))


def gold_precios_provincia_dia(prod, suc):
    provincias = suc.select("id_comercio", "id_sucursal", "provincia")
    return (prod.filter("es_ean")
            .join(provincias, ["id_comercio", "id_sucursal"], "left")
            .withColumn("provincia", F.coalesce("provincia", F.lit("sin_dato")))
            .groupBy("fecha", "provincia", "id_producto")
            .agg(F.max("descripcion").alias("descripcion"),
                 F.countDistinct("id_comercio").alias("comercios"),
                 F.countDistinct("id_comercio", "id_sucursal").alias("sucursales"),
                 F.round(F.min(PRECIO), 2).alias("precio_min"),
                 F.round(F.max(PRECIO), 2).alias("precio_max"),
                 F.round(F.avg(PRECIO), 2).alias("precio_promedio")))


def gold_resumen_comercio_dia(prod, quar, com):
    """filas_aceptadas = filas únicas en Silver; filas_rechazadas = líneas en cuarentena.
    Los duplicados descartados no cuentan en ninguna de las dos."""
    aceptadas = (prod.groupBy("fecha", "id_comercio")
                 .agg(F.count("*").alias("filas_aceptadas"),
                      F.countDistinct("id_sucursal").alias("sucursales_con_precios"),
                      F.countDistinct("id_producto").alias("productos"),
                      F.round(F.avg(PRECIO), 2).alias("precio_promedio")))
    rechazadas = (quar.filter("id_comercio IS NOT NULL")
                  .groupBy("fecha", "id_comercio")
                  .agg(F.count("*").alias("filas_rechazadas")))
    nombres = com.select("id_comercio", "razon_social", "bandera_nombre")
    return (aceptadas.join(rechazadas, ["fecha", "id_comercio"], "full_outer")
            .join(nombres, "id_comercio", "left")
            .withColumn("filas_aceptadas", F.coalesce("filas_aceptadas", F.lit(0)))
            .withColumn("filas_rechazadas", F.coalesce("filas_rechazadas", F.lit(0)))
            .withColumn("tasa_rechazo",
                        F.round(F.col("filas_rechazadas") / (F.col("filas_aceptadas") + F.col("filas_rechazadas")), 4)))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Construcción

# COMMAND ----------

prod, quar = spark.table(T_PROD), spark.table(T_QUAR)
suc, com = spark.table(T_SUC), spark.table(T_COM)

outputs = {
    G_PRODUCTO: (gold_precios_producto_dia(prod), "Precio de cada producto (EAN) por día: mínimo, máximo, promedio y mediana entre comercios"),
    G_PROVINCIA: (gold_precios_provincia_dia(prod, suc), "Precio de cada producto (EAN) por día y provincia"),
    G_COMERCIO: (gold_resumen_comercio_dia(prod, quar, com), "Cobertura y calidad por comercio y día"),
}
for table, (frame, comment) in outputs.items():
    frame.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(table)
    spark.sql(f"COMMENT ON TABLE {table} IS '{comment}'")
    print(f"{table}: {spark.table(table).count():,} filas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Controles
# MAGIC
# MAGIC Gold tiene que reconciliar con Silver y respetar su grano.

# COMMAND ----------

g_producto, g_provincia, g_comercio = (spark.table(t) for t in (G_PRODUCTO, G_PROVINCIA, G_COMERCIO))

silver_total = prod.count()
quarantine_total = quar.filter("id_comercio IS NOT NULL").count()
checks = {
    "grano_producto_unico": g_producto.count() == g_producto.select("fecha", "id_producto").distinct().count(),
    "grano_provincia_unico": g_provincia.count() == g_provincia.select("fecha", "provincia", "id_producto").distinct().count(),
    "grano_comercio_unico": g_comercio.count() == g_comercio.select("fecha", "id_comercio").distinct().count(),
    "comercio_reconcilia_aceptadas": g_comercio.agg(F.sum("filas_aceptadas")).first()[0] == silver_total,
    "comercio_reconcilia_rechazadas": g_comercio.agg(F.sum("filas_rechazadas")).first()[0] == quarantine_total,
    "precios_ordenados": g_producto.filter("precio_min > precio_max").count() == 0,
}
display(spark.createDataFrame([(k, bool(v)) for k, v in checks.items()], ["control", "ok"]))
failed = [k for k, v in checks.items() if not v]
assert not failed, f"Fallaron controles: {failed}"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Consultas para verificar el resultado
# MAGIC
# MAGIC **¿Qué productos tienen mayor diferencia de precio entre comercios?** (sólo los que informan al menos 2 comercios)

# COMMAND ----------

display(spark.table(G_PRODUCTO).filter("comercios >= 2")
        .orderBy(F.desc("dispersion")).limit(15)
        .select("fecha", "id_producto", "descripcion", "comercios", "precio_min", "precio_max", "dispersion"))

# COMMAND ----------

# MAGIC %md
# MAGIC **¿Cómo cambia el precio promedio de un mismo producto según la provincia?** (el producto presente en más provincias)

# COMMAND ----------

top = (spark.table(G_PROVINCIA).groupBy("id_producto").agg(F.countDistinct("provincia").alias("provincias"))
       .orderBy(F.desc("provincias"), "id_producto").first())
print("Producto:", top["id_producto"], "· provincias:", top["provincias"])
display(spark.table(G_PROVINCIA).filter(F.col("id_producto") == top["id_producto"])
        .orderBy("precio_promedio").select("fecha", "provincia", "descripcion", "comercios", "precio_promedio"))

# COMMAND ----------

# MAGIC %md
# MAGIC **¿Qué comercios informan datos de peor calidad?**

# COMMAND ----------

display(spark.table(G_COMERCIO).orderBy(F.desc("tasa_rechazo"))
        .select("fecha", "id_comercio", "razon_social", "filas_aceptadas", "filas_rechazadas", "tasa_rechazo"))
