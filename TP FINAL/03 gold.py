# Databricks notebook source
# MAGIC %md
# MAGIC # 3 · Gold — SEPA: inflación mayorista y minorista
# MAGIC
# MAGIC Gold arma series de tiempo de precios para medir la inflación de cada canal y, a medida que se acumulen
# MAGIC datos, estudiar cuánto de la inflación mayorista se traslada a la minorista.
# MAGIC
# MAGIC | Tabla | Grano | Para qué sirve |
# MAGIC |---|---|---|
# MAGIC | `gold_precio_item_dia` | fecha + formato + comercio + producto | Precio de cada producto de cada comercio, un valor por día |
# MAGIC | `gold_variacion_dia` | fecha + formato | Cuánto cambiaron los precios respecto de la fecha anterior |
# MAGIC | `gold_indice_precios_dia` | fecha + formato | Índice de precios (base 100 en la primera fecha) del canal mayorista y del minorista |
# MAGIC | `gold_inflacion_mensual` | mes + formato | Variación mensual del índice de cada canal |
# MAGIC | `gold_mayorista_vs_minorista_semana` | semana | Ambos índices lado a lado, con la inflación mayorista de las semanas anteriores (rezagos) |
# MAGIC | `gold_resumen_comercio_dia` | fecha + comercio | Cobertura y calidad de lo que informa cada comercio |
# MAGIC
# MAGIC ## Cómo se mide la inflación
# MAGIC
# MAGIC - **Mismo producto, mismo comercio.** Para cada comercio y producto se calcula su precio del día (la mediana entre
# MAGIC   sus sucursales) y se compara con su precio de la fecha anterior. Así la variación mide cambios de precio y no
# MAGIC   cambios en qué productos se relevaron.
# MAGIC - **Índice de Jevons.** La variación entre dos fechas es el promedio geométrico de los cambios de precio de todos los
# MAGIC   productos que están en ambas fechas. Las variaciones se encadenan: el índice arranca en 100 en la primera fecha y
# MAGIC   se multiplica por la variación de cada período. Todos los productos pesan igual (SEPA no informa ventas).
# MAGIC - **Un índice por formato.** El mayorista y el minorista se miden por separado, cada uno con sus propios productos.
# MAGIC   Como cada índice compara un precio contra sí mismo en el tiempo, no importa que el precio del formato mayorista
# MAGIC   (unitario dentro del bulto) y el del minorista (de lista) no sean el mismo concepto.
# MAGIC - **Precio de lista con IVA** (`precio_unit_con_iva`), sin promociones, para que una oferta pasajera no se lea como baja.
# MAGIC - **Cambios extremos excluidos.** Un producto cuyo precio se multiplica por más de 2 o por menos de 0,5 entre dos
# MAGIC   fechas casi seguro es un error de carga o un código repetido por otro producto. Se excluye de la variación y se
# MAGIC   cuenta en `productos_excluidos`.
# MAGIC
# MAGIC ## Qué se puede concluir
# MAGIC
# MAGIC Con pocos días de datos las tablas se pueden validar, pero no sacar conclusiones sobre inflación ni sobre traslado de
# MAGIC precios: eso requiere varios meses de historia. La tabla semanal ya trae los rezagos de la inflación mayorista, lista
# MAGIC para ajustar un modelo (por ejemplo una regresión de la inflación minorista contra la mayorista de las 1 a 4 semanas
# MAGIC anteriores) cuando haya semanas suficientes.
# MAGIC
# MAGIC ## Actualización incremental
# MAGIC
# MAGIC Cada corrida procesa sólo las fechas nuevas o modificadas en Silver, y recalcula las variaciones que dependen de ellas.
# MAGIC Los índices, las series mensual y semanal son tablas chicas y se reconstruyen completas.
# MAGIC Parámetros: `fecha` (recalcula un día) y `full_refresh = si` (borra las tablas de Gold y recalcula todo).

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace", "01 - Catálogo")
dbutils.widgets.text("schema", "sepa_tp", "02 - Esquema")
dbutils.widgets.text("fecha", "", "03 - Fecha (opcional)")
dbutils.widgets.dropdown("full_refresh", "no", ["no", "si"], "04 - Reconstruir todo")

# COMMAND ----------

# MAGIC %run "./00 config"

# COMMAND ----------

from pyspark.sql import functions as F, Window

cfg = SepaConfig(dbutils.widgets.get("catalog"), dbutils.widgets.get("schema"))
create_namespace(spark, cfg)
FECHA = dbutils.widgets.get("fecha").strip()
FULL = dbutils.widgets.get("full_refresh") == "si"
assert not (FULL and FECHA), "full_refresh y fecha son excluyentes"

T_PROD = cfg.table("silver_productos")
T_QUAR = cfg.table("silver_productos_quarantine")
T_COM = cfg.table("silver_comercio")
G_ITEM = cfg.table("gold_precio_item_dia")
G_VAR = cfg.table("gold_variacion_dia")
G_INDICE = cfg.table("gold_indice_precios_dia")
G_MES = cfg.table("gold_inflacion_mensual")
G_SEMANA = cfg.table("gold_mayorista_vs_minorista_semana")
G_COMERCIO = cfg.table("gold_resumen_comercio_dia")

for table in [T_PROD, T_QUAR, T_COM]:
    assert spark.catalog.tableExists(table), f"Falta {table}. Ejecutá primero 02 silver."

if FULL:
    for table in [G_ITEM, G_VAR, G_INDICE, G_MES, G_SEMANA, G_COMERCIO]:
        spark.sql(f"DROP TABLE IF EXISTS {table}")
    print("full_refresh: tablas de Gold eliminadas; se reconstruyen desde Silver.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Definición de las tablas

# COMMAND ----------

PRECIO = "precio_unit_con_iva"
REL_MIN, REL_MAX = 0.5, 2.0   # cambios de precio fuera de este rango se excluyen (error de carga o código repetido)
CLAVE_ITEM = ["formato_productos", "id_comercio", "id_producto"]


def precio_item_dia(prod):
    """Un precio por fecha, formato, comercio y producto: la mediana entre las sucursales del comercio."""
    return (prod.groupBy("fecha", *CLAVE_ITEM)
            .agg(F.max("id_gtin14").alias("id_gtin14"),
                 F.max("descripcion").alias("descripcion"),
                 F.countDistinct("id_sucursal").alias("sucursales"),
                 F.round(F.percentile_approx(F.col(PRECIO).cast("double"), 0.5), 2).alias("precio_mediana"),
                 F.max("ingested_at").alias("silver_at")))


def variacion_par(item_actual, item_anterior, fecha, fecha_anterior):
    """Variación de precios entre dos fechas, por formato (sólo productos presentes en ambas)."""
    a = item_actual.select(*CLAVE_ITEM, F.col("precio_mediana").alias("p1"))
    b = item_anterior.select(*CLAVE_ITEM, F.col("precio_mediana").alias("p0"))
    j = a.join(b, CLAVE_ITEM, "inner").withColumn("relativo", F.col("p1") / F.col("p0"))
    dentro = F.col("relativo").between(REL_MIN, REL_MAX)
    return (j.groupBy("formato_productos")
            .agg(F.sum(F.when(dentro, 1).otherwise(0)).alias("productos_comparados"),
                 F.sum(F.when(~dentro, 1).otherwise(0)).alias("productos_excluidos"),
                 F.avg(F.when(dentro, F.log("relativo"))).alias("media_log_relativo"))
            .withColumn("fecha", F.lit(fecha))
            .withColumn("fecha_anterior", F.lit(fecha_anterior))
            .withColumn("dias", F.datediff(F.to_date("fecha"), F.to_date("fecha_anterior")))
            .withColumn("variacion", F.round(F.exp("media_log_relativo") - 1, 6)))


def indice_precios(fechas_formato, variaciones):
    """Índice encadenado: base 100 en la primera fecha de cada formato."""
    base = fechas_formato.join(
        variaciones.select("fecha", "formato_productos", "media_log_relativo", "variacion",
                           "productos_comparados", "productos_excluidos", "fecha_anterior"),
        ["fecha", "formato_productos"], "left")
    acumulado = (Window.partitionBy("formato_productos").orderBy("fecha")
                 .rowsBetween(Window.unboundedPreceding, Window.currentRow))
    return (base.withColumn("log_acumulado", F.sum(F.coalesce("media_log_relativo", F.lit(0.0))).over(acumulado))
            .withColumn("indice", F.round(100 * F.exp("log_acumulado"), 4))
            .select("fecha", "formato_productos", "indice", "variacion", "productos_comparados",
                    "productos_excluidos", "fecha_anterior"))


def inflacion_mensual(indice):
    """Variación entre el índice promedio de cada mes y el del mes anterior. Mirar `dias_con_datos`:
    un mes incompleto no es comparable con uno completo."""
    mensual = (indice.withColumn("mes", F.trunc(F.to_date("fecha"), "month"))
               .groupBy("mes", "formato_productos")
               .agg(F.round(F.avg("indice"), 4).alias("indice_promedio"),
                    F.count("*").alias("dias_con_datos"),
                    F.min("fecha").alias("primera_fecha"),
                    F.max("fecha").alias("ultima_fecha"))
               .withColumn("dias_del_mes", F.dayofmonth(F.last_day("mes"))))
    w = Window.partitionBy("formato_productos").orderBy("mes")
    return mensual.withColumn("variacion_mensual",
                              F.round(F.col("indice_promedio") / F.lag("indice_promedio").over(w) - 1, 6))


REZAGOS = 4   # semanas de inflación mayorista anteriores que se incluyen


def serie_semanal(indice):
    """Una fila por semana con ambos índices y la inflación mayorista de las semanas anteriores.
    Variación = log(índice promedio de la semana / el de la anterior)."""
    semanal = (indice.withColumn("semana", F.date_trunc("week", F.to_date("fecha")).cast("date"))
               .groupBy("semana", "formato_productos")
               .agg(F.avg("indice").alias("indice_promedio"), F.count("*").alias("dias_con_datos")))
    w = Window.partitionBy("formato_productos").orderBy("semana")
    semanal = semanal.withColumn("var_semanal",
                                 F.round(F.log(F.col("indice_promedio") / F.lag("indice_promedio").over(w)), 6))

    def canal(nombre):
        return (semanal.filter(F.col("formato_productos") == nombre)
                .select("semana",
                        F.round(F.col("indice_promedio"), 4).alias(f"indice_{nombre}"),
                        F.col("var_semanal").alias(f"var_{nombre}"),
                        F.col("dias_con_datos").alias(f"dias_{nombre}")))

    semanas = (semanal.agg(F.min("semana").alias("a"), F.max("semana").alias("b"))
               .select(F.explode(F.sequence("a", "b", F.expr("interval 7 days"))).alias("semana")))
    unida = semanas.join(canal("mayorista"), "semana", "left").join(canal("minorista"), "semana", "left")
    orden = Window.orderBy("semana")      # tabla chica: una fila por semana
    for k in range(1, REZAGOS + 1):
        unida = unida.withColumn(f"var_mayorista_lag{k}", F.lag("var_mayorista", k).over(orden))
    return unida


def resumen_comercio_dia(prod, quar, com):
    """filas_aceptadas = filas únicas en Silver; filas_rechazadas = líneas en cuarentena.
    Los duplicados descartados no cuentan en ninguna de las dos."""
    aceptadas = (prod.groupBy("fecha", "id_comercio")
                 .agg(F.count("*").alias("filas_aceptadas"),
                      F.countDistinct("id_sucursal").alias("sucursales_con_precios"),
                      F.countDistinct("id_producto").alias("productos")))
    rechazadas = (quar.filter("id_comercio IS NOT NULL").groupBy("fecha", "id_comercio")
                  .agg(F.count("*").alias("filas_rechazadas")))
    nombres = (com.groupBy("id_comercio")
               .agg(F.max("razon_social").alias("razon_social"),
                    F.max("bandera_nombre").alias("bandera_nombre"),
                    F.max("formato_productos").alias("formato_productos")))
    return (aceptadas.join(rechazadas, ["fecha", "id_comercio"], "full_outer")
            .join(nombres, "id_comercio", "left")
            .withColumn("filas_aceptadas", F.coalesce("filas_aceptadas", F.lit(0)))
            .withColumn("filas_rechazadas", F.coalesce("filas_rechazadas", F.lit(0)))
            .withColumn("tasa_rechazo",
                        F.round(F.col("filas_rechazadas") / (F.col("filas_aceptadas") + F.col("filas_rechazadas")), 4)))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Fechas a procesar
# MAGIC
# MAGIC Se procesan las fechas de Silver que Gold todavía no tiene o que Silver actualizó después de la última corrida.

# COMMAND ----------

def escribir_fecha(df, table, fecha):
    """Reemplaza sólo las filas de esa fecha (idempotente); crea la tabla si no existe."""
    if spark.catalog.tableExists(table):
        (df.write.format("delta").mode("overwrite")
           .option("replaceWhere", f"fecha = '{fecha}'").saveAsTable(table))
    else:
        df.write.format("delta").saveAsTable(table)


silver_fechas = spark.table(T_PROD).groupBy("fecha").agg(F.max("ingested_at").alias("silver_at"))
if spark.catalog.tableExists(G_ITEM):
    gold_fechas = spark.table(G_ITEM).groupBy("fecha").agg(F.max("silver_at").alias("gold_at"))
    pendientes = (silver_fechas.join(gold_fechas, "fecha", "left")
                  .filter("gold_at IS NULL OR silver_at > gold_at"))
else:
    pendientes = silver_fechas

fechas = sorted(r["fecha"] for r in pendientes.select("fecha").collect() if r["fecha"])
if FECHA:
    fechas = [FECHA]
print("Fechas a procesar:", fechas or "(ninguna: Gold está al día)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Precio por producto y resumen por comercio (un día por vez)

# COMMAND ----------

com = spark.table(T_COM)
for f in fechas:
    prod_dia = spark.table(T_PROD).filter(F.col("fecha") == f)
    items = precio_item_dia(prod_dia).cache()
    n_items = items.count()
    assert n_items > 0, f"No hay productos en Silver para {f}"
    assert n_items == items.select(*CLAVE_ITEM).distinct().count(), f"{f}: item duplicado en gold_precio_item_dia"
    escribir_fecha(items, G_ITEM, f)
    quar_dia = spark.table(T_QUAR).filter(F.col("fecha") == f)
    resumen = resumen_comercio_dia(prod_dia, quar_dia, com).cache()
    assert resumen.agg(F.sum("filas_aceptadas")).first()[0] == prod_dia.count(), f"{f}: el resumen no reconcilia con Silver"
    escribir_fecha(resumen, G_COMERCIO, f)
    print(f"{f}: {n_items:,} productos")
    items.unpersist()
    resumen.unpersist()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Variación de precios entre fechas consecutivas
# MAGIC
# MAGIC La variación de una fecha depende de la anterior: al incorporar un día se recalcula su variación y la del día siguiente.

# COMMAND ----------

todas = sorted(r["fecha"] for r in spark.table(G_ITEM).select("fecha").distinct().collect())
anterior = {f: todas[i - 1] for i, f in enumerate(todas) if i > 0}
siguiente = {f: todas[i + 1] for i, f in enumerate(todas) if i < len(todas) - 1}

pares = set()
for f in fechas:
    if f in anterior:
        pares.add((anterior[f], f))
    if f in siguiente:
        pares.add((f, siguiente[f]))

items_gold = spark.table(G_ITEM)
for f0, f1 in sorted(pares, key=lambda p: p[1]):
    var = variacion_par(items_gold.filter(F.col("fecha") == f1), items_gold.filter(F.col("fecha") == f0), f1, f0)
    escribir_fecha(var, G_VAR, f1)
    print(f"Variación {f0} → {f1} calculada")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Índices y series (tablas chicas: se reconstruyen completas)

# COMMAND ----------

fechas_formato = spark.table(G_ITEM).select("fecha", "formato_productos").distinct()
variaciones = spark.table(G_VAR) if spark.catalog.tableExists(G_VAR) else spark.createDataFrame(
    [], "fecha string, formato_productos string, media_log_relativo double, variacion double, "
        "productos_comparados long, productos_excluidos long, fecha_anterior string")
indice = indice_precios(fechas_formato, variaciones)
indice.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(G_INDICE)

indice = spark.table(G_INDICE)
inflacion_mensual(indice).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(G_MES)
serie_semanal(indice).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(G_SEMANA)

for t, d in [(G_ITEM, "Precio de cada producto de cada comercio por día (mediana entre sucursales)"),
             (G_VAR, "Variación de precios respecto de la fecha anterior, por formato"),
             (G_INDICE, "Índice de precios encadenado (Jevons), base 100 en la primera fecha, por formato"),
             (G_MES, "Variación mensual del índice de precios, por formato"),
             (G_SEMANA, "Índices mayorista y minorista por semana, con rezagos de la inflación mayorista"),
             (G_COMERCIO, "Cobertura y calidad por comercio y día")]:
    spark.sql(f"COMMENT ON TABLE {t} IS '{d}'")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Controles

# COMMAND ----------

idx = spark.table(G_INDICE)
primera = idx.groupBy("formato_productos").agg(F.min("fecha").alias("fecha")).join(idx, ["formato_productos", "fecha"])
sin_variacion = idx.join(primera.select("formato_productos", "fecha"), ["formato_productos", "fecha"], "left_anti") \
                   .filter("productos_comparados IS NULL").count()
checks = {
    "indice_base_100_en_primera_fecha": primera.filter("indice != 100").count() == 0,
    "indice_sin_nulos": idx.filter("indice IS NULL").count() == 0,
    "todas_las_fechas_tienen_variacion": sin_variacion == 0,
    "indice_unico_por_fecha_y_formato": idx.count() == idx.select("fecha", "formato_productos").distinct().count(),
}
display(spark.createDataFrame([(k, bool(v)) for k, v in checks.items()], ["control", "ok"]))
failed = [k for k, v in checks.items() if not v]
assert not failed, f"Fallaron controles: {failed}"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Consultas para verificar el resultado
# MAGIC
# MAGIC **Índice de precios de cada canal**

# COMMAND ----------

display(spark.table(G_INDICE).orderBy("formato_productos", "fecha")
        .select("fecha", "formato_productos", "indice", "variacion", "productos_comparados", "productos_excluidos"))

# COMMAND ----------

# MAGIC %md
# MAGIC **Inflación mensual por canal** (mirar `dias_con_datos`: un mes incompleto no es comparable)

# COMMAND ----------

display(spark.table(G_MES).orderBy("formato_productos", "mes"))

# COMMAND ----------

# MAGIC %md
# MAGIC **Mayorista vs. minorista por semana** (base para estudiar el traslado de precios cuando haya más semanas)

# COMMAND ----------

display(spark.table(G_SEMANA).orderBy("semana"))

# COMMAND ----------

# MAGIC %md
# MAGIC **Comercios con peor calidad de datos**

# COMMAND ----------

display(spark.table(G_COMERCIO).orderBy(F.desc("tasa_rechazo"))
        .select("fecha", "id_comercio", "razon_social", "formato_productos", "filas_aceptadas", "filas_rechazadas", "tasa_rechazo"))
