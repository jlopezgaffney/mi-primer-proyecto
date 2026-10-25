# Databricks notebook source
# MAGIC %md
# MAGIC # 2 · Silver — SEPA
# MAGIC
# MAGIC Silver tipa, valida y deduplica. Los registros aceptados se integran con `MERGE`; los rechazados
# MAGIC quedan en cuarentena con una causa explícita, conservando la línea original.
# MAGIC
# MAGIC | Tabla | Grano |
# MAGIC |---|---|
# MAGIC | `silver_productos` | Un precio por fecha, comercio, sucursal y producto |
# MAGIC | `silver_productos_quarantine` | Líneas de productos rechazadas, con motivo |
# MAGIC | `silver_sucursales` | Última versión de cada sucursal |
# MAGIC | `silver_comercio` | Última versión de cada comercio |
# MAGIC
# MAGIC El parámetro `fecha` (opcional, `AAAA-MM-DD`) limita el procesamiento de productos a un día;
# MAGIC vacío procesa todo Bronce. Es seguro reejecutar: el `MERGE` no duplica.

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace", "01 - Catálogo")
dbutils.widgets.text("schema", "sepa_tp", "02 - Esquema")
dbutils.widgets.text("fecha", "", "03 - Fecha (opcional)")

# COMMAND ----------

# MAGIC %run "./00 config"

# COMMAND ----------

from pyspark.sql import functions as F, Window

cfg = SepaConfig(dbutils.widgets.get("catalog"), dbutils.widgets.get("schema"))
create_namespace(spark, cfg)
FECHA = dbutils.widgets.get("fecha").strip()

T_BRONZE = cfg.table("bronze_sepa_lines")
T_PROD = cfg.table("silver_productos")
T_QUAR = cfg.table("silver_productos_quarantine")
T_SUC = cfg.table("silver_sucursales")
T_COM = cfg.table("silver_comercio")

assert spark.catalog.tableExists(T_BRONZE), f"Falta {T_BRONZE}. Ejecutá primero 01_bronce."
print("Fecha:", FECHA or "(todas)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Funciones de parseo y calidad

# COMMAND ----------

def txt(i):
    return F.expr(f"nullif(trim(try_element_at(p, {i + 1})), '')")


def num(i):
    return F.expr(f"try_cast(nullif(trim(try_element_at(p, {i + 1})), '') as double)")


def entero(i):
    return F.expr(f"try_cast(nullif(trim(try_element_at(p, {i + 1})), '') as bigint)")


def lineas(bronze, file_name):
    """Líneas de datos de un tipo de CSV: sin encabezado, vacías, pie de página ni errores de lectura."""
    b = bronze.filter((F.col("file_name") == file_name)
                      & (F.col("ingest_status") == "ok")
                      & (F.col("line_no") > 0)
                      & (F.trim("raw_line") != "")
                      & (~F.col("raw_line").rlike("^.ltima actualizaci")))
    return b.withColumn("p", F.split("raw_line", r"\|", -1)).withColumn("n", F.size("p"))


def invalido(column):
    """True si el valor es NULL o no es positivo (un `~(x > 0)` solo dejaría pasar los NULL)."""
    return ~F.coalesce(F.col(column) > 0, F.lit(False))


PRODUCTOS_COLUMNAS = 19


def parse_productos(bronze):
    df = lineas(bronze, "productos.csv").select(
        "fecha", "fecha_export", "source_zip", "inner_zip", "line_no", "ingested_at", "raw_line",
        F.col("n").alias("n_columnas"),
        entero(0).alias("id_comercio"),
        entero(1).alias("id_bandera"),
        entero(2).alias("id_sucursal"),
        txt(3).alias("id_producto"),
        (txt(4) == "1").alias("es_ean"),
        txt(5).alias("id_dun_14"),
        txt(6).alias("descripcion"),
        txt(7).alias("marca"),
        num(8).alias("precio_unit_con_iva"),
        num(9).alias("precio_unit_sin_iva"),
        num(10).alias("unidad_venta"),
        num(11).alias("precio_bulto_con_iva"),
        num(12).alias("precio_bulto_sin_iva"),
    )
    reason = (F.when(F.col("n_columnas") != PRODUCTOS_COLUMNAS, "columnas_inesperadas")
              .when(F.col("id_comercio").isNull() | F.col("id_sucursal").isNull(), "sin_id_comercio_o_sucursal")
              .when(F.col("id_producto").isNull(), "sin_id_producto")
              .when(invalido("precio_unit_con_iva"), "precio_con_iva_invalido")
              .when(invalido("precio_unit_sin_iva"), "precio_sin_iva_invalido")
              .when(invalido("unidad_venta"), "unidad_venta_invalida")
              .when(F.col("precio_unit_sin_iva") > F.col("precio_unit_con_iva"), "sin_iva_mayor_que_con_iva"))
    return df.withColumn("quality_reason", reason)


def latest_productos(ok):
    """Una fila por fecha/comercio/sucursal/producto: gana el export más nuevo, luego la ingesta más reciente."""
    w = (Window.partitionBy("fecha", "id_comercio", "id_sucursal", "id_producto")
         .orderBy(F.col("fecha_export").desc_nulls_last(), F.col("ingested_at").desc(), F.col("line_no").desc()))
    return (ok.withColumn("rn", F.row_number().over(w)).filter("rn = 1")
              .withColumn("alicuota_iva_implicita",
                          F.round(F.col("precio_unit_con_iva") / F.col("precio_unit_sin_iva") - 1, 2))
              .drop("rn", "raw_line", "n_columnas", "quality_reason", "inner_zip"))


def parse_sucursales(bronze):
    b = lineas(bronze, "sucursales.csv").filter(F.col("n") == 21)
    df = b.select(
        "fecha", "fecha_export", "ingested_at",
        entero(0).alias("id_comercio"),
        entero(1).alias("id_bandera"),
        entero(2).alias("id_sucursal"),
        txt(3).alias("nombre"),
        F.lower(txt(4)).alias("tipo"),
        txt(5).alias("calle"),
        txt(10).alias("barrio"),
        txt(11).alias("codigo_postal"),
        txt(12).alias("localidad"),
        txt(13).alias("provincia_raw"),
        num(7).alias("latitud_raw"),
        num(8).alias("longitud_raw"),
    ).filter(F.col("id_comercio").isNotNull() & F.col("id_sucursal").isNotNull())
    df = (df.withColumn("provincia",
                        F.when(F.col("provincia_raw").rlike("^AR-[A-Za-z]$"), F.upper("provincia_raw"))
                         .when(F.lower("provincia_raw") == "jujuy", F.lit("AR-Y")))
          .withColumn("provincia_normalizada", F.col("provincia").isNotNull())
          .withColumn("geo_valida",
                      F.coalesce(F.col("latitud_raw").between(-56, -21) & F.col("longitud_raw").between(-74, -53),
                                 F.lit(False)))
          .withColumn("latitud", F.when(F.col("geo_valida"), F.col("latitud_raw")))
          .withColumn("longitud", F.when(F.col("geo_valida"), F.col("longitud_raw"))))
    w = (Window.partitionBy("id_comercio", "id_sucursal")
         .orderBy(F.col("fecha").desc(), F.col("fecha_export").desc_nulls_last(), F.col("ingested_at").desc()))
    return (df.withColumn("rn", F.row_number().over(w)).filter("rn = 1")
              .drop("rn", "latitud_raw", "longitud_raw"))


def parse_comercio(bronze):
    b = lineas(bronze, "comercio.csv").filter(F.col("n") == 8)
    df = b.select(
        "fecha", "fecha_export", "ingested_at",
        entero(0).alias("id_comercio"),
        entero(1).alias("id_bandera"),
        txt(2).alias("cuit"),
        txt(3).alias("razon_social"),
        txt(4).alias("bandera_nombre"),
        F.expr("try_cast(nullif(trim(try_element_at(p, 7)), '') as timestamp)").alias("ultima_actualizacion"),
    ).filter(F.col("id_comercio").isNotNull())
    w = (Window.partitionBy("id_comercio")
         .orderBy(F.col("fecha").desc(), F.col("fecha_export").desc_nulls_last(), F.col("ingested_at").desc()))
    return df.withColumn("rn", F.row_number().over(w)).filter("rn = 1").drop("rn")


def ensure_table(df, table):
    """Crea la tabla vacía con el esquema de `df` si todavía no existe (para poder hacer MERGE)."""
    if not spark.catalog.tableExists(table):
        df.limit(0).write.format("delta").saveAsTable(table)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Productos: cuarentena y `MERGE`

# COMMAND ----------

bronze = spark.table(T_BRONZE)
bronze_prod = bronze.filter(F.col("fecha") == FECHA) if FECHA else bronze
parsed = parse_productos(bronze_prod).cache()

rejected = parsed.filter("quality_reason IS NOT NULL")
valid = latest_productos(parsed.filter("quality_reason IS NULL"))

# Cuarentena: una fila por línea rechazada (no se duplica al reejecutar)
ensure_table(rejected, T_QUAR)
rejected.createOrReplaceTempView("_productos_rechazados")
spark.sql(f"""
MERGE INTO {T_QUAR} t USING _productos_rechazados s
ON t.source_zip = s.source_zip AND t.inner_zip = s.inner_zip AND t.line_no = s.line_no
WHEN NOT MATCHED THEN INSERT *""")

# Silver: se actualiza sólo si llega una versión más nueva
ensure_table(valid, T_PROD)
valid.createOrReplaceTempView("_productos_validos")
spark.sql(f"""
MERGE INTO {T_PROD} t USING _productos_validos s
ON t.fecha <=> s.fecha AND t.id_comercio = s.id_comercio AND t.id_sucursal = s.id_sucursal
   AND t.id_producto = s.id_producto
WHEN MATCHED AND (s.fecha_export > t.fecha_export
                  OR (s.fecha_export <=> t.fecha_export AND s.ingested_at > t.ingested_at)) THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *""")

display(spark.table(T_QUAR).groupBy("fecha", "quality_reason").count().orderBy("fecha", "quality_reason"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Sucursales y comercios
# MAGIC
# MAGIC Son dimensiones chicas: se reconstruyen completas desde Bronce (siempre con todos los días).

# COMMAND ----------

(parse_sucursales(bronze).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(T_SUC))
(parse_comercio(bronze).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(T_COM))
print(f"silver_sucursales: {spark.table(T_SUC).count():,} · silver_comercio: {spark.table(T_COM).count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Controles
# MAGIC
# MAGIC Reconciliación: toda línea de datos de productos termina aceptada, duplicada (se quedó con otra versión)
# MAGIC o en cuarentena. Nada se pierde.

# COMMAND ----------

total = parsed.count()
n_rechazadas = parsed.filter("quality_reason IS NOT NULL").count()
n_validas = total - n_rechazadas
claves = ["fecha", "id_comercio", "id_sucursal", "id_producto"]
silver = spark.table(T_PROD)
silver_scope = silver.filter(F.col("fecha") == FECHA) if FECHA else silver
n_silver = silver_scope.count()
n_claves = silver_scope.select(*claves).distinct().count()
n_claves_validas = parsed.filter("quality_reason IS NULL").select(*claves).distinct().count()

display(spark.createDataFrame(
    [("líneas de productos en Bronce", total),
     ("rechazadas (cuarentena)", n_rechazadas),
     ("válidas", n_validas),
     ("claves distintas entre las válidas", n_claves_validas),
     ("filas en silver_productos", n_silver)],
    ["concepto", "filas"]))

assert n_silver == n_claves, "silver_productos tiene claves duplicadas"
assert n_silver == n_claves_validas, "silver_productos no coincide con las claves válidas de Bronce"
assert total == n_rechazadas + n_validas
print("Controles OK")
parsed.unpersist()
