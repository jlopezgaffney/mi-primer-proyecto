import dlt
from pyspark.sql import functions as F, Window

def txt(i):
    return F.expr(f"nullif(trim(try_element_at(p, {i + 1})), '')")

def num(i):
    return F.expr(f"try_cast(nullif(trim(try_element_at(p, {i + 1})), '') as double)")

def entero(i):
    return F.expr(f"try_cast(nullif(trim(try_element_at(p, {i + 1})), '') as bigint)")

def lineas(file_name):
    b = dlt.read("bronze_sepa_lines").filter(F.col("file_name") == file_name)
    b = b.filter((F.col("line_no") > 0)
                 & (F.trim("raw_line") != "")
                 & (~F.col("raw_line").rlike("^.ltima actualizaci")))
    return b.withColumn("p", F.split("raw_line", r"\|", -1)).withColumn("n", F.size("p"))

# ---------- PRODUCTOS ----------
@dlt.view(name="productos_parsed")
@dlt.expect_all({
    "n_columnas_ok": "n_columnas = 19",
    "precio_con_iva_positivo": "precio_unit_con_iva > 0",
    "precio_sin_iva_positivo": "precio_unit_sin_iva > 0",
    "unidad_venta_positiva": "unidad_venta > 0",
    "sin_iva_menor_o_igual_con_iva": "precio_unit_sin_iva <= precio_unit_con_iva",
})
def productos_parsed():
    b = lineas("productos.csv")
    df = b.select(
        "fecha", "source_zip", "line_no", "ingested_at", "raw_line",
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
    reason = (F.when(F.col("n_columnas") != 19, "columnas_inesperadas")
              .when(F.col("id_producto").isNull(), "sin_id_producto")
              .when(~(F.col("precio_unit_con_iva") > 0), "precio_con_iva_invalido")
              .when(~(F.col("precio_unit_sin_iva") > 0), "precio_sin_iva_invalido")
              .when(~(F.col("unidad_venta") > 0), "unidad_venta_invalida")
              .when(F.col("precio_unit_sin_iva") > F.col("precio_unit_con_iva"), "sin_iva_mayor_que_con_iva"))
    return df.withColumn("quality_reason", reason)

@dlt.table(name="silver_productos_quarantine",
           comment="Filas de productos rechazadas, con motivo")
def silver_productos_quarantine():
    return dlt.read("productos_parsed").filter(F.col("quality_reason").isNotNull())

@dlt.table(name="silver_productos",
           comment="Un precio por fecha, comercio, sucursal y producto")
def silver_productos():
    ok = dlt.read("productos_parsed").filter(F.col("quality_reason").isNull())
    w = (Window.partitionBy("fecha", "id_comercio", "id_sucursal", "id_producto")
         .orderBy(F.col("ingested_at").desc(), F.col("line_no").desc()))
    return (ok.withColumn("rn", F.row_number().over(w)).filter("rn = 1")
              .withColumn("alicuota_iva_implicita",
                          F.round(F.col("precio_unit_con_iva") / F.col("precio_unit_sin_iva") - 1, 2))
              .drop("rn", "raw_line", "n_columnas", "quality_reason"))

# ---------- SUCURSALES ----------
@dlt.table(name="silver_sucursales", comment="Última versión de cada sucursal")
def silver_sucursales():
    b = lineas("sucursales.csv").filter(F.col("n") == 21)
    df = b.select(
        "fecha", "ingested_at",
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
            F.col("latitud_raw").between(-56, -21) & F.col("longitud_raw").between(-74, -53))
          .withColumn("latitud", F.when(F.col("geo_valida"), F.col("latitud_raw")))
          .withColumn("longitud", F.when(F.col("geo_valida"), F.col("longitud_raw"))))
    w = (Window.partitionBy("id_comercio", "id_sucursal")
         .orderBy(F.col("fecha").desc(), F.col("ingested_at").desc()))
    return (df.withColumn("rn", F.row_number().over(w)).filter("rn = 1")
              .drop("rn", "latitud_raw", "longitud_raw"))

# ---------- COMERCIOS ----------
@dlt.table(name="silver_comercio", comment="Última versión de cada comercio")
def silver_comercio():
    b = lineas("comercio.csv").filter(F.col("n") == 8)
    df = b.select(
        "fecha", "ingested_at",
        entero(0).alias("id_comercio"),
        entero(1).alias("id_bandera"),
        txt(2).alias("cuit"),
        txt(3).alias("razon_social"),
        txt(4).alias("bandera_nombre"),
        F.expr("try_cast(nullif(trim(try_element_at(p, 7)), '') as timestamp)").alias("ultima_actualizacion"),
    ).filter(F.col("id_comercio").isNotNull())
    w = Window.partitionBy("id_comercio").orderBy(F.col("fecha").desc(), F.col("ingested_at").desc())
    return df.withColumn("rn", F.row_number().over(w)).filter("rn = 1").drop("rn")
