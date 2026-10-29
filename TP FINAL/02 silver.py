# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 2 · Silver — SEPA
# MAGIC
# MAGIC Silver tipa, valida y deduplica. Los aceptados se integran con `MERGE`; los rechazados quedan en
# MAGIC cuarentena con causa explícita, conservando la línea original.
# MAGIC
# MAGIC | Tabla | Grano |
# MAGIC |---|---|
# MAGIC | `silver_productos` | Un precio por fecha, comercio, sucursal y producto |
# MAGIC | `silver_productos_quarantine` | Líneas de productos rechazadas, con motivo |
# MAGIC | `silver_sucursales` | Última versión de cada sucursal (provincia normalizada) |
# MAGIC | `silver_comercio` | Última versión de cada comercio **y bandera** |
# MAGIC | `silver_dimensiones_quarantine` | Líneas de sucursales/comercio rechazadas, con motivo |
# MAGIC | `silver_ref_provincias` | Maestro de provincias (Excel) + datos de Georef |
# MAGIC | `silver_ref_departamentos` | Departamentos de Georef (si la API respondió) |
# MAGIC | `pipeline_run_log` | Una fila por corrida: alcance, conteos y marca de agua |
# MAGIC
# MAGIC **Incremental:** sin parámetros, procesa sólo las fechas con líneas de Bronce ingeridas después de la
# MAGIC última corrida exitosa (marca de agua sobre `ingested_at`). De cada fecha afectada se reprocesan *todas*
# MAGIC sus líneas, así que el resultado no depende de cuántas veces se corra.
# MAGIC
# MAGIC | Parámetro | Uso |
# MAGIC |---|---|
# MAGIC | `fecha` | `AAAA-MM-DD`: procesa sólo ese día y **no** mueve la marca de agua |
# MAGIC | `full_refresh` | `si`: borra productos y cuarentena y reconstruye todo desde Bronce (Bronce no se toca) |
# MAGIC | `job_run_id` | Opcional: se guarda en `pipeline_run_log` |

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace", "01 - Catálogo")
dbutils.widgets.text("schema", "sepa_tp", "02 - Esquema")
dbutils.widgets.text("fecha", "", "03 - Fecha (opcional)")
dbutils.widgets.dropdown("full_refresh", "no", ["no", "si"], "04 - Reconstruir todo")
dbutils.widgets.text("job_run_id", "", "05 - Job run id (opcional)")

# COMMAND ----------

# MAGIC %run "./00 config"

# COMMAND ----------

import re

from pyspark.sql import functions as F, Window

cfg = SepaConfig(dbutils.widgets.get("catalog"), dbutils.widgets.get("schema"))
create_namespace(spark, cfg)

FECHA = dbutils.widgets.get("fecha").strip()
FULL = dbutils.widgets.get("full_refresh") == "si"
RUN_ID = dbutils.widgets.get("job_run_id").strip()
assert not FECHA or re.fullmatch(r"\d{4}-\d{2}-\d{2}", FECHA), "fecha debe ser AAAA-MM-DD"
assert not (FULL and FECHA), "full_refresh y fecha son excluyentes"

T_BRONZE = cfg.table("bronze_sepa_lines")
T_MAESTRO = cfg.table("bronze_ref_maestro_provincias")
T_GEOREF = cfg.table("bronze_georef_raw")
T_PROD = cfg.table("silver_productos")
T_QUAR = cfg.table("silver_productos_quarantine")
T_SUC = cfg.table("silver_sucursales")
T_COM = cfg.table("silver_comercio")
T_DIM_QUAR = cfg.table("silver_dimensiones_quarantine")
T_REF_PROV = cfg.table("silver_ref_provincias")
T_REF_DEP = cfg.table("silver_ref_departamentos")
T_LOG = cfg.table("pipeline_run_log")

assert spark.catalog.tableExists(T_BRONZE), f"Falta {T_BRONZE}. Ejecutá primero 01_bronce."
assert spark.catalog.tableExists(T_MAESTRO), f"Falta {T_MAESTRO}. Ejecutá primero 01b_bronce_referencias."

if FULL:
    for t in (T_PROD, T_QUAR):
        spark.sql(f"DROP TABLE IF EXISTS {t}")
    print("full_refresh: productos y cuarentena eliminados; se reconstruyen desde Bronce.")
print("Fecha:", FECHA or "(pendientes por marca de agua)" if not FULL else "(todas)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Funciones de parseo y calidad
# MAGIC
# MAGIC El CSV usa `|` como separador y comillas dobles para valores que contienen `|` o `"` (con `""` como
# MAGIC comilla escapada). El corte por `|` respeta las comillas y las celdas se desenvuelven al leerlas.

# COMMAND ----------

SPLIT_RE = r'\|(?=(?:[^"]*"[^"]*")*[^"]*$)'          # `|` fuera de comillas (cantidad par de " hacia adelante)
_UNQUOTE = "regexp_replace(regexp_replace(try_element_at(p, {n}), '^\"(.*)\"$', '$1'), '\"\"', '\"')"

ACENTOS_DE, ACENTOS_A = "áéíóúüñÁÉÍÓÚÜÑ", "aeiouunAEIOUUN"
ALIAS_PROVINCIA = {                                  # variantes frecuentes → ISO (ya normalizadas con norm())
    "capital federal": "AR-C", "ciudad autonoma de buenos aires": "AR-C", "ciudad de buenos aires": "AR-C",
    "c a b a": "AR-C", "bs as": "AR-B", "bsas": "AR-B",
    "tierra del fuego antartida e islas del atlantico sur": "AR-V",
}


def _clean(i):
    """Texto de la celda i (base 0): sin comillas envolventes, sin espacios; '' pasa a NULL."""
    return f"nullif(trim({_UNQUOTE.format(n=i + 1)}), '')"


def txt(i):
    return F.expr(_clean(i))


def num(i):
    return F.expr(f"try_cast({_clean(i)} as double)")


def dec(i):
    """Dinero: DECIMAL(18,2)."""
    return F.expr(f"try_cast({_clean(i)} as decimal(18,2))")


def entero(i):
    return F.expr(f"try_cast({_clean(i)} as bigint)")


def norm(c):
    """Minúsculas, sin tildes ni puntuación, espacios simples (para comparar nombres)."""
    col = F.col(c) if isinstance(c, str) else c
    s = F.regexp_replace(F.lower(F.translate(col, ACENTOS_DE, ACENTOS_A)), r"[^a-z0-9]+", " ")
    return F.trim(s)


def alias_df():
    return spark.createDataFrame(list(ALIAS_PROVINCIA.items()), "nombre_norm string, iso_id string")


def lineas(bronze, file_name):
    """Líneas de datos de un tipo de CSV: sin encabezado, vacías, pie de página ni errores de lectura."""
    b = bronze.filter((F.col("file_name") == file_name)
                      & (F.col("ingest_status") == "ok")
                      & (F.col("line_no") > 0)
                      & (F.trim("raw_line") != "")
                      & (~F.col("raw_line").rlike("^.ltima actualizaci")))
    return b.withColumn("p", F.split("raw_line", SPLIT_RE, -1)).withColumn("n", F.size("p"))


def invalido(column):
    """True si el valor es NULL o no es positivo (un `~(x > 0)` solo dejaría pasar los NULL)."""
    return ~F.coalesce(F.col(column) > 0, F.lit(False))


# Dígito verificador GS1 sobre el GTIN-14 (pesos 3,1,3,1… de izquierda a derecha en los primeros 13 dígitos)
GTIN_CHECK = ("(10 - (aggregate(sequence(0, 12), 0, (acc, i) -> acc + cast(substring(id_gtin14, i + 1, 1) as int) "
              "* (CASE WHEN i % 2 = 0 THEN 3 ELSE 1 END)) % 10)) % 10 = cast(substring(id_gtin14, 14, 1) as int)")

PRODUCTOS_COLUMNAS = 19


def parse_productos(bronze):
    df = lineas(bronze, "productos.csv").select(
        "fecha", "fecha_export", "source_zip", "inner_zip", "line_no", "ingested_at", "raw_line",
        F.col("n").alias("n_columnas"),
        entero(0).alias("id_comercio"),
        entero(1).alias("id_bandera"),
        entero(2).alias("id_sucursal"),
        txt(3).alias("id_producto"),
        F.coalesce(txt(4) == "1", F.lit(False)).alias("es_ean"),
        txt(5).alias("id_dun_14"),
        txt(6).alias("descripcion"),
        txt(7).alias("marca"),
        dec(8).alias("precio_unit_con_iva"),
        dec(9).alias("precio_unit_sin_iva"),
        num(10).alias("unidad_venta"),
        dec(11).alias("precio_bulto_con_iva"),
        dec(12).alias("precio_bulto_sin_iva"),
        dec(13).alias("precio_promo1_con_iva"),
        dec(14).alias("precio_promo1_sin_iva"),
        txt(15).alias("leyenda_promo1"),
        dec(16).alias("precio_promo2_con_iva"),
        dec(17).alias("precio_promo2_sin_iva"),
        txt(18).alias("leyenda_promo2"),
    )
    # GTIN-14 sólo para códigos EAN/UPC: se quitan ceros a la izquierda y se rellena a 14, así el mismo
    # producto coincide aunque un comercio lo informe con ceros de más. Son *indicadores*, no rechazan.
    df = (df.withColumn("id_gtin14",
                        F.when(F.col("es_ean") & F.col("id_producto").rlike("^[0-9]{8,14}$")
                               & F.col("id_producto").rlike("[1-9]"),
                               F.lpad(F.regexp_replace("id_producto", "^0+", ""), 14, "0")))
            .withColumn("ean_valido",
                        F.when(~F.col("es_ean"), F.lit(None).cast("boolean"))
                         .when(F.col("id_gtin14").isNull(), F.lit(False))
                         .otherwise(F.coalesce(F.expr(GTIN_CHECK), F.lit(False)))))
    reason = (F.when(F.col("n_columnas") != PRODUCTOS_COLUMNAS, "columnas_inesperadas")
              .when(F.col("fecha").isNull(), "sin_fecha")
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
    d = ok.withColumn("rn", F.row_number().over(w)).filter("rn = 1")
    con, sin, uv = (F.col("precio_unit_con_iva").cast("double"), F.col("precio_unit_sin_iva").cast("double"),
                    F.col("unidad_venta"))

    def bulto_ok(bulto, unitario):
        # bulto = unitario × unidad_venta; tolerancia por el redondeo a 2 decimales de cada término
        return F.when(F.col(bulto).isNotNull() & uv.isNotNull(),
                      F.abs(F.col(bulto).cast("double") - unitario * uv) <= 0.01 * uv + 0.01)

    return (d.withColumn("alicuota_iva_implicita", F.round(con / sin - 1, 4))
             .withColumn("bulto_con_iva_consistente", bulto_ok("precio_bulto_con_iva", con))
             .withColumn("bulto_sin_iva_consistente", bulto_ok("precio_bulto_sin_iva", sin))
             .drop("rn", "raw_line", "n_columnas", "quality_reason", "inner_zip"))


def parse_sucursales(bronze, ref):
    b = lineas(bronze, "sucursales.csv").filter(F.col("n") == 21)
    df = b.select(
        "fecha", "fecha_export", "ingested_at",
        entero(0).alias("id_comercio"),
        entero(1).alias("id_bandera"),
        entero(2).alias("id_sucursal"),
        txt(3).alias("nombre"),
        F.lower(txt(4)).alias("tipo"),
        txt(5).alias("calle"),
        txt(6).alias("numero"),
        txt(10).alias("barrio"),
        txt(11).alias("codigo_postal"),
        txt(12).alias("localidad"),
        txt(13).alias("provincia_raw"),
        num(7).alias("latitud_raw"),
        num(8).alias("longitud_raw"),
    ).filter(F.col("id_comercio").isNotNull() & F.col("id_sucursal").isNotNull())

    # Provincia: 1) código ISO que exista en el maestro, 2) nombre (o variante) del maestro, 3) inválida.
    codigos = F.broadcast(ref.select(F.col("iso_id").alias("_iso_codigo")))
    nombres = F.broadcast(ref.select("nombre_norm", "iso_id").unionByName(alias_df())
                          .dropDuplicates(["nombre_norm"])
                          .select(F.col("nombre_norm").alias("_nombre_norm"), F.col("iso_id").alias("_iso_nombre")))
    df = (df.withColumn("_up", F.upper(F.trim("provincia_raw")))
            .withColumn("_norm", norm("provincia_raw"))
            .join(codigos, F.col("_up") == F.col("_iso_codigo"), "left")
            .join(nombres, F.col("_norm") == F.col("_nombre_norm"), "left")
            .withColumn("provincia", F.coalesce("_iso_codigo", "_iso_nombre"))
            .withColumn("provincia_estado",
                        F.when(F.col("provincia_raw").isNull(), "vacia")
                         .when(F.col("_iso_codigo").isNotNull(), "iso_valido")
                         .when(F.col("_iso_nombre").isNotNull(), "por_nombre")
                         .otherwise("invalida"))
            .withColumn("provincia_normalizada", F.col("provincia").isNotNull())
            .drop("_up", "_norm", "_iso_codigo", "_nombre_norm", "_iso_nombre"))

    df = (df.withColumn("geo_valida",
                        F.coalesce(F.col("latitud_raw").between(-56, -21) & F.col("longitud_raw").between(-74, -53),
                                   F.lit(False)))
          .withColumn("latitud", F.when(F.col("geo_valida"), F.col("latitud_raw")))
          .withColumn("longitud", F.when(F.col("geo_valida"), F.col("longitud_raw"))))
    w = (Window.partitionBy("id_comercio", "id_sucursal")
         .orderBy(F.col("fecha").desc(), F.col("fecha_export").desc_nulls_last(), F.col("ingested_at").desc()))
    ultimo = df.withColumn("rn", F.row_number().over(w)).filter("rn = 1").drop("rn", "latitud_raw", "longitud_raw")
    nombres_prov = ref.select(F.col("iso_id").alias("provincia"), F.col("nombre").alias("provincia_nombre"))
    return ultimo.join(F.broadcast(nombres_prov), "provincia", "left")


def parse_comercio(bronze):
    """Una fila por comercio y bandera (comercio.csv trae una fila por bandera)."""
    b = lineas(bronze, "comercio.csv").filter(F.col("n") == 8)
    df = b.select(
        "fecha", "fecha_export", "ingested_at",
        entero(0).alias("id_comercio"),
        entero(1).alias("id_bandera"),
        txt(2).alias("cuit"),
        txt(3).alias("razon_social"),
        txt(4).alias("bandera_nombre"),
        txt(5).alias("bandera_url"),
        F.expr(f"try_cast({_clean(6)} as timestamp)").alias("ultima_actualizacion"),
        entero(7).alias("version_sepa"),
    ).filter(F.col("id_comercio").isNotNull())
    w = (Window.partitionBy("id_comercio", "id_bandera")
         .orderBy(F.col("fecha").desc(), F.col("fecha_export").desc_nulls_last(), F.col("ingested_at").desc()))
    return df.withColumn("rn", F.row_number().over(w)).filter("rn = 1").drop("rn")


def dim_rechazos(bronze):
    """Líneas de sucursales/comercio que no entran a Silver, con su causa."""
    s = lineas(bronze, "sucursales.csv").withColumn(
        "quality_reason",
        F.when(F.col("n") != 21, "columnas_inesperadas")
         .when(entero(0).isNull() | entero(2).isNull(), "sin_id_comercio_o_sucursal"))
    c = lineas(bronze, "comercio.csv").withColumn(
        "quality_reason",
        F.when(F.col("n") != 8, "columnas_inesperadas").when(entero(0).isNull(), "sin_id_comercio"))
    cols = ["file_name", "source_zip", "inner_zip", "fecha", "line_no", "n", "raw_line", "quality_reason", "ingested_at"]
    return (s.filter("quality_reason IS NOT NULL").select(*cols)
            .unionByName(c.filter("quality_reason IS NOT NULL").select(*cols)))


def ensure_table(df, table):
    """Crea la tabla vacía con el esquema de `df` si todavía no existe (para poder hacer MERGE)."""
    if not spark.catalog.tableExists(table):
        df.limit(0).write.format("delta").saveAsTable(table)


def fecha_cond(fechas, col="fecha"):
    validas = [f for f in fechas if f]
    cond = F.col(col).isin(validas) if validas else F.lit(False)
    return (cond | F.col(col).isNull()) if None in fechas else cond


def fecha_sql(alias, fechas):
    """Predicado de fechas para el ON del MERGE (poda de archivos). Las fechas vienen validadas."""
    validas = [f for f in fechas if f]
    partes = []
    if validas:
        partes.append(f"{alias}.fecha IN (" + ", ".join(f"'{f}'" for f in validas) + ")")
    if None in fechas:
        partes.append(f"{alias}.fecha IS NULL")
    return "(" + " OR ".join(partes) + ")" if partes else "(1 = 0)"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Referencias: provincias y departamentos
# MAGIC
# MAGIC El Excel es la fuente del código ISO y el nombre. Georef aporta el id INDEC y el centroide; se une por
# MAGIC `iso_id` si la API lo devuelve, o por nombre normalizado (con variantes como CABA). Sin respuesta de
# MAGIC Georef, las columnas de Georef quedan en NULL y todo lo demás funciona.

# COMMAND ----------

GEOREF_ITEMS = {
    "provincias": ("provincias array<struct<id:string,nombre:string,iso_id:string,iso_nombre:string,"
                   "nombre_completo:string,centroide:struct<lat:double,lon:double>>>"),
    "departamentos": ("departamentos array<struct<id:string,nombre:string,nombre_completo:string,"
                      "provincia:struct<id:string,nombre:string>,centroide:struct<lat:double,lon:double>>>"),
}


def georef_items(resource):
    """Elementos de la última respuesta OK de Georef para `resource` (o None si no hay)."""
    if not spark.catalog.tableExists(T_GEOREF):
        return None
    g = spark.table(T_GEOREF).filter((F.col("resource") == resource) & (F.col("fetch_status") == "ok"))
    ultimo = g.orderBy(F.desc("fetched_at")).select("fetch_id").first()
    if ultimo is None:
        return None
    pages = g.filter(F.col("fetch_id") == ultimo["fetch_id"])
    return (pages.select(F.from_json("response_json", GEOREF_ITEMS[resource]).alias("j"))
            .select(F.explode(f"j.{resource}").alias("x")).select("x.*"))


def build_refs():
    mae_all = spark.table(T_MAESTRO)
    sha = mae_all.orderBy(F.desc("ingested_at")).select("file_sha256").first()["file_sha256"]
    mae = (mae_all.filter(F.col("file_sha256") == sha)
           .select(F.upper(F.trim("sucursales_provincia")).alias("iso_id"), F.trim("provincia").alias("nombre"))
           .filter("iso_id IS NOT NULL AND iso_id <> ''")
           .dropDuplicates(["iso_id"])
           .withColumn("nombre_norm", norm("nombre")))

    geo = georef_items("provincias")
    if geo is None:
        print("Georef sin respuesta: silver_ref_provincias se arma sólo con el Excel.")
        ref = (mae.withColumn("id_indec", F.lit(None).cast("string"))
                  .withColumn("nombre_georef", F.lit(None).cast("string"))
                  .withColumn("centroide_lat", F.lit(None).cast("double"))
                  .withColumn("centroide_lon", F.lit(None).cast("double")))
    else:
        mapa = (mae.select("nombre_norm", "iso_id").unionByName(alias_df())
                .dropDuplicates(["nombre_norm"])
                .select("nombre_norm", F.col("iso_id").alias("iso_por_nombre")))
        geo = (geo.withColumn("nombre_norm", norm("nombre"))
                  .join(mapa, "nombre_norm", "left")
                  .select(F.coalesce(F.upper("iso_id"), F.col("iso_por_nombre")).alias("iso_id"),
                          F.col("id").alias("id_indec"),
                          F.col("nombre").alias("nombre_georef"),
                          F.col("centroide.lat").alias("centroide_lat"),
                          F.col("centroide.lon").alias("centroide_lon"))
                  .filter("iso_id IS NOT NULL").dropDuplicates(["iso_id"]))
        ref = mae.join(geo, "iso_id", "left")
    ref.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(T_REF_PROV)

    dep = georef_items("departamentos")
    if dep is None:
        print("Sin departamentos de Georef: silver_ref_departamentos no se actualiza.")
        return
    prov_iso = spark.table(T_REF_PROV).select(F.col("iso_id").alias("provincia_iso"),
                                              F.col("id_indec").alias("id_provincia_indec"))
    (dep.select(F.col("id").alias("id_departamento"), "nombre",
                F.col("provincia.id").alias("id_provincia_indec"),
                F.col("provincia.nombre").alias("provincia_nombre_georef"),
                F.col("centroide.lat").alias("centroide_lat"),
                F.col("centroide.lon").alias("centroide_lon"))
        .join(prov_iso, "id_provincia_indec", "left")
        .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(T_REF_DEP))


build_refs()
ref_prov = spark.table(T_REF_PROV)
print(f"silver_ref_provincias: {ref_prov.count()} filas · con id INDEC: {ref_prov.filter('id_indec IS NOT NULL').count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Qué fechas procesar

# COMMAND ----------

bronze = spark.table(T_BRONZE)
prod_lines = bronze.filter(F.col("file_name") == "productos.csv")


def ultima_marca():
    if FULL or not spark.catalog.tableExists(T_LOG):
        return None
    return (spark.table(T_LOG).filter((F.col("tabla") == "silver_productos") & F.col("ok"))
            .agg(F.max("watermark_ingested_at")).first()[0])


marca_vieja = ultima_marca()
marca_nueva = prod_lines.agg(F.max("ingested_at")).first()[0]      # se fija ANTES de procesar

if FECHA:
    fechas = [FECHA]
elif marca_vieja is None:
    fechas = [r["fecha"] for r in prod_lines.select("fecha").distinct().collect()]
else:
    fechas = [r["fecha"] for r in prod_lines.filter(F.col("ingested_at") > F.lit(marca_vieja))
              .select("fecha").distinct().collect()]
fechas = sorted(fechas, key=lambda f: (f is None, f))

# Si cambió el esquema de Silver (p. ej. dinero a DECIMAL), hay que reconstruir desde Bronce una vez.
for t in (T_PROD, T_QUAR):
    if spark.catalog.tableExists(t):
        tipos = dict(spark.table(t).dtypes)
        if not tipos.get("precio_unit_con_iva", "").startswith("decimal") or "id_gtin14" not in tipos:
            raise Exception(f"{t} tiene el esquema anterior. Ejecutá una vez con full_refresh = si "
                            "(se reconstruye desde Bronce sin perder datos).")

print("Marca de agua anterior:", marca_vieja)
print("Fechas a procesar:", fechas or "(ninguna: no hay líneas nuevas en Bronce)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Productos: cuarentena y `MERGE`
# MAGIC
# MAGIC - **Cuarentena:** una fila por línea de origen. Si al reprocesar la línea ya es válida, sale de cuarentena;
# MAGIC   si cambia la causa, se actualiza.
# MAGIC - **Silver:** una versión sólo reemplaza a la existente si su export es más nuevo (o, a igual export, su ingesta).
# MAGIC   Si el export más nuevo es inválido, se conserva la última versión válida.
# MAGIC - **Indicadores (no rechazan):** `ean_valido`, `id_gtin14`, `bulto_*_consistente`, `alicuota_iva_implicita`.

# COMMAND ----------

CLAVES = ["fecha", "id_comercio", "id_sucursal", "id_producto"]


def procesar_productos(fechas):
    parsed = parse_productos(bronze.filter(fecha_cond(fechas))).cache()
    try:
        valid = latest_productos(parsed.filter("quality_reason IS NULL"))

        ensure_table(parsed, T_QUAR)
        parsed.createOrReplaceTempView("_productos_lineas")
        spark.sql(f"""
        MERGE INTO {T_QUAR} t USING _productos_lineas s
        ON t.source_zip = s.source_zip AND t.inner_zip = s.inner_zip AND t.line_no = s.line_no
           AND {fecha_sql('t', fechas)}
        WHEN MATCHED AND s.quality_reason IS NULL THEN DELETE
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED AND s.quality_reason IS NOT NULL THEN INSERT *""")

        ensure_table(valid, T_PROD)
        valid.createOrReplaceTempView("_productos_validos")
        spark.sql(f"""
        MERGE INTO {T_PROD} t USING _productos_validos s
        ON t.fecha = s.fecha AND t.id_comercio = s.id_comercio AND t.id_sucursal = s.id_sucursal
           AND t.id_producto = s.id_producto AND {fecha_sql('t', [f for f in fechas if f])}
        WHEN MATCHED AND (s.fecha_export > t.fecha_export
                          OR (t.fecha_export IS NULL AND s.fecha_export IS NOT NULL)
                          OR (s.fecha_export <=> t.fecha_export AND s.ingested_at > t.ingested_at)) THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *""")

        # Controles: reconciliación dentro del alcance procesado
        total = parsed.count()
        n_rech = parsed.filter("quality_reason IS NOT NULL").count()
        n_val = total - n_rech
        n_claves_val = parsed.filter("quality_reason IS NULL").select(*CLAVES).distinct().count()
        silver_scope = spark.table(T_PROD).filter(fecha_cond(fechas))
        n_silver = silver_scope.count()
        n_claves = silver_scope.select(*CLAVES).distinct().count()
        n_quar = spark.table(T_QUAR).filter(fecha_cond(fechas)).count()

        display(spark.createDataFrame(
            [("líneas de productos en Bronce (alcance)", total),
             ("rechazadas", n_rech),
             ("válidas", n_val),
             ("claves distintas entre las válidas", n_claves_val),
             ("filas en silver_productos (alcance)", n_silver),
             ("filas en cuarentena (alcance)", n_quar)],
            ["concepto", "filas"]))
        assert n_silver == n_claves, "silver_productos tiene claves duplicadas"
        assert n_silver == n_claves_val, "silver_productos no coincide con las claves válidas de Bronce"
        assert n_quar == n_rech, "la cuarentena no coincide con las líneas rechazadas de Bronce"
        return {"lineas": total, "rechazadas": n_rech, "validas": n_val, "filas_silver": n_silver}
    finally:
        parsed.unpersist()


if fechas:
    metricas = procesar_productos(fechas)
    print("Controles de productos OK")
else:
    metricas = {"lineas": 0, "rechazadas": 0, "validas": 0, "filas_silver": 0}
    print("Nada pendiente en productos.")

display(spark.table(T_QUAR).groupBy("fecha", "quality_reason").count().orderBy("fecha", "quality_reason"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Sucursales y comercios
# MAGIC
# MAGIC Dimensiones chicas: se reconstruyen completas desde Bronce en cada corrida (siempre con todos los días).
# MAGIC Lo que no entra queda en `silver_dimensiones_quarantine`.

# COMMAND ----------

ref_prov = spark.table(T_REF_PROV)

(parse_sucursales(bronze, ref_prov).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(T_SUC))
(parse_comercio(bronze).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(T_COM))
(dim_rechazos(bronze).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(T_DIM_QUAR))
print(f"silver_sucursales: {spark.table(T_SUC).count():,} · silver_comercio: {spark.table(T_COM).count():,} · "
      f"dimensiones en cuarentena: {spark.table(T_DIM_QUAR).count():,}")

display(spark.table(T_SUC).groupBy("provincia_estado").count().orderBy(F.desc("count")))
print("Valores de provincia que no se pudieron reconocer (si hay, agregalos a ALIAS_PROVINCIA):")
display(spark.table(T_SUC).filter("provincia_estado IN ('invalida', 'vacia')")
        .groupBy("provincia_raw", "provincia_estado").count().orderBy(F.desc("count")))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Registro de la corrida
# MAGIC
# MAGIC La marca de agua sólo avanza en corridas automáticas. Con `fecha` se conserva la anterior para no
# MAGIC saltear fechas pendientes.

# COMMAND ----------

marca_a_guardar = marca_vieja if FECHA else marca_nueva
alcance = ",".join(f or "(sin fecha)" for f in fechas) if fechas else "(nada pendiente)"
(spark.createDataFrame(
    [("silver", "silver_productos", RUN_ID, alcance, marca_a_guardar, metricas["lineas"],
      metricas["rechazadas"], metricas["validas"], metricas["filas_silver"], True)],
    "capa string, tabla string, run_id string, alcance string, watermark_ingested_at timestamp, "
    "lineas_bronce long, rechazadas long, validas long, filas_silver long, ok boolean")
 .withColumn("run_ts", F.current_timestamp())
 .write.mode("append").saveAsTable(T_LOG))
display(spark.table(T_LOG).orderBy(F.desc("run_ts")).limit(10))
