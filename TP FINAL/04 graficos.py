# Databricks notebook source
# MAGIC %md
# MAGIC # 4 · Gráficos — SEPA
# MAGIC
# MAGIC Este notebook sólo lee Gold: no escribe ni modifica ninguna tabla. Responde dos preguntas.
# MAGIC
# MAGIC **1. ¿Cómo cambió el precio de un mismo producto entre una fecha pasada y la actual?** (mayorista y minorista)
# MAGIC
# MAGIC No se pueden graficar todos los EAN: hay miles y la mayoría los informan muy pocos comercios. Se arma una
# MAGIC **canasta** con los `top_n` EAN que informan más comercios en las dos fechas, porque son los más representativos
# MAGIC y los menos sensibles a un error de carga. Para cada EAN, el cambio se mide como en Gold: **mismo producto en
# MAGIC el mismo comercio**, con la mediana entre comercios, y se excluyen los cambios fuera del rango 0,5 a 2.
# MAGIC
# MAGIC **2. ¿El precio mayorista de un relevamiento anterior se refleja en el precio minorista del relevamiento siguiente?**
# MAGIC
# MAGIC Para cada EAN que está en los dos canales se compara su variación mayorista de un período con su variación
# MAGIC minorista del período siguiente. Hacen falta al menos 3 fechas para ver el efecto rezagado.
# MAGIC
# MAGIC | Parámetro | Uso |
# MAGIC |---|---|
# MAGIC | `fecha_base` / `fecha_actual` | Fechas a comparar en el gráfico 1 (vacío: la primera y la última de Gold) |
# MAGIC | `min_comercios` | Mínimo de comercios que informan un EAN en las dos fechas para entrar en la canasta |
# MAGIC | `top_n` | Cantidad de EAN de la canasta, por canal |
# MAGIC | `min_comercios_par` | Mínimo de comercios por EAN y canal para el gráfico 2 |

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace", "01 - Catálogo")
dbutils.widgets.text("schema", "sepa_tp", "02 - Esquema")
dbutils.widgets.text("fecha_base", "", "03 - Fecha pasada (vacío = la primera)")
dbutils.widgets.text("fecha_actual", "", "04 - Fecha actual (vacío = la última)")
dbutils.widgets.text("min_comercios", "10", "05 - Mín. comercios por EAN (gráfico 1)")
dbutils.widgets.text("top_n", "30", "06 - Cantidad de EAN (gráfico 1)")
dbutils.widgets.text("min_comercios_par", "3", "07 - Mín. comercios por EAN y canal (gráfico 2)")

# COMMAND ----------

# MAGIC %run "./00 config"

# COMMAND ----------

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from pyspark.sql import functions as F, Window

cfg = SepaConfig(dbutils.widgets.get("catalog"), dbutils.widgets.get("schema"))
G_ITEM = cfg.table("gold_precio_item_dia")
G_INDICE = cfg.table("gold_indice_precios_dia")
G_SEMANA = cfg.table("gold_mayorista_vs_minorista_semana")
for t in [G_ITEM, G_INDICE]:
    assert spark.catalog.tableExists(t), f"Falta {t}. Ejecutá primero 03 gold."

MIN_COM = int(dbutils.widgets.get("min_comercios"))
TOP_N = int(dbutils.widgets.get("top_n"))
MIN_COM_PAR = int(dbutils.widgets.get("min_comercios_par"))
REL_MIN, REL_MAX = 0.5, 2.0          # mismo criterio que Gold: fuera de este rango se excluye
FORMATOS = ["mayorista", "minorista"]
COLOR = {"mayorista": "#1f77b4", "minorista": "#d95f02"}

# Un precio por fecha, formato, comercio y EAN (sólo productos con EAN válido de formato único: id_gtin14).
precios = (spark.table(G_ITEM)
           .filter(F.col("id_gtin14").isNotNull() & (F.col("precio_mediana") > 0))
           .withColumn("fecha_s", F.col("fecha").cast("string"))
           .groupBy("fecha_s", "formato_productos", "id_comercio", "id_gtin14")
           .agg(F.percentile_approx("precio_mediana", 0.5).alias("precio"),
                F.max("descripcion").alias("descripcion")))

fechas = sorted(r["fecha_s"] for r in precios.select("fecha_s").distinct().collect())
print("Fechas disponibles en Gold:", fechas)
assert len(fechas) >= 2, "Hacen falta al menos 2 fechas en Gold para comparar."

F0 = dbutils.widgets.get("fecha_base").strip() or fechas[0]
F1 = dbutils.widgets.get("fecha_actual").strip() or fechas[-1]
assert F0 in fechas and F1 in fechas, f"Las fechas tienen que existir en Gold: {fechas}"
assert F0 < F1, "fecha_base tiene que ser anterior a fecha_actual"
print(f"Comparación del gráfico 1: {F0} → {F1}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Precio de un mismo EAN: fecha pasada vs. fecha actual
# MAGIC
# MAGIC Primero se cuenta cuántos EAN se pueden comparar y cuántos entran en la canasta.

# COMMAND ----------

a = (precios.filter(F.col("fecha_s") == F0)
     .select("formato_productos", "id_comercio", "id_gtin14", F.col("precio").alias("p0"), "descripcion"))
b = (precios.filter(F.col("fecha_s") == F1)
     .select("formato_productos", "id_comercio", "id_gtin14", F.col("precio").alias("p1")))

pares = (a.join(b, ["formato_productos", "id_comercio", "id_gtin14"])
         .withColumn("relativo", F.col("p1") / F.col("p0"))
         .filter(F.col("relativo").between(REL_MIN, REL_MAX)))

por_ean = (pares.groupBy("formato_productos", "id_gtin14")
           .agg(F.countDistinct("id_comercio").alias("n_comercios"),
                F.percentile_approx("p0", 0.5).alias("precio_antes"),
                F.percentile_approx("p1", 0.5).alias("precio_ahora"),
                F.percentile_approx("relativo", 0.5).alias("relativo"),
                F.max("descripcion").alias("descripcion")))

display(por_ean.groupBy("formato_productos")
        .agg(F.count("*").alias("ean_comparables"),
             F.sum(F.when(F.col("n_comercios") >= MIN_COM, 1).otherwise(0)).alias(f"ean_con_{MIN_COM}_o_mas_comercios")))

orden = Window.partitionBy("formato_productos").orderBy(F.desc("n_comercios"), "id_gtin14")
canasta = (por_ean.filter(F.col("n_comercios") >= MIN_COM)
           .withColumn("puesto", F.row_number().over(orden))
           .filter(F.col("puesto") <= TOP_N))
canasta_pd = canasta.toPandas()
print(f"Canasta: {len(canasta_pd)} EAN en total (hasta {TOP_N} por canal).")
if canasta_pd.empty:
    print("La canasta quedó vacía: bajá `min_comercios` o elegí otras fechas.")

# COMMAND ----------

def etiqueta(fila):
    d = (fila["descripcion"] or "").strip()
    d = d if d else fila["id_gtin14"]
    return d[:30] + "…" if len(d) > 30 else d


def grafico_canasta(df, formato):
    d = df[df["formato_productos"] == formato].copy()
    if d.empty:
        print(f"{formato}: no hay EAN en la canasta.")
        return
    d["cambio_pct"] = (d["relativo"] - 1) * 100
    d = d.sort_values("cambio_pct").reset_index(drop=True)

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, max(5, 0.3 * len(d) + 1.5)),
                                   gridspec_kw={"width_ratios": [1, 1.2]})
    # Izquierda: cómo se reparten los cambios de precio de la canasta.
    ax1.hist(d["cambio_pct"], bins=min(15, max(5, len(d) // 2)), color=COLOR[formato], alpha=0.85, edgecolor="white")
    ax1.axvline(0, color="black", lw=0.8)
    ax1.axvline(d["cambio_pct"].median(), color="gray", ls="--", label=f"mediana {d['cambio_pct'].median():.2f} %")
    ax1.set_xlabel("Cambio de precio (%)")
    ax1.set_ylabel("Cantidad de EAN")
    ax1.set_title(f"Distribución del cambio entre {F0} y {F1}")
    ax1.legend()

    # Derecha: cambio porcentual por EAN.
    colores = np.where(d["cambio_pct"] >= 0, "#c0392b", "#27ae60")
    ax2.barh(range(len(d)), d["cambio_pct"], color=colores)
    ax2.set_yticks(range(len(d)))
    ax2.set_yticklabels([etiqueta(r) for _, r in d.iterrows()], fontsize=8)
    ax2.axvline(0, color="black", lw=0.8)
    ax2.set_xlabel("Cambio de precio (%)")
    ax2.set_title("Cambio por EAN (rojo: subió, verde: bajó)")

    fig.suptitle(f"{formato.capitalize()}: {len(d)} EAN más relevados, {F0} vs. {F1}", fontsize=13)
    plt.tight_layout()
    plt.show()
    print(f"{formato}: mediana del cambio {d['cambio_pct'].median():.2f} %, "
          f"subió {(d['cambio_pct'] > 0).mean() * 100:.0f} % de los EAN, "
          f"sin cambio {(d['cambio_pct'] == 0).mean() * 100:.0f} %, "
          f"bajó {(d['cambio_pct'] < 0).mean() * 100:.0f} %.")


for formato in FORMATOS:
    grafico_canasta(canasta_pd, formato)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Evolución de la canasta en todas las fechas
# MAGIC
# MAGIC Si hay más de dos fechas, se sigue la misma canasta a lo largo del tiempo (índice 100 en la fecha base) y se la
# MAGIC compara con el índice general de Gold, recalculado con base 100 en esa misma fecha.

# COMMAND ----------

if len(fechas) > 2 and not canasta_pd.empty:
    sel = spark.createDataFrame(canasta_pd[["formato_productos", "id_gtin14"]])
    base0 = (precios.filter(F.col("fecha_s") == F0)
             .select("formato_productos", "id_comercio", "id_gtin14", F.col("precio").alias("p0")))
    evol = (precios.filter(F.col("fecha_s") >= F0)
            .join(sel, ["formato_productos", "id_gtin14"])
            .join(base0, ["formato_productos", "id_comercio", "id_gtin14"])
            .withColumn("relativo", F.col("precio") / F.col("p0"))
            .filter(F.col("relativo").between(REL_MIN, REL_MAX))
            .groupBy("fecha_s", "formato_productos")
            .agg(F.round(100 * F.exp(F.avg(F.log("relativo"))), 3).alias("canasta"))
            .toPandas())

    idx = (spark.table(G_INDICE).withColumn("fecha_s", F.col("fecha").cast("string"))
           .select("fecha_s", "formato_productos", "indice").toPandas())

    fig, axes = plt.subplots(1, 2, figsize=(13, 4), sharey=False)
    for ax, formato in zip(axes, FORMATOS):
        e = evol[evol["formato_productos"] == formato].sort_values("fecha_s")
        i = idx[idx["formato_productos"] == formato].sort_values("fecha_s")
        base_i = i.loc[i["fecha_s"] == F0, "indice"]
        if not e.empty:
            ax.plot(e["fecha_s"], e["canasta"], marker="o", color=COLOR[formato], label="Canasta de EAN")
        if len(base_i) and base_i.iloc[0] > 0:
            i = i[i["fecha_s"] >= F0]
            ax.plot(i["fecha_s"], 100 * i["indice"] / base_i.iloc[0], marker="s", ls="--", color="gray",
                    label="Índice general (Gold)")
        ax.set_title(formato.capitalize())
        ax.set_ylabel(f"Índice (100 = {F0})")
        ax.tick_params(axis="x", rotation=45)
        ax.legend()
    fig.suptitle("Evolución de la canasta vs. índice general", fontsize=13)
    plt.tight_layout()
    plt.show()
else:
    print("Este gráfico necesita al menos 3 fechas en Gold y una canasta no vacía.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. ¿El mayorista de un relevamiento anterior se refleja en el minorista del siguiente?
# MAGIC
# MAGIC Se calcula, para cada EAN y cada par de fechas consecutivas, el cambio de precio (mismo comercio, mediana entre
# MAGIC comercios). Después se une el cambio **mayorista** del período *k* con el cambio **minorista** del período *k + 1*,
# MAGIC para el mismo EAN. Como referencia también se arma el cruce del **mismo período** (sin rezago).

# COMMAND ----------

calendario = spark.createDataFrame(
    [(fechas[i], fechas[i - 1], i) for i in range(1, len(fechas))],
    "fecha_s string, fecha_prev string, k int")
actual = precios.join(calendario, "fecha_s")
previo = (precios.select(F.col("fecha_s").alias("fecha_prev"), "formato_productos", "id_comercio", "id_gtin14",
                         F.col("precio").alias("p0")))

variacion = (actual.join(previo, ["fecha_prev", "formato_productos", "id_comercio", "id_gtin14"])
             .withColumn("relativo", F.col("precio") / F.col("p0"))
             .filter(F.col("relativo").between(REL_MIN, REL_MAX))
             .groupBy("k", "fecha_prev", "fecha_s", "formato_productos", "id_gtin14")
             .agg(F.countDistinct("id_comercio").alias("n"),
                  F.percentile_approx("relativo", 0.5).alias("relativo"))
             .filter(F.col("n") >= MIN_COM_PAR))

may = (variacion.filter(F.col("formato_productos") == "mayorista")
       .select("k", "id_gtin14", ((F.col("relativo") - 1) * 100).alias("var_mayorista"),
               F.col("fecha_prev").alias("mayorista_desde"), F.col("fecha_s").alias("mayorista_hasta")))
mino = (variacion.filter(F.col("formato_productos") == "minorista")
        .select(F.col("k").alias("k_min"), "id_gtin14", ((F.col("relativo") - 1) * 100).alias("var_minorista"),
                F.col("fecha_prev").alias("minorista_desde"), F.col("fecha_s").alias("minorista_hasta")))

con_rezago = may.withColumn("k_min", F.col("k") + 1).join(mino, ["k_min", "id_gtin14"]).toPandas()
mismo_periodo = may.withColumn("k_min", F.col("k")).join(mino, ["k_min", "id_gtin14"]).toPandas()

en_ambos = (may.select("id_gtin14").distinct().join(mino.select("id_gtin14").distinct(), "id_gtin14").count())
print(f"EAN con variación mayorista y minorista (≥ {MIN_COM_PAR} comercios por canal): {en_ambos}")
print(f"Pares EAN × período con rezago: {len(con_rezago)} | del mismo período: {len(mismo_periodo)}")

# COMMAND ----------

def ajuste(x, y):
    """Pendiente, ordenada y correlación de la recta y = m x + b (None si no hay datos suficientes)."""
    if len(x) < 5 or np.std(x) == 0 or np.std(y) == 0:
        return None
    m, b = np.polyfit(x, y, 1)
    return m, b, np.corrcoef(x, y)[0, 1]


def grafico_traslado(df, titulo, etiqueta_x, etiqueta_y):
    if df.empty:
        print(f"{titulo}: no hay pares de EAN para graficar.")
        return
    x, y = df["var_mayorista"].to_numpy(), df["var_minorista"].to_numpy()
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 5))

    # Izquierda: cada punto es un EAN en un par de períodos.
    lx = np.percentile(x, [1, 99])
    ly = np.percentile(y, [1, 99])
    if len(df) > 2000:
        hb = ax1.hexbin(x, y, gridsize=40, cmap="Blues", mincnt=1, extent=(lx[0], lx[1], ly[0], ly[1]))
        fig.colorbar(hb, ax=ax1, label="cantidad de EAN")
    else:
        ax1.scatter(x, y, alpha=0.4, color=COLOR["minorista"], s=14)
    ax1.set_xlim(lx[0] - 0.5, lx[1] + 0.5)
    ax1.set_ylim(ly[0] - 0.5, ly[1] + 0.5)
    ax1.axhline(0, color="gray", lw=0.7)
    ax1.axvline(0, color="gray", lw=0.7)
    fit = ajuste(x, y)
    if fit:
        m, b, r = fit
        xs = np.array([lx[0], lx[1]])
        ax1.plot(xs, m * xs + b, color="black", lw=2, label=f"recta: pendiente {m:.2f}, correlación {r:.2f}")
        ax1.legend(loc="upper left")
    ax1.set_xlabel(etiqueta_x)
    ax1.set_ylabel(etiqueta_y)
    ax1.set_title(f"{len(df):,} pares EAN × período".replace(",", "."))

    # Derecha: promedio del cambio minorista según lo que hizo el mayorista.
    grupo = np.where(x < -0.5, "Mayorista bajó", np.where(x > 0.5, "Mayorista subió", "Mayorista sin cambio"))
    tabla = (pd.DataFrame({"grupo": grupo, "y": y}).groupby("grupo")["y"]
             .agg(["mean", "std", "count"]).reindex(["Mayorista bajó", "Mayorista sin cambio", "Mayorista subió"])
             .dropna(subset=["mean"]))
    err = (tabla["std"] / np.sqrt(tabla["count"])).fillna(0)
    nombres = [f"{nombre}\n(n = {int(fila['count'])})" for nombre, fila in tabla.iterrows()]
    colores = {"Mayorista bajó": "#27ae60", "Mayorista sin cambio": "#7f8c8d", "Mayorista subió": "#c0392b"}
    ax2.bar(nombres, tabla["mean"], yerr=err, color=[colores[n] for n in tabla.index], capsize=6)
    ax2.axhline(0, color="black", lw=0.8)
    ax2.set_ylabel("Cambio minorista promedio (%)")
    ax2.set_title("Cambio minorista según lo que hizo el mayorista (±1 error estándar)")
    fig.suptitle(titulo, fontsize=13)
    plt.tight_layout()
    plt.show()
    if fit:
        print(f"{titulo}: por cada 1 punto de variación mayorista, el minorista varió {fit[0]:.2f} puntos "
              f"(correlación {fit[2]:.2f}, {len(df)} pares).")


if con_rezago.empty:
    print(f"No hay pares con rezago: hacen falta al menos 3 fechas y EAN en los dos canales (hoy hay {len(fechas)} fechas).")
else:
    grafico_traslado(con_rezago, "Mayorista de un relevamiento → minorista del siguiente",
                     "Cambio mayorista en el período k (%)", "Cambio minorista en el período k + 1 (%)")

grafico_traslado(mismo_periodo, "Referencia: mayorista y minorista en el mismo período",
                 "Cambio mayorista en el período (%)", "Cambio minorista en el mismo período (%)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Vista agregada: índice mayorista vs. índice minorista
# MAGIC
# MAGIC Los dos índices de Gold, cada uno con base 100 en su primera fecha. Si hay semanas suficientes, también se
# MAGIC compara la inflación mayorista de la semana anterior con la minorista de la semana (tabla semanal de Gold).

# COMMAND ----------

indices = (spark.table(G_INDICE).withColumn("fecha_s", F.col("fecha").cast("string"))
           .select("fecha_s", "formato_productos", "indice").toPandas())
semanal = spark.table(G_SEMANA).toPandas() if spark.catalog.tableExists(G_SEMANA) else pd.DataFrame()

hay_semanal = (not semanal.empty) and len(semanal[["var_mayorista_lag1", "var_minorista"]].dropna()) >= 5
fig, axes = plt.subplots(1, 2 if hay_semanal else 1, figsize=(14 if hay_semanal else 8, 4.5), squeeze=False)
ax = axes[0][0]
for formato in FORMATOS:
    d = indices[indices["formato_productos"] == formato].sort_values("fecha_s")
    ax.plot(d["fecha_s"], d["indice"], marker="o", color=COLOR[formato], label=formato.capitalize())
ax.set_ylabel("Índice (base 100 en la primera fecha)")
ax.set_title("Índices de precios de Gold")
ax.tick_params(axis="x", rotation=45)
ax.legend()

if hay_semanal:
    d = semanal[["var_mayorista_lag1", "var_minorista"]].dropna() * 100
    ax2 = axes[0][1]
    ax2.scatter(d["var_mayorista_lag1"], d["var_minorista"], color=COLOR["minorista"])
    fit = ajuste(d["var_mayorista_lag1"].to_numpy(), d["var_minorista"].to_numpy())
    if fit:
        xs = np.array([d["var_mayorista_lag1"].min(), d["var_mayorista_lag1"].max()])
        ax2.plot(xs, fit[0] * xs + fit[1], color="black", label=f"pendiente {fit[0]:.2f}, correlación {fit[2]:.2f}")
        ax2.legend()
    ax2.set_xlabel("Inflación mayorista de la semana anterior (%)")
    ax2.set_ylabel("Inflación minorista de la semana (%)")
    ax2.set_title(f"Semanal ({len(d)} semanas)")
plt.tight_layout()
plt.show()
if not hay_semanal:
    print("Todavía no hay semanas suficientes (al menos 5 con rezago) para el gráfico semanal.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cómo leer estos gráficos
# MAGIC
# MAGIC - **Es una canasta, no el universo.** Los EAN más relevados son los más confiables, pero no representan a todos
# MAGIC   los productos. Los resultados describen esa muestra.
# MAGIC - **Con pocas fechas no hay conclusión sólida.** Una pendiente y una correlación calculadas sobre pocos períodos
# MAGIC   describen esos días, no una relación estable. Para afirmar un traslado de precios hacen falta varios meses.
# MAGIC - **Correlación no es causalidad.** Que el minorista suba después del mayorista no prueba que lo siga: pueden
# MAGIC   influir ajustes generales de precios, el dólar o la estacionalidad.
# MAGIC - **El precio mayorista es unitario dentro del bulto** y el minorista es de lista. Por eso se compara cada canal
# MAGIC   contra sí mismo en el tiempo (cambio porcentual) y no el nivel de un canal contra el otro.
# MAGIC - **Cambios extremos excluidos.** Los productos cuyo precio se multiplica por más de 2 o menos de 0,5 entre dos
# MAGIC   fechas no entran, igual que en Gold.
