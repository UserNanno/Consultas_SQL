from pyspark.sql import functions as F
from pyspark.sql.window import Window

# =========================================================
# 1) SNAPSHOTS (1 fila por CODSOLICITUD) desde ESTADOS y PRODUCTOS
# =========================================================

def build_last_estado_snapshot(df_estados_enriq):
    """
    1 fila por CODSOLICITUD (el registro más reciente por timestamp de inicio/fin del paso).
    Aquí es donde conviene “colgar” todo lo que viene de SF_ESTADOS y debe respetar recencia.
    """
    w_last = Window.partitionBy("CODSOLICITUD").orderBy(
        F.col("FECHORINICIOEVALUACION").desc_nulls_last(),
        F.col("FECHORFINEVALUACION").desc_nulls_last()
    )

    return (
        df_estados_enriq
          .withColumn("rn_last", F.row_number().over(w_last))
          .filter(F.col("rn_last") == 1)
          .select(
              "CODSOLICITUD",
              # Campos SF_ESTADOS
              F.col("PROCESO").alias("PROCESO"),
              F.col("ESTADOSOLICITUD").alias("ESTADOSOLICITUD"),
              F.col("ESTADOSOLICITUDPASO").alias("ESTADOSOLICITUDPASO"),
              # Timestamp del último evento (útil para trazabilidad)
              F.col("FECHORINICIOEVALUACION").alias("TS_ULTIMO_EVENTO_ESTADOS"),
              # Fecha inicio (base para CODMESEVALUACION)
              F.col("FECINICIOEVALUACION").alias("FECINICIOEVALUACION_ULT"),
              # YYYYMM desde FECINICIOEVALUACION
              F.date_format(F.col("FECINICIOEVALUACION"), "yyyyMM").cast("string").alias("CODMESEVALUACION")
          )
          .drop("rn_last")
    )


def build_productos_snapshot(df_productos_enriq):
    """
    1 fila por CODSOLICITUD (el producto más reciente por FECCREACION).
    Esto lo usas para traer ETAPA/TIPACCION/MONTOS/DIVISA/NBRPRODUCTO.
    """
    w_prod = Window.partitionBy("CODSOLICITUD").orderBy(F.col("FECCREACION").desc_nulls_last())

    return (
        df_productos_enriq
          .withColumn("rn_prod", F.row_number().over(w_prod))
          .filter(F.col("rn_prod") == 1)
          .select(
              "CODSOLICITUD",
              "NBRPRODUCTO",   # viene de SF_PRODUCTOS (si tú lo llamas así)
              "ETAPA",
              "TIPACCION",
              "NBRDIVISA",
              "MTOSOLICITADO",
              "MTOAPROBADO",
              "MTOOFERTADO",
              "MTODESEMBOLSADO",
              F.col("FECCREACION").alias("TS_PRODUCTOS")
          )
          .drop("rn_prod")
    )

# =========================================================
# 2) DERIVADOS FINALES
#    - PRODUCTO (TC/CEF) desde PROCESO
#    - MATSUPERIOR desde ORGÁNICO usando MATANALISTA_FINAL + CODMESEVALUACION
# =========================================================

def add_producto_tipo(df_final):
    is_tc  = F.col("PROCESO").like("%APROBACION CREDITOS TC%")
    is_cef = F.col("PROCESO").isin(
        "CO SOLICITUD APROBACIONES TLMK",
        "SFCP APROBACIONES EDUCATIVO",
        "CO SOLICITUD APROBACIONES"
    )
    return df_final.withColumn(
        "PRODUCTO",
        F.when(is_tc, F.lit("TC"))
         .when(is_cef, F.lit("CEF"))
         .otherwise(F.lit(None))
    )


def add_matsuperior_from_organico(df_final, df_org):
    """
    Trae MATSUPERIOR del analista final:
    Join por (CODMESEVALUACION == CODMES) y (MATANALISTA_FINAL == MATORGANICO).
    """
    df_org_key = (
        df_org
          .select(
              F.col("CODMES").cast("string").alias("CODMESEVALUACION"),
              F.col("MATORGANICO").alias("MATANALISTA_FINAL"),
              F.col("MATSUPERIOR").alias("MATSUPERIOR")
          )
          .dropDuplicates(["CODMESEVALUACION", "MATANALISTA_FINAL"])
    )

    return (
        df_final
          .join(df_org_key, on=["CODMESEVALUACION", "MATANALISTA_FINAL"], how="left")
    )

# =========================================================
# 3) ARMADO FINAL
#    df_final_autonomias ya es 1 fila por CODSOLICITUD
# =========================================================

df_last_estado = build_last_estado_snapshot(df_estados_enriq)
df_prod_snap   = build_productos_snapshot(df_productos_enriq)

df_final = (
    df_final_autonomias
      # 1) Traemos “foto” más reciente de ESTADOS (PROCESO/ESTADOS/CODMESEVALUACION)
      .join(df_last_estado, on="CODSOLICITUD", how="left")
      # 2) Traemos “foto” más reciente de PRODUCTOS (ETAPA/TIPACCION/DIVISA/MONTOS/NBRPRODUCTO)
      .join(df_prod_snap,   on="CODSOLICITUD", how="left")
)

# PRODUCTO = TC/CEF desde PROCESO
df_final = add_producto_tipo(df_final)

# MATSUPERIOR desde ORGÁNICO usando MATANALISTA_FINAL (y el mes de evaluación)
df_final = add_matsuperior_from_organico(df_final, df_org)

# =========================================================
# 4) Selección de columnas finales (opcional, para ordenar)
# =========================================================
df_final = df_final.select(
    # Core
    "CODSOLICITUD",
    "PRODUCTO",
    "CODMESEVALUACION",

    # Analista / autonomía (ya venían)
    "TS_BASE_ESTADOS",
    "MATANALISTA_FINAL",
    "ORIGEN_MATANALISTA",
    "MATSUPERIOR",
    "FLGAUTONOMIA",
    "FLGAUTONOMIAOBSERVADA",
    "NIVEL_AUTONOMIA",
    "MATAUTONOMIA",
    "PASO_AUTONOMIA",
    "TS_AUTONOMIA",

    # Estados (snapshot reciente)
    "PROCESO",
    "ESTADOSOLICITUD",
    "ESTADOSOLICITUDPASO",
    "TS_ULTIMO_EVENTO_ESTADOS",
    "FECINICIOEVALUACION_ULT",

    # Productos (snapshot reciente)
    "NBRPRODUCTO",
    "ETAPA",
    "TIPACCION",
    "NBRDIVISA",
    "MTOSOLICITADO",
    "MTOAPROBADO",
    "MTOOFERTADO",
    "MTODESEMBOLSADO",
    "TS_PRODUCTOS",
)

# df_final.show() / display(df_final)
