from pyspark.sql import functions as F
from pyspark.sql.window import Window

# ============================================================
# 0) REGLA CLAVE: el universo de CODSOLICITUD DEBE SALIR DE ESTADOS
#    - df_final_autonomias ya tiene 1 fila por CODSOLICITUD (ok)
#    - Igual, anclamos el join a ESTADOS (snapshot) para evitar
#      “meter” solicitudes que solo estén en PRODUCTOS.
# ============================================================

def build_last_estado_snapshot(df_estados_enriq):
    """
    Snapshot 1 fila por CODSOLICITUD (el registro MÁS RECIENTE),
    definido por FECHORINICIOEVALUACION y FECHORFINEVALUACION.
    """
    w_last = Window.partitionBy("CODSOLICITUD").orderBy(
        F.col("FECHORINICIOEVALUACION").desc_nulls_last(),
        F.col("FECHORFINEVALUACION").desc_nulls_last()
    )

    return (
        df_estados_enriq
          .withColumn("CODSOLICITUD", F.trim(F.col("CODSOLICITUD").cast("string")))
          .withColumn("rn_last", F.row_number().over(w_last))
          .filter(F.col("rn_last") == 1)
          .select(
              "CODSOLICITUD",
              # Campos de ESTADOS que quieres traer:
              F.col("PROCESO").alias("PROCESO"),
              F.col("ESTADOSOLICITUD").alias("ESTADOSOLICITUD"),
              F.col("ESTADOSOLICITUDPASO").alias("ESTADOSOLICITUDPASO"),
              # Timestamp del último evento de ESTADOS (útil para auditoría)
              F.col("FECHORINICIOEVALUACION").alias("TS_ULTIMO_EVENTO_ESTADOS")
          )
    )

def build_productos_snapshot(df_productos_enriq):
    """
    Snapshot 1 fila por CODSOLICITUD (el registro MÁS RECIENTE en productos),
    definido por FECCREACION.
    OJO: este snapshot se une SOLO para traer atributos de negocio (monto, etapa, etc),
    pero NO define el universo de solicitudes.
    """
    w_prod = Window.partitionBy("CODSOLICITUD").orderBy(
        F.col("FECCREACION").desc_nulls_last()
    )

    return (
        df_productos_enriq
          .withColumn("CODSOLICITUD", F.trim(F.col("CODSOLICITUD").cast("string")))
          .withColumn("rn_prod", F.row_number().over(w_prod))
          .filter(F.col("rn_prod") == 1)
          .select(
              "CODSOLICITUD",
              "ETAPA",
              "TIPACCION",
              "NBRDIVISA",
              "MTOSOLICITADO",
              "MTOAPROBADO",
              "MTOOFERTADO",
              "MTODESEMBOLSADO",
              F.col("FECCREACION").alias("TS_PRODUCTOS")
          )
    )

# ============================================================
# 1) Snapshots
# ============================================================
df_last_estado = build_last_estado_snapshot(df_estados_enriq)
df_prod_snap   = build_productos_snapshot(df_productos_enriq)

# ============================================================
# 2) ENSAMBLE FINAL (ANCLADO A ESTADOS)
#    - Si df_final_autonomias ya viene de estados, normalmente bastaría
#      left join a productos.
#    - PERO para blindar que SOLO salgan solicitudes que existan en ESTADOS,
#      hacemos: df_last_estado (base) -> join df_final_autonomias -> join productos
# ============================================================

df_final_autonomias_1 = (
    df_final_autonomias
      .withColumn("CODSOLICITUD", F.trim(F.col("CODSOLICITUD").cast("string")))
      .dropDuplicates(["CODSOLICITUD"])  # ya dijiste 1 fila, esto es “seguro”
)

df_final = (
    df_last_estado.alias("e")                       # BASE: ESTADOS (universo)
      .join(df_final_autonomias_1.alias("a"), on="CODSOLICITUD", how="left")
      .join(df_prod_snap.alias("p"), on="CODSOLICITUD", how="left")
      .select(
          # ========= lo que YA tienes en autonomías =========
          F.col("CODSOLICITUD"),

          F.col("a.TS_BASE_ESTADOS").alias("TS_BASE_ESTADOS"),
          F.col("a.MATANALISTA_FINAL").alias("MATANALISTA_FINAL"),
          F.col("a.ORIGEN_MATANALISTA").alias("ORIGEN_MATANALISTA"),

          F.col("a.FLGAUTONOMIA").alias("FLGAUTONOMIA"),
          F.col("a.FLGAUTONOMIAOBSERVADA").alias("FLGAUTONOMIAOBSERVADA"),
          F.col("a.NIVEL_AUTONOMIA").alias("NIVEL_AUTONOMIA"),
          F.col("a.MATAUTONOMIA").alias("MATAUTONOMIA"),
          F.col("a.PASO_AUTONOMIA").alias("PASO_AUTONOMIA"),
          F.col("a.TS_AUTONOMIA").alias("TS_AUTONOMIA"),

          # ========= lo que quieres agregar (ESTADOS) =========
          F.col("e.PROCESO").alias("PROCESO"),
          F.col("e.ESTADOSOLICITUD").alias("ESTADOSOLICITUD"),
          F.col("e.ESTADOSOLICITUDPASO").alias("ESTADOSOLICITUDPASO"),

          # ========= lo que quieres agregar (PRODUCTOS) =========
          F.col("p.ETAPA").alias("ETAPA"),
          F.col("p.TIPACCION").alias("TIPACCION"),
          F.col("p.NBRDIVISA").alias("NBRDIVISA"),
          F.col("p.MTOSOLICITADO").alias("MTOSOLICITADO"),
          F.col("p.MTOAPROBADO").alias("MTOAPROBADO"),
          F.col("p.MTOOFERTADO").alias("MTOOFERTADO"),
          F.col("p.MTODESEMBOLSADO").alias("MTODESEMBOLSADO"),

          # opcional (útil para trazabilidad)
          F.col("e.TS_ULTIMO_EVENTO_ESTADOS").alias("TS_ULTIMO_EVENTO_ESTADOS"),
          F.col("p.TS_PRODUCTOS").alias("TS_PRODUCTOS"),
      )
)

# df_final ahora es 1 fila por CODSOLICITUD y SOLO incluye solicitudes presentes en SF_ESTADOS.
