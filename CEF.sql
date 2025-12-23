from pyspark.sql import functions as F
from pyspark.sql.window import Window

def build_last_estado_snapshot(df_estados_enriq):
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
              F.col("PROCESO").alias("PROCESO_ESTADOS"),
              F.col("ESTADOSOLICITUD").alias("ESTADOSOLICITUD_ESTADOS"),
              F.col("ESTADOSOLICITUDPASO").alias("ESTADOSOLICITUDPASO_ESTADOS"),
              F.col("FECHORINICIOEVALUACION").alias("TS_ULTIMO_EVENTO_ESTADOS")
          )
    )





def build_productos_snapshot(df_productos_enriq):
    # Si hubiese duplicados, nos quedamos con el más reciente por FECCREACION
    w_prod = Window.partitionBy("CODSOLICITUD").orderBy(F.col("FECCREACION").desc_nulls_last())

    return (
        df_productos_enriq
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




df_last_estado = build_last_estado_snapshot(df_estados_enriq)
df_prod_snap   = build_productos_snapshot(df_productos_enriq)

df_final_autonomias_plus = (
    df_final_autonomias
      .join(df_last_estado, on="CODSOLICITUD", how="left")
      .join(df_prod_snap,   on="CODSOLICITUD", how="left")
      # Si quieres conservar los nombres EXACTOS que pediste (sin sufijos):
      .withColumnRenamed("PROCESO_ESTADOS", "PROCESO")
      .withColumnRenamed("ESTADOSOLICITUD_ESTADOS", "ESTADOSOLICITUD")
      .withColumnRenamed("ESTADOSOLICITUDPASO_ESTADOS", "ESTADOSOLICITUDPASO")
)
