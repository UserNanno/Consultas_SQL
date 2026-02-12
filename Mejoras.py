from pyspark.sql import functions as F
from pyspark.sql.window import Window

def build_atencion_analista_primera_fmt(df_estados_enriq):

    # 1) Filtrar solo pasos de analista
    paso_tc_analista  = (F.col("NBRPASO") == "APROBACION DE CREDITOS ANALISTA")
    paso_cef_analista = (F.col("NBRPASO") == "EVALUACION DE SOLICITUD")

    df_paso = df_estados_enriq.filter(paso_tc_analista | paso_cef_analista)

    # 2) Tomar el inicio más antiguo (cuando llegó al analista)
    w_first = Window.partitionBy("CODSOLICITUD").orderBy(
        F.col("FECHORINICIOEVALUACION").asc_nulls_last(),
        F.col("FECHORFINEVALUACION").asc_nulls_last()
    )

    df_first = (
        df_paso
        .withColumn("rn", F.row_number().over(w_first))
        .filter(F.col("rn") == 1)
        .select(
            "CODSOLICITUD",
            F.col("FECHORINICIOEVALUACION").alias("TS_LLEGA_ANALISTA"),
            F.col("FECHORFINEVALUACION").alias("TS_FIN_ANALISTA"),
            F.col("ESTADOSOLICITUDPASO").alias("ESTADO_ANALISTA")
        )
    )

    # 3) Formatear fecha-hora
    df_first = (
        df_first
        .withColumn(
            "FH_LLEGA_ANALISTA",
            F.date_format("TS_LLEGA_ANALISTA", "yyyy-MM-dd HH:mm")
        )
        .withColumn(
            "FH_FIN_ANALISTA",
            F.date_format("TS_FIN_ANALISTA", "yyyy-MM-dd HH:mm")
        )
    )

    return df_first



df_atencion_analista = build_atencion_analista_primera_fmt(df_estados_enriq)


df_final = (
    df_last_estado
    .join(df_atencion_analista, on="CODSOLICITUD", how="left")
    .join(df_matanalista, on="CODSOLICITUD", how="left")
    .join(df_prod_snap.select(...), on="CODSOLICITUD", how="left")
)


"FH_LLEGA_ANALISTA",
"FH_FIN_ANALISTA",
"ESTADO_ANALISTA",
