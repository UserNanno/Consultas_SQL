from pyspark.sql import functions as F
from pyspark.sql.window import Window

def build_atencion_analista_primera(df_estados_enriq):
    # 1) Definir paso analista (exactamente como indicas)
    is_tc  = F.col("PROCESO").like("%APROBACION CREDITOS TC%")
    is_cef = F.col("PROCESO").isin(
        "CO SOLICITUD APROBACIONES TLMK",
        "SFCP APROBACIONES EDUCATIVO",
        "CO SOLICITUD APROBACIONES"
    )

    paso_tc_analista  = (F.col("NBRPASO") == "APROBACION DE CREDITOS ANALISTA")
    paso_cef_analista = (F.col("NBRPASO") == "EVALUACION DE SOLICITUD")

    es_paso_analista = (is_tc & paso_tc_analista) | (is_cef & paso_cef_analista)

    df_paso = df_estados_enriq.filter(es_paso_analista)

    # 2) Elegir el evento más antiguo (cuando "llegó" al analista)
    # Tie-breakers: si hay mismos inicios, escoger el que tenga fin más antiguo (o no nulo primero)
    w_first = Window.partitionBy("CODSOLICITUD").orderBy(
        F.col("FECHORINICIOEVALUACION").asc_nulls_last(),
        F.col("FECHORFINEVALUACION").asc_nulls_last()
    )

    df_first_evt = (
        df_paso
        .withColumn("rn_first", F.row_number().over(w_first))
        .filter(F.col("rn_first") == 1)
        .select(
            "CODSOLICITUD",
            F.col("FECHORINICIOEVALUACION").alias("TS_LLEGA_ANALISTA"),
            F.col("FECHORFINEVALUACION").alias("TS_FIN_ATENCION_ANALISTA"),
            F.col("ESTADOSOLICITUDPASO").alias("ESTADO_ATENCION_ANALISTA"),
            F.col("NBRPASO").alias("PASO_ANALISTA_ATENCION"),
            F.col("PROCESO").alias("PROCESO_ATENCION"),
        )
    )

    # 3) Duración (si quieres)
    df_first_evt = df_first_evt.withColumn(
        "DURACION_MIN_ATENCION_ANALISTA",
        F.when(
            F.col("TS_LLEGA_ANALISTA").isNotNull() & F.col("TS_FIN_ATENCION_ANALISTA").isNotNull(),
            (F.unix_timestamp("TS_FIN_ATENCION_ANALISTA") - F.unix_timestamp("TS_LLEGA_ANALISTA")) / 60.0
        ).otherwise(F.lit(None))
    )

    return df_first_evt



df_atencion_analista = build_atencion_analista_primera(df_estados_enriq)

df_final = (
    df_last_estado
    .join(df_atencion_analista, on="CODSOLICITUD", how="left")
    .join(df_matanalista, on="CODSOLICITUD", how="left")
    .join(df_prod_snap.select(...), on="CODSOLICITUD", how="left")
)
