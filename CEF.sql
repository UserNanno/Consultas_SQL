from pyspark.sql import functions as F
from pyspark.sql.window import Window

def build_estado_analista_snapshot(df_estados_enriq):
    """
    Devuelve 1 fila por CODSOLICITUD con:
    - ESTADOSOLICITUDANALISTA: última "decisión" del analista con prioridad:
        1) APROBADO/RECHAZADO (más reciente)
        2) RECUPERADA (más reciente)
        3) PENDIENTE (si nunca hubo APROBADO/RECHAZADO/RECUPERADA)
    - TS_DECISION_ANALISTA: timestamp del evento donde ocurrió la decisión elegida (si existe)
    - TS_ULTIMO_EVENTO_PASO_ANALISTA: timestamp del último evento del paso analista (incluye PENDIENTE)
    """

    # 1) Definir "paso de analista" (misma lógica que atribución)
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

    # 2) Último evento del paso analista (incluye PENDIENTE)
    w_evt = Window.partitionBy("CODSOLICITUD").orderBy(
        F.col("FECHORINICIOEVALUACION").desc_nulls_last(),
        F.col("FECHORFINEVALUACION").desc_nulls_last()
    )

    df_last_evt = (
        df_paso
        .withColumn("rn_evt", F.row_number().over(w_evt))
        .filter(F.col("rn_evt") == 1)
        .select(
            "CODSOLICITUD",
            F.col("FECHORINICIOEVALUACION").alias("TS_ULTIMO_EVENTO_PASO_ANALISTA")
        )
    )

    # 3) Última decisión fuerte (APROBADO/RECHAZADO) ignorando PENDIENTE
    df_fuerte = df_paso.filter(F.col("ESTADOSOLICITUDPASO").isin("APROBADO", "RECHAZADO"))

    df_last_fuerte = (
        df_fuerte
        .withColumn("rn", F.row_number().over(w_evt))
        .filter(F.col("rn") == 1)
        .select(
            "CODSOLICITUD",
            F.col("ESTADOSOLICITUDPASO").alias("ESTADO_FUERTE"),
            F.col("FECHORINICIOEVALUACION").alias("TS_FUERTE")
        )
    )

    # 4) Si no hubo fuerte, última decisión alternativa: RECUPERADA
    df_rec = df_paso.filter(F.col("ESTADOSOLICITUDPASO") == "RECUPERADA")

    df_last_rec = (
        df_rec
        .withColumn("rn", F.row_number().over(w_evt))
        .filter(F.col("rn") == 1)
        .select(
            "CODSOLICITUD",
            F.col("ESTADOSOLICITUDPASO").alias("ESTADO_REC"),
            F.col("FECHORINICIOEVALUACION").alias("TS_REC")
        )
    )

    # 5) Ensamble final con prioridad: fuerte > recuperada > pendiente
    out = (
        df_last_evt
        .join(df_last_fuerte, on="CODSOLICITUD", how="left")
        .join(df_last_rec,   on="CODSOLICITUD", how="left")
        .withColumn(
            "ESTADOSOLICITUDANALISTA",
            F.when(F.col("ESTADO_FUERTE").isNotNull(), F.col("ESTADO_FUERTE"))
             .when(F.col("ESTADO_REC").isNotNull(),    F.col("ESTADO_REC"))
             .otherwise(F.lit("PENDIENTE"))
        )
        .withColumn(
            "TS_DECISION_ANALISTA",
            F.when(F.col("ESTADO_FUERTE").isNotNull(), F.col("TS_FUERTE"))
             .when(F.col("ESTADO_REC").isNotNull(),    F.col("TS_REC"))
             .otherwise(F.lit(None).cast("timestamp"))
        )
        .drop("ESTADO_FUERTE", "TS_FUERTE", "ESTADO_REC", "TS_REC")
    )

    return out
