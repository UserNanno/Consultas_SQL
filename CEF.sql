def build_estado_analista_snapshot(df_estados_enriq):
    # ... (misma definición de es_paso_analista y df_paso)

    # Último evento del paso analista (incluye PENDIENTE)
    w_last_evt = Window.partitionBy("CODSOLICITUD").orderBy(
        F.col("FECHORINICIOEVALUACION").desc_nulls_last(),
        F.col("FECHORFINEVALUACION").desc_nulls_last()
    )
    df_last_evt = (
        df_paso
        .withColumn("rn_evt", F.row_number().over(w_last_evt))
        .filter("rn_evt = 1")
        .select(
            "CODSOLICITUD",
            F.col("FECHORINICIOEVALUACION").alias("TS_ULTIMO_EVENTO_PASO_ANALISTA")
        )
    )

    # 1) Última decisión fuerte: APROBADO/RECHAZADO
    df_dec_fuerte = df_paso.filter(F.col("ESTADOSOLICITUDPASO").isin("APROBADO", "RECHAZADO"))
    w_last_dec = Window.partitionBy("CODSOLICITUD").orderBy(
        F.col("FECHORINICIOEVALUACION").desc_nulls_last(),
        F.col("FECHORFINEVALUACION").desc_nulls_last()
    )
    df_last_fuerte = (
        df_dec_fuerte
        .withColumn("rn", F.row_number().over(w_last_dec))
        .filter("rn = 1")
        .select(
            "CODSOLICITUD",
            F.col("ESTADOSOLICITUDPASO").alias("ESTADO_FUERTE"),
            F.col("FECHORINICIOEVALUACION").alias("TS_FUERTE")
        )
    )

    # 2) Última decisión alternativa: RECUPERADA
    df_dec_rec = df_paso.filter(F.col("ESTADOSOLICITUDPASO") == "RECUPERADA")
    df_last_rec = (
        df_dec_rec
        .withColumn("rn", F.row_number().over(w_last_dec))
        .filter("rn = 1")
        .select(
            "CODSOLICITUD",
            F.col("ESTADOSOLICITUDPASO").alias("ESTADO_REC"),
            F.col("FECHORINICIOEVALUACION").alias("TS_REC")
        )
    )

    # Ensamble:
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
