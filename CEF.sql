def build_estado_analista_snapshot(df_estados_enriq):
    """
    Devuelve 1 fila por CODSOLICITUD con:
    - ESTADOSOLICITUDANALISTA: última decisión del analista (APROBADO/RECHAZADO);
      si nunca hubo decisión (solo PENDIENTE) => PENDIENTE
    - TS_DECISION_ANALISTA: timestamp del evento (inicio del paso) donde ocurrió esa decisión (si existe)
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
    w_last_evt = Window.partitionBy("CODSOLICITUD").orderBy(
        F.col("FECHORINICIOEVALUACION").desc_nulls_last(),
        F.col("FECHORFINEVALUACION").desc_nulls_last()
    )

    df_last_evt = (
        df_paso
        .withColumn("rn_evt", F.row_number().over(w_last_evt))
        .filter(F.col("rn_evt") == 1)
        .select(
            "CODSOLICITUD",
            F.col("FECHORINICIOEVALUACION").alias("TS_ULTIMO_EVENTO_PASO_ANALISTA")
        )
    )

    # 3) Última decisión (APROBADO/RECHAZADO) ignorando PENDIENTE
    df_decisiones = df_paso.filter(F.col("ESTADOSOLICITUDPASO").isin("APROBADO", "RECHAZADO"))

    w_last_dec = Window.partitionBy("CODSOLICITUD").orderBy(
        F.col("FECHORINICIOEVALUACION").desc_nulls_last(),
        F.col("FECHORFINEVALUACION").desc_nulls_last()
    )

    df_last_dec = (
        df_decisiones
        .withColumn("rn_dec", F.row_number().over(w_last_dec))
        .filter(F.col("rn_dec") == 1)
        .select(
            "CODSOLICITUD",
            F.col("ESTADOSOLICITUDPASO").alias("ESTADOSOLICITUDANALISTA_DEC"),
            F.col("FECHORINICIOEVALUACION").alias("TS_DECISION_ANALISTA")
        )
    )

    # 4) Ensamble final:
    # - Si hay decisión => usa esa
    # - Si no hay decisión pero sí hay eventos del paso => PENDIENTE
    # - Si no hay ni siquiera eventos del paso => null
    out = (
        df_last_evt
        .join(df_last_dec, on="CODSOLICITUD", how="left")
        .withColumn(
            "ESTADOSOLICITUDANALISTA",
            F.coalesce(F.col("ESTADOSOLICITUDANALISTA_DEC"), F.lit("PENDIENTE"))
        )
        .drop("ESTADOSOLICITUDANALISTA_DEC")
    )

    return out
