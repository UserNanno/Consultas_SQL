df_sf_estados_raw = (
    spark.read
        .format("csv")
        .option("header", "true")
        .option("encoding", "UTF-8")
        .load(PATH_SF_ESTADOS)
)

df_salesforce_estados = (
    df_sf_estados_raw
        .select(
            F.col("Fecha de inicio del paso").alias("FECINICIOEVALUACION"),
            F.col("Fecha de finalización del paso").alias("FECFINEVALUACION"),
            F.col("Nombre del registro").alias("CODSOLICITUD"),
            F.col("Estado").alias("ESTADOSOLICITUD"),
            F.col("Paso: Nombre").alias("NBRPASO"),
            F.col("Último actor: Nombre completo").alias("NBRULTACTOR"),
            F.col("Último actor del paso: Nombre completo").alias("NBRULTACTORPASO"),
            F.col("Proceso de aprobación: Nombre").alias("PROCESO"),
            F.col("Estado del paso").alias("ESTADOSOLICITUDPASO")
        )
)

df_salesforce_estados = (
    df_salesforce_estados
        .withColumn("ESTADOSOLICITUD", norm_txt_spark("ESTADOSOLICITUD"))
        .withColumn("ESTADOSOLICITUDPASO", norm_txt_spark("ESTADOSOLICITUDPASO"))
        .withColumn("NBRPASO", norm_txt_spark("NBRPASO"))
        .withColumn("NBRULTACTOR", norm_txt_spark("NBRULTACTOR"))
        .withColumn("NBRULTACTORPASO", norm_txt_spark("NBRULTACTORPASO"))
        .withColumn("PROCESO", norm_txt_spark("PROCESO"))
)

df_salesforce_estados = (
    df_salesforce_estados
        .withColumn("FECHORINICIOEVALUACION", parse_fecha_hora_esp_col("FECINICIOEVALUACION"))
        .withColumn("FECHORFINEVALUACION", parse_fecha_hora_esp_col("FECFINEVALUACION"))
        .withColumn("FECINICIOEVALUACION", F.to_date("FECHORINICIOEVALUACION"))
        .withColumn("FECFINEVALUACION", F.to_date("FECHORFINEVALUACION"))
        .withColumn("HORINICIOEVALUACION", F.date_format("FECHORINICIOEVALUACION", "HH:mm:ss"))
        .withColumn("HORFINEVALUACION", F.date_format("FECHORFINEVALUACION", "HH:mm:ss"))
        .withColumn("CODMESEVALUACION", F.date_format("FECINICIOEVALUACION", "yyyyMM"))
)

df_salesforce_estados = df_salesforce_estados.withColumn(
    "CODMESEVALUACION", F.col("CODMESEVALUACION").cast("string")
)

print("Registros originales SF estados:", df_salesforce_estados.count())




AnalysisException: [UNRESOLVED_COLUMN.WITH_SUGGESTION] A column or function parameter with name `Fecha de finalización del paso` cannot be resolved. Did you mean one of the following? [`Fecha de finalizaci�n del paso`, `Fecha de inicio del paso`, `Estado del paso`, `Proceso de aprobaci�n: Nombre`, `Nombre del registro`].;
