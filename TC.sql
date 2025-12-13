df_sf_estados_raw = (
    spark.read
        .format("csv")
        .option("header", "true")
        .option("encoding", "ISO-8859-1")
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
        )
)

