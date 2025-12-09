df_repeticiones = (
    df_salesforce_estados
        .groupBy("CODSOLICITUD")
        .count()
        .withColumnRenamed("count", "CANTIDAD_REPETICIONES")
)
