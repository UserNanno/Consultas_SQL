df_repetidas = (
    df_salesforce_estados
        .groupBy("CODSOLICITUD")
        .count()
        .withColumnRenamed("count", "REPETICIONES")
)
