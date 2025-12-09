from pyspark.sql.window import Window

w = Window.partitionBy("CODSOLICITUD").orderBy(F.col("FECHORINICIOEVALUACION").desc())

df_salesforce_estados_rn = (
    df_salesforce_estados
        .withColumn("rn", F.row_number().over(w))
)


df_salesforce_estados_unicos = (
    df_salesforce_estados_rn
        .filter(F.col("rn") == 1)
        .drop("rn")
)



df_salesforce_estados_descartados = (
    df_salesforce_estados_rn
        .filter(F.col("rn") > 1)
        .drop("rn")
)
