from pyspark.sql import functions as F

df_salesforce = (
    df_salesforce
        .withColumn("ESTADOSOLICITUD", F.upper(F.col("ESTADOSOLICITUD")))
        .withColumn("NBRPASO", F.upper(F.col("NBRPASO")))
        .withColumn("NBRANALISTA", F.upper(F.col("NBRANALISTA")))
        .withColumn("PROCESO", F.upper(F.col("PROCESO")))
)
