from pyspark.sql import functions as F

# 1. Ruta de los archivos CSV (ajusta según tu entorno)
salesforce_path = "dbfs:/INPUT/SALESFORCE/INFORME_ESTADO_*.csv"

# 2. Lectura de los CSV de Salesforce
df_salesforce_raw = (
    spark.read
        .format("csv")
        .option("header", "true")
        .option("encoding", "ISO-8859-1")  # equivalente a latin1
        .load(salesforce_path)
)

# 3. Selección y renombrado de columnas
df_salesforce = (
    df_salesforce_raw
        .select(
            F.col("Nombre del registro").alias("CODSOLICITUD"),
            F.col("Estado").alias("ESTADOSOLICITUD"),
            F.col("Fecha de inicio del paso").alias("FECINICIOEVALUACION"),
            F.col("Fecha de finalización del paso").alias("FECFINEVALUACION"),
            F.col("Paso: Nombre").alias("NBRPASO"),
            F.col("Último actor: Nombre completo").alias("NBRANALISTA"),
            F.col("Proceso de aprobación: Nombre").alias("PROCESO"),
        )
)
