from pyspark.sql import functions as F

df_salesforce_estados = (
    df_salesforce_estados
        # EXTRAER FECHA
        .withColumn("FECINICIOEVALUACION", F.to_date("FECHORINICIOEVALUACION"))
        .withColumn("FECFINEVALUACION", F.to_date("FECHORFINEVALUACION"))

        # EXTRAER HORA (en string HH:mm:ss)
        .withColumn("HORINICIOEVALUACION", F.date_format("FECHORINICIOEVALUACION", "HH:mm:ss"))
        .withColumn("HORFINEVALUACION", F.date_format("FECHORFINEVALUACION", "HH:mm:ss"))

        # CODMESEVALUACION = YYYYMM
        .withColumn("CODMESEVALUACION",
                    F.date_format("FECINICIOEVALUACION", "yyyyMM"))
)
