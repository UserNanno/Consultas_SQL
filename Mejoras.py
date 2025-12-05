from pyspark.sql import functions as F

def parse_fecha_hora_esp_col(col):
    s = F.lower(F.trim(col))
    s = F.regexp_replace(s, r'\s+', ' ')
    s = F.regexp_replace(s, r'(?i)a\.?\s*m\.?', 'AM')
    s = F.regexp_replace(s, r'(?i)p\.?\s*m\.?', 'PM')
    return F.to_timestamp(s, 'dd/MM/yyyy hh:mm a')



df_salesforce = (
    df_salesforce
        .withColumn("FECHORINICIOEVALUACION", parse_fecha_hora_esp_col(F.col("FECINICIOEVALUACION")))
        .withColumn("FECHORFINEVALUACION",    parse_fecha_hora_esp_col(F.col("FECFINEVALUACION")))
        .withColumn("FECINICIOEVALUACION", F.to_date("FECHORINICIOEVALUACION"))
        .withColumn("FECFINEVALUACION",   F.to_date("FECHORFINEVALUACION"))
        .withColumn("HORINICIOEVALUACION", F.date_format("FECHORINICIOEVALUACION", "HH:mm:ss"))
        .withColumn("HORFINEVALUACION",   F.date_format("FECHORFINEVALUACION", "HH:mm:ss"))
        .withColumn("CODMESEVALUACION",   F.date_format("FECINICIOEVALUACION", "yyyyMM"))
)
