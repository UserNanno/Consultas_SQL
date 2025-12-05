df_salesforce = (
    df_salesforce_raw
        .select(
            F.col("Fecha de inicio del paso").alias("FECINICIOEVALUACION_TXT"),
            F.col("Fecha de finalización del paso").alias("FECFINEVALUACION_TXT"),
            F.col("Nombre del registro").alias("CODSOLICITUD"),
            F.col("Estado").alias("ESTADOSOLICITUD"),
            F.col("Paso: Nombre").alias("NBRPASO"),
            F.col("Último actor: Nombre completo").alias("NBRANALISTA"),
            F.col("Proceso de aprobación: Nombre").alias("PROCESO"),
        )
)



from pyspark.sql import functions as F

def parse_fecha_hora_esp_col(col):
    s = F.col(col).cast("string")
    s = F.trim(s)
    s = F.regexp_replace(s, r'\s+', ' ')
    s = F.regexp_replace(s, r'(?i)a\.?\s*m\.?', 'AM')
    s = F.regexp_replace(s, r'(?i)p\.?\s*m\.?', 'PM')
    return F.to_timestamp(s, 'dd/MM/yyyy hh:mm a')


df_salesforce = (
    df_salesforce
        # 1. Parsear texto → timestamp
        .withColumn(
            "FECHORINICIOEVALUACION",
            parse_fecha_hora_esp_col("FECINICIOEVALUACION_TXT")
        )
        .withColumn(
            "FECHORFINEVALUACION",
            parse_fecha_hora_esp_col("FECFINEVALUACION_TXT")
        )

        # 2. Solo fecha (tipo date)
        .withColumn(
            "FECINICIOEVALUACION",
            F.to_date("FECHORINICIOEVALUACION")
        )
        .withColumn(
            "FECFINEVALUACION",
            F.to_date("FECHORFINEVALUACION")
        )

        # 3. Solo hora (string HH:mm:ss)
        .withColumn(
            "HORINICIOEVALUACION",
            F.date_format("FECHORINICIOEVALUACION", "HH:mm:ss")
        )
        .withColumn(
            "HORFINEVALUACION",
            F.date_format("FECHORFINEVALUACION", "HH:mm:ss")
        )

        # 4. AñoMes (YYYYMM)
        .withColumn(
            "CODMESEVALUACION",
            F.date_format("FECINICIOEVALUACION", "yyyyMM")
        )
)
