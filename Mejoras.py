df_salesforce = (
    df_salesforce
        # Parseo de texto → timestamp
        .withColumn("FECHORINICIOEVALUACION", parse_fecha_hora_esp_col("FECINICIOEVALUACION"))
        .withColumn("FECHORFINEVALUACION",    parse_fecha_hora_esp_col("FECFINEVALUACION"))

        # Extraer solo fecha
        .withColumn("FECINICIOEVALUACION", F.to_date("FECHORINICIOEVALUACION"))
        .withColumn("FECFINEVALUACION",   F.to_date("FECHORFINEVALUACION"))

        # Extraer solo hora
        .withColumn("HORINICIOEVALUACION", F.date_format("FECHORINICIOEVALUACION", "HH:mm:ss"))
        .withColumn("HORFINEVALUACION",   F.date_format("FECHORFINEVALUACION", "HH:mm:ss"))

        # Año + mes en formato YYYYMM
        .withColumn("CODMESEVALUACION", F.date_format("FECINICIOEVALUACION", "yyyyMM"))
)
