from pyspark.sql import functions as F

def parse_fecha_hora_esp_col(col):
    # Nos aseguramos de trabajar con string
    s = F.col(col).cast("string")
    
    # Limpiar espacios al inicio/fin y espacios múltiples
    s = F.trim(s)
    s = F.regexp_replace(s, r'\s+', ' ')
    
    # Pasar "a. m." / "p. m." (y variantes) a AM / PM
    s = F.regexp_replace(s, r'(?i)a\.?\s*m\.?', 'AM')
    s = F.regexp_replace(s, r'(?i)p\.?\s*m\.?', 'PM')
    
    # Ahora debería quedar así: "01/04/2025 10:31 AM"
    return F.to_timestamp(s, 'dd/MM/yyyy hh:mm a')



df_test = spark.createDataFrame(
    [
        ("01/04/2025 10:31 a. m.",),
        ("12/05/2025 04:59 p. m.",)
    ],
    ["FECINICIOEVALUACION"]
)

df_test = df_test.withColumn(
    "FECHORINICIOEVALUACION",
    parse_fecha_hora_esp_col("FECINICIOEVALUACION")
)

df_test.show(truncate=False)
