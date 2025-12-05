from pyspark.sql import functions as F

def parse_fecha_hora_esp_col(col):
    # Aseguramos string
    s = F.col(col).cast("string")
    
    # 1) Normalizar espacios (incluyendo espacios no separables de Salesforce)
    # Reemplazamos NBSP y NARROW NBSP por espacio normal
    s = F.regexp_replace(s, u'\u00A0', ' ')
    s = F.regexp_replace(s, u'\u202F', ' ')
    
    # Pasamos a minúsculas y recortamos
    s = F.lower(F.trim(s))
    
    # Colapsar espacios múltiples en uno
    s = F.regexp_replace(s, r'\s+', ' ')
    
    # 2) Normalizar "a. m." / "p. m." -> AM / PM
    # \W = "no palabra": puntos, espacios, NBSP, etc.
    s = F.regexp_replace(s, r'(?i)a\W*m\W*', 'AM')
    s = F.regexp_replace(s, r'(?i)p\W*m\W*', 'PM')

    # 3) Parsear al timestamp con formato día/mes/año + hora 12h
    # dd/MM/yyyy -> día/mes/año
    # hh:mm      -> hora 12h
    # a          -> AM/PM
    return F.to_timestamp(s, 'dd/MM/yyyy hh:mm a')



df_salesforce_estados = (
    df_salesforce_estados
        .withColumn(
            "FECHORINICIOEVALUACION",
            parse_fecha_hora_esp_col("FECINICIOEVALUACION")
        )
        .withColumn(
            "FECHORFINEVALUACION",
            parse_fecha_hora_esp_col("FECFINEVALUACION")
        )
)




df_salesforce_estados.select(
    "FECINICIOEVALUACION",
    parse_fecha_hora_esp_col("FECINICIOEVALUACION").alias("PARS_FECHORINICIO")
).show(truncate=False)
