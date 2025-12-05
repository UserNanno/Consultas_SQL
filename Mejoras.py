from pyspark.sql import functions as F

def parse_fecha_hora_esp_col(col):
    # col: objeto Column de PySpark (F.col("nombre_columna"))
    
    # Pasamos a minúsculas y limpiamos espacios extra
    s = F.lower(F.trim(col))
    s = F.regexp_replace(s, r'\s+', ' ')  # colapsar espacios múltiples

    # Normalizar variantes de a. m. / p. m. a AM / PM
    # (?i) = case-insensitive
    s = F.regexp_replace(s, r'(?i)a\.?\s*m\.?', 'AM')
    s = F.regexp_replace(s, r'(?i)p\.?\s*m\.?', 'PM')

    # Parsear al timestamp con día/mes/año y hora 12h
    # dd/MM/yyyy -> día/mes/año
    # hh:mm      -> hora 12h
    # a          -> AM/PM
    return F.to_timestamp(s, 'dd/MM/yyyy hh:mm a')
