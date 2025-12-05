from pyspark.sql import functions as F

def parse_fecha_hora_esp_col(col):
    s = F.lower(F.trim(col))
    s = F.regexp_replace(s, r'\s+', ' ')
    s = F.regexp_replace(s, r'(?i)a\.?\s*m\.?', 'AM')
    s = F.regexp_replace(s, r'(?i)p\.?\s*m\.?', 'PM')
    return F.to_timestamp(s, 'dd/MM/yyyy hh:mm a')
