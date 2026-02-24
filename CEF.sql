Revisa este codigo que tengo en databricks pyspark

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import DecimalType


PATH_PA_SOLICITUDES = "abfss://bcp-edv-rbmbdn@adlscu1lhclbackp05.dfs.core.windows.net/T72496/CARGA/POWERAPPS/1n_Apps_*.csv"

def parse_fecha_hora_esp(col):
    s = col.cast("string")
    s = F.regexp_replace(s, u"\u00A0", " ")
    s = F.regexp_replace(s, u"\u202F", " ")
    s = F.lower(F.trim(s))
    s = F.regexp_replace(s, r"\s+", " ")
    s = F.regexp_replace(s, r"(?i)a\W*m\W*", "AM")
    s = F.regexp_replace(s, r"(?i)p\W*m\W*", "PM")
    return F.to_timestamp(s, "dd/MM/yyyy hh:mm a")

def quitar_tildes(col):
    c = F.regexp_replace(col, "[ÁÀÂÄáàâä]", "A")
    c = F.regexp_replace(c, "[ÉÈÊËéèêë]", "E")
    c = F.regexp_replace(c, "[ÍÌÎÏíìîï]", "I")
    c = F.regexp_replace(c, "[ÓÒÔÖóòôö]", "O")
    c = F.regexp_replace(c, "[ÚÙÛÜúùûü]", "U")
    return c

def norm_txt(col):
    """
    Normalización 'robusta':
    - upper
    - quitar tildes
    - normalizar espacios
    - dejar letras/números/espacios (reduce ruido de caracteres especiales)
    """
    c = col.cast("string")
    c = F.upper(c)
    c = quitar_tildes(c)
    c = F.regexp_replace(c, u"\u00A0", " ")
    c = F.regexp_replace(c, u"\u202F", " ")
    c = F.regexp_replace(c, r"\s+", " ")
    c = F.trim(c)
    c = F.regexp_replace(c, r"[^A-Z0-9 ]", " ")
    c = F.regexp_replace(c, r"\s+", " ")
    c = F.trim(c)
    return c

def norm_col(df, cols):
    for c in cols:
        df = df.withColumn(c, norm_txt(F.col(c)))
    return df



def parse_created_to_utc_ts(created_col):
   """
   PowerApps 'Created' a veces viene como:
   - ISO: 2024-01-31T15:20:00Z / 2024-01-31T15:20:00.000Z
   - o con offset: 2024-01-31T15:20:00-05:00
   - o dd/MM/yyyy hh:mm a (menos común, pero pasa)
   Probamos varios parses y hacemos coalesce.
   """
   s = created_col.cast("string")
   # ISO
   ts_iso = F.to_timestamp(s)
   # ISO con Z explícita y milisegundos (por si acaso)
   ts_iso_z_ms = F.to_timestamp(s, "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
   ts_iso_z = F.to_timestamp(s, "yyyy-MM-dd'T'HH:mm:ss'Z'")
   # Formato tipo "31/01/2024 03:25 p. m."
   s2 = F.lower(F.trim(s))
   s2 = F.regexp_replace(s2, u"\u00A0", " ")
   s2 = F.regexp_replace(s2, u"\u202F", " ")
   s2 = F.regexp_replace(s2, r"\s+", " ")
   s2 = F.regexp_replace(s2, r"(?i)a\W*m\W*", "AM")
   s2 = F.regexp_replace(s2, r"(?i)p\W*m\W*", "PM")
   ts_es = F.to_timestamp(s2, "dd/MM/yyyy hh:mm a")
   return F.coalesce(ts_iso, ts_iso_z_ms, ts_iso_z, ts_es)



def parse_fecasignacion(col):
   """
   FechaAsignacion suele venir como dd/MM/yyyy o dd/MM/yyyy hh:mm a
   Intentamos ambos y nos quedamos con timestamp.
   """
   s = col.cast("string")
   s = F.regexp_replace(s, u"\u00A0", " ")
   s = F.regexp_replace(s, u"\u202F", " ")
   s = F.trim(s)
   # intenta timestamp con hora
   s_l = F.lower(s)
   s_l = F.regexp_replace(s_l, r"\s+", " ")
   s_l = F.regexp_replace(s_l, r"(?i)a\W*m\W*", "AM")
   s_l = F.regexp_replace(s_l, r"(?i)p\W*m\W*", "PM")
   ts_dt = F.to_timestamp(s_l, "dd/MM/yyyy hh:mm a")
   ts_d  = F.to_timestamp(s,   "dd/MM/yyyy")  # midnight
   return F.coalesce(ts_dt, ts_d)


def load_powerapps(spark, path_apps):
   """
   Opción B: control total de TZ
   1) Forzamos la sesión a UTC para parsear CREATED.
   2) Convertimos explícitamente a America/Lima.
   3) Derivamos FECCREACION/HORACREACION/FECHORACREACION (floor a minuto).
   4) Ordenamos y deduplicamos 1 fila por CODSOLICITUD (última foto).
   """
   # 1) Parse seguro en UTC
   spark.conf.set("spark.sql.session.timeZone", "UTC")
   df = (
       spark.read.format("csv")
       .option("header", "true")
       .option("sep", ",")
       .option("encoding", "utf-8")
       .option("ignoreLeadingWhiteSpace", "true")
       .option("ignoreTrailingWhiteSpace", "true")
       .load(path_apps)
       .select(
           F.col("Title").alias("CODSOLICITUD"),
           F.col("FechaAsignacion").alias("FECASIGNACION"),
           F.col("Tipo de Producto").alias("PRODUCTO"),
           F.col("ResultadoAnalista").alias("RESULTADOANALISTA"),
           F.col("Mail").alias("CORREO"),
           F.col("Motivo Resultado Analista").alias("MOTIVORESULTADOANALISTA"),
           F.col("AñoMes").alias("CODMES"),
           F.col("Created").alias("CREATED"),
           F.col("Motivo_MD").alias("MOTIVOMALADERIVACION"),
           F.col("Submotivo_MD").alias("SUBMOTIVOMALADERIVACION"),
       )
   )
   # limpieza mínima
   df = df.withColumn("CODSOLICITUD", F.trim(F.col("CODSOLICITUD").cast("string")))
   # parse fecha asignación para ordenar (sin tocar tu columna original)
   df = df.withColumn("FECASIGNACION_TS", parse_fecasignacion(F.col("FECASIGNACION")))
   # 2) CREATED: parse en UTC + conversión a Lima explícita
   # to_timestamp con ISO 8601 y 'Z' funciona bien en Databricks si la sesión está en UTC.
   df = df.withColumn("CREATED_TS_UTC", F.to_timestamp(F.col("CREATED")))
   # Convertimos a hora Lima (UTC-5) para replicar pandas tz_convert("America/Lima")
   df = df.withColumn("CREATED_LIMA", F.from_utc_timestamp(F.col("CREATED_TS_UTC"), "America/Lima"))
   # 3) Derivados (floor/min) como pandas dt.floor("min")
   df = (
       df.withColumn("FECHORACREACION", F.date_trunc("minute", F.col("CREATED_LIMA")))
         .withColumn("FECCREACION", F.to_date("FECHORACREACION"))
         .withColumn("HORACREACION", F.date_format("FECHORACREACION", "HH:mm:ss"))
   )
   # normalización texto (como tu loop pandas)
   df = norm_col(df, [
       "PRODUCTO", "RESULTADOANALISTA", "MOTIVORESULTADOANALISTA",
       "MOTIVOMALADERIVACION", "SUBMOTIVOMALADERIVACION"
   ])
   # replace RESULTADOANALISTA
   df = df.withColumn(
       "RESULTADOANALISTA",
       F.when(F.col("RESULTADOANALISTA") == "DENEGADO POR ANALISTA DE CREDITO", F.lit("DENEGADO"))
        .when(F.col("RESULTADOANALISTA") == "APROBADO POR ANALISTA DE CREDITO", F.lit("APROBADO"))
        .when(F.col("RESULTADOANALISTA") == "DEVOLVER AL GESTOR", F.lit("DEVUELTO AL GESTOR"))
        .otherwise(F.col("RESULTADOANALISTA"))
   )
   # 4) sort desc + drop_duplicates keep=first (última foto)
   w = Window.partitionBy("CODSOLICITUD").orderBy(
       F.col("FECHORACREACION").desc_nulls_last(),
       F.col("FECASIGNACION_TS").desc_nulls_last()
   )
   df_clean = (
       df.withColumn("rn", F.row_number().over(w))
         .filter(F.col("rn") == 1)
         .drop("rn")
   )
   # salida final (similar a tp_powerapp_clean)
   df_clean = df_clean.select(
       "CODSOLICITUD",
       "FECASIGNACION",
       "PRODUCTO",
       "RESULTADOANALISTA",
       "CORREO",
       "MOTIVORESULTADOANALISTA",
       "CODMES",
       "MOTIVOMALADERIVACION",
       "SUBMOTIVOMALADERIVACION",
       "FECCREACION",
       "HORACREACION",
       "FECHORACREACION"
   )
   return df_clean




Ahora voy a incluir PATH_ORGANICO = "abfss://bcp-edv-rbmbdn@adlscu1lhclbackp05.dfs.core.windows.net/T72496/EXPORTS/ORGANICO/ORGANICO.csv"
El cual tiene estos campos

def load_organico(spark, path_apps):

   df = (
       spark.read.format("csv")
       .option("header", "true")
       .option("sep", ",")
       .option("encoding", "utf-8")
       .option("ignoreLeadingWhiteSpace", "true")
       .option("ignoreTrailingWhiteSpace", "true")
       .load(path_apps)
       .select(
           F.col("MATORGANICO"),
           F.col("CODMES"),
           F.col("MATSUPERIOR"),
           F.col("CORREO"),
           F.col("NBRCORTO"),
       )
   )

   return df

Y por el medio del correo en ambos df, quiero obtener el MATORGANICO y pegarlo en el df de power app usa CODMES en ambos para hacer el match preciso

df_apps = load_powerapps(spark, PATH_PA_SOLICITUDES)
df_organico = load_organico(spark, PATH_ORGANICO)
