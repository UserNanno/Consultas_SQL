from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import DecimalType

PATH_PA_SOLICITUDES = "abfss://bcp-edv-rbmbdn@adlscu1lhclbackp05.dfs.core.windows.net/T72496/CARGA/POWERAPPS/1n_Apps_*.csv"
PATH_ORGANICO = "abfss://bcp-edv-rbmbdn@adlscu1lhclbackp05.dfs.core.windows.net/T72496/EXPORTS/ORGANICO/ORGANICO.csv"

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
    Normalización:
    - upper
    - quitar tildes de vocales (no toca Ñ)
    - normalizar espacios
    - dejar letras/números/espacios
    """
    c = col.cast("string")
    c = F.upper(c)
    c = quitar_tildes(c)

    c = F.regexp_replace(c, u"\u00A0", " ")
    c = F.regexp_replace(c, u"\u202F", " ")

    c = F.regexp_replace(c, r"\s+", " ")
    c = F.trim(c)

    # Mantener A-Z, Ñ, dígitos y espacio
    c = F.regexp_replace(c, r"[^A-ZÑ0-9 ]", " ")

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



def norm_email(col):
   c = col.cast("string")
   c = F.regexp_replace(c, u"\u00A0", " ")
   c = F.regexp_replace(c, u"\u202F", " ")
   c = F.lower(F.trim(c))
   # en emails, mejor eliminar espacios internos por si vienen "a b@c.com"
   c = F.regexp_replace(c, r"\s+", "")
   return c



def norm_codmes(col):
   s = col.cast("string")
   s = F.regexp_replace(s, r"[^0-9]", "")
   s = F.when(F.length(s) >= 6, F.substring(s, 1, 6)).otherwise(s)
   return s



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
   df = df.withColumn("FECASIGNACION_TS", parse_fecasignacion(F.col("FECASIGNACION")))
   df = df.withColumn("CREATED_TS_UTC", F.to_timestamp(F.col("CREATED")))
   df = df.withColumn("CREATED_LIMA", F.from_utc_timestamp(F.col("CREATED_TS_UTC"), "America/Lima"))

   df = (
       df.withColumn("FECHORACREACION", F.date_trunc("minute", F.col("CREATED_LIMA")))
         .withColumn("FECCREACION", F.to_date("FECHORACREACION"))
         .withColumn("HORACREACION", F.date_format("FECHORACREACION", "HH:mm:ss"))
   )
   # normalización texto
   df = norm_col(df, [
       "PRODUCTO", "RESULTADOANALISTA", "MOTIVORESULTADOANALISTA",
       "MOTIVOMALADERIVACION", "SUBMOTIVOMALADERIVACION"
   ])
   # RESULTADOANALISTA
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


def load_organico(spark, path_organico_csv):
   df = (
       spark.read.format("csv")
       .option("header", "true")
       .option("sep", ";")
       .option("encoding", "utf-8")  # si notas caracteres raros, cambia a ISO-8859-1
       .option("ignoreLeadingWhiteSpace", "true")
       .option("ignoreTrailingWhiteSpace", "true")
       .load(path_organico_csv)
   )

   cols = df.columns
   pick = [c for c in ["CODMES","CORREO","MATORGANICO","MATSUPERIOR","NBRCORTO","TOKENS_MATCH","JACCARD"] if c in cols]
   df = df.select(*[F.col(c) for c in pick])

   # Normaliza llaves
   df = (
       df.withColumn("CODMES", norm_codmes(F.col("CODMES")))
         .withColumn("CORREO", norm_email(F.col("CORREO")))
   )

   # Dedup por (CODMES, CORREO) priorizando mejor calidad de match
   # - JACCARD desc (mejor)
   # - TOKENS_MATCH desc
   # - MATORGANICO no nulo
   order_exprs = []
   if "JACCARD" in df.columns:
       order_exprs.append(F.col("JACCARD").cast("double").desc_nulls_last())
   if "TOKENS_MATCH" in df.columns:
       order_exprs.append(F.col("TOKENS_MATCH").cast("int").desc_nulls_last())
   order_exprs.append(F.col("MATORGANICO").isNotNull().desc())
   w = Window.partitionBy("CODMES", "CORREO").orderBy(*order_exprs)
   df = (
       df.withColumn("rn", F.row_number().over(w))
         .filter(F.col("rn") == 1)
         .drop("rn")
   )
   return df


df_apps = load_powerapps(spark, PATH_PA_SOLICITUDES)
df_org = load_organico(spark, PATH_ORGANICO)


# Normaliza llaves en Apps (si tu CORREO ya viene ok, igual lo hacemos por seguridad)
df_apps_key = (
   df_apps
   .withColumn("CODMES", norm_codmes(F.col("CODMES")))
   .withColumn("CORREO", norm_email(F.col("CORREO")))
)



df_apps_key = (
   df_apps
   .withColumn("CODMES", norm_codmes(F.col("CODMES")))
   .withColumn("CORREO", norm_email(F.col("CORREO")))
)


# Join preciso (CODMES, CORREO)
df_apps_enriq = (
   df_apps_key.alias("a")
   .join(
       df_org.alias("o"),
       on=[F.col("a.CODMES") == F.col("o.CODMES"), F.col("a.CORREO") == F.col("o.CORREO")],
       how="left"
   )
   .drop(F.col("o.CODMES"))
   .drop(F.col("o.CORREO"))
)


df_apps_enriq = df_apps_enriq.drop("NBRCORTO", "TOKENS_MATCH", "JACCARD")


final_dir = "abfss://bcp-edv-rbmbdn@adlscu1lhclbackp05.dfs.core.windows.net/T72496/EXPORTS/POWERAPPS/"
tmp_dir   = "abfss://bcp-edv-rbmbdn@adlscu1lhclbackp05.dfs.core.windows.net/T72496/EXPORTS/POWERAPPS/.tmp_export/"

(df_apps_enriq
    .coalesce(1)
    .write
    .mode("overwrite")
    .option("header", "true")
    .option("delimiter", ";")
    .option("quote", '"')
    .option("escape", '"')
    .option("nullValue", "")
    .csv(tmp_dir)
)


# Encuentra el part-*.csv
part_files = [f.path for f in dbutils.fs.ls(tmp_dir) if f.name.startswith("part-") and f.name.endswith(".csv")]
assert len(part_files) == 1, f"Esperaba 1 part-*.csv, encontré {len(part_files)}"
part_file = part_files[0]

dbutils.fs.mkdirs(final_dir)
for f in dbutils.fs.ls(final_dir):
    if f.name.endswith(".csv"):
        dbutils.fs.rm(f.path, True)

custom_name = "POWERAPP_EDV.csv"
dest_file = f"{final_dir}{custom_name}"

dbutils.fs.mv(part_file, dest_file)
dbutils.fs.rm(tmp_dir, True)
