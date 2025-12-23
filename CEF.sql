from pyspark.sql import functions as F
from pyspark.sql.window import Window
import re
from functools import reduce

# ---------- PATHS ----------
BASE_DIR_ORGANICO = "abfss://bcp-edv-rbmbdn@adlscu1lhclbackp05.dfs.core.windows.net/T72496/CARGA/ORGANICO/1n_Activos_*.csv"
PATH_PA_SOLICITUDES = "abfss://bcp-edv-rbmbdn@adlscu1lhclbackp05.dfs.core.windows.net/T72496/CARGA/POWERAPPS/BASESOLICITUDES/POWERAPP_EDV.csv"
PATH_SF_ESTADOS = "abfss://bcp-edv-rbmbdn@adlscu1lhclbackp05.dfs.core.windows.net/T72496/CARGA/SALESFORCE/INFORME_ESTADO/INFORME_ESTADO_*.csv"
PATH_SF_PRODUCTOS = "abfss://bcp-edv-rbmbdn@adlscu1lhclbackp05.dfs.core.windows.net/T72496/CARGA/SALESFORCE/INFORME_PRODUCTO/INFORME_PRODUCTO_*.csv"

# ---------- NORMALIZACION ----------
def quitar_tildes(col):
   c = F.regexp_replace(col, "[ÁÀÂÄáàâä]", "A")
   c = F.regexp_replace(c, "[ÉÈÊËéèêë]", "E")
   c = F.regexp_replace(c, "[ÍÌÎÏíìîï]", "I")
   c = F.regexp_replace(c, "[ÓÒÔÖóòôö]", "O")
   c = F.regexp_replace(c, "[ÚÙÛÜúùûü]", "U")
   return c
def norm_txt(col):
   return F.trim(quitar_tildes(F.upper(col)))
def norm_col(df, cols):
   for c in cols:
       df = df.withColumn(c, norm_txt(F.col(c)))
   return df
def limpiar_cesado(col):
   return F.trim(F.regexp_replace(col, r'\(CESADO\)', ''))
# ---------- FECHA/HORA ESP ----------
def parse_fecha_hora_esp(col):
   s = col.cast("string")
   s = F.regexp_replace(s, u'\u00A0', ' ')
   s = F.regexp_replace(s, u'\u202F', ' ')
   s = F.lower(F.trim(s))
   s = F.regexp_replace(s, r'\s+', ' ')
   s = F.regexp_replace(s, r'(?i)a\W*m\W*', 'AM')
   s = F.regexp_replace(s, r'(?i)p\W*m\W*', 'PM')
   return F.to_timestamp(s, 'dd/MM/yyyy hh:mm a')
# ---------- MATCH POR TOKENS ----------
def build_org_tokens(df_org):
   df_org = (
       df_org
       .withColumn("TOKENS_NOMBRE_ORG", F.array_distinct(F.split(F.col("NOMBRECOMPLETO_CLEN"), r"\s+")))
       .withColumn("N_TOK_ORG", F.size("TOKENS_NOMBRE_ORG"))
   )
   return (
       df_org
       .select("CODMES","MATORGANICO","MATSUPERIOR","NOMBRECOMPLETO","TOKENS_NOMBRE_ORG","N_TOK_ORG")
       .withColumn("TOKEN", F.explode("TOKENS_NOMBRE_ORG"))
   )
def match_persona_vs_organico(df_org_tokens, df_sf, codmes_sf_col, codsol_col, nombre_sf_col,
                            min_tokens=3, min_ratio_sf=0.60):
   df_sf_nombres = (
       df_sf
       .withColumn("TOKENS_NOMBRE_SF", F.array_distinct(F.split(F.col(nombre_sf_col), r"\s+")))
       .withColumn("N_TOK_SF", F.size("TOKENS_NOMBRE_SF"))
   )
   df_sf_tokens = (
       df_sf_nombres
       .select(codmes_sf_col, codsol_col, nombre_sf_col, "TOKENS_NOMBRE_SF", "N_TOK_SF")
       .withColumn("TOKEN", F.explode("TOKENS_NOMBRE_SF"))
   )
   df_join = (
       df_sf_tokens.alias("sf")
       .join(
           df_org_tokens.alias("org"),
           (F.col(f"sf.{codmes_sf_col}") == F.col("org.CODMES")) &
           (F.col("sf.TOKEN") == F.col("org.TOKEN")),
           "inner"
       )
   )
   match_cols = [codmes_sf_col, codsol_col, nombre_sf_col, "MATORGANICO", "MATSUPERIOR"]
   df_scores = (
       df_join
       .groupBy(match_cols)
       .agg(
           F.countDistinct("sf.TOKEN").alias("TOKENS_MATCH"),
           F.first("sf.N_TOK_SF").alias("N_TOK_SF"),
           F.first("org.N_TOK_ORG").alias("N_TOK_ORG"),
       )
       .withColumn("RATIO_SF", F.col("TOKENS_MATCH") / F.col("N_TOK_SF"))
       .withColumn("RATIO_ORG", F.col("TOKENS_MATCH") / F.col("N_TOK_ORG"))
       .filter((F.col("TOKENS_MATCH") >= min_tokens) & (F.col("RATIO_SF") >= min_ratio_sf))
   )
   w = Window.partitionBy(codmes_sf_col, codsol_col).orderBy(
       F.col("TOKENS_MATCH").desc(),
       F.col("RATIO_SF").desc(),
       F.col("RATIO_ORG").desc(),
       F.col("MATORGANICO").asc()
   )
   return (
       df_scores
       .withColumn("rn", F.row_number().over(w))
       .filter(F.col("rn") == 1)
       .drop("rn")
   )




def load_organico(spark, path_organico):
   df_raw = (
       spark.read.format("csv")
       .option("header", "true")
       .option("sep", ";")
       .option("encoding", "ISO-8859-1")
       .option("ignoreLeadingWhiteSpace", "true")
       .option("ignoreTrailingWhiteSpace", "true")
       .load(path_organico)
   )
   df = (
       df_raw.select(
           F.col("CODMES").cast("string").alias("CODMES"),
           F.col("Matrícula").alias("MATORGANICO"),
           F.col("Nombre Completo").alias("NOMBRECOMPLETO"),
           F.col("Correo electronico").alias("CORREO"),
           F.col("Fecha Ingreso").alias("FECINGRESO"),
           F.col("Matrícula Superior").alias("MATSUPERIOR"),
       )
   )
   df = norm_col(df, ["MATORGANICO","NOMBRECOMPLETO","MATSUPERIOR"])
   # regla de formato matrícula superior (tu regex)
   df = df.withColumn("MATSUPERIOR", F.regexp_replace("MATSUPERIOR", r'^0(?=[A-Z]\d{5})', ''))
   df = (
       df.withColumn("FECINGRESO", F.to_date("FECINGRESO"))
         .withColumn("NOMBRECOMPLETO_CLEAN", limpiar_cesado(F.col("NOMBRECOMPLETO")))
   )
   return df




def load_sf_estados(spark, path_estados):
   df_raw = (
       spark.read.format("csv")
       .option("header", "true")
       .option("encoding", "ISO-8859-1")
       .load(path_estados)
   )
   df = df_raw.select(
       F.col("Fecha de inicio del paso").alias("FECINICIOEVALUACION"),
       F.col("Fecha de finalización del paso").alias("FECFINEVALUACION"),
       F.col("Nombre del registro").alias("CODSOLICITUD"),
       F.col("Estado").alias("ESTADOSOLICITUD"),
       F.col("Paso: Nombre").alias("NBRPASO"),
       F.col("Último actor: Nombre completo").alias("NBRULTACTOR"),
       F.col("Último actor del paso: Nombre completo").alias("NBRULTACTORPASO"),
       F.col("Proceso de aprobación: Nombre").alias("PROCESO"),
       F.col("Estado del paso").alias("ESTADOSOLICITUDPASO")
   )
   df = norm_col(df, ["ESTADOSOLICITUD","ESTADOSOLICITUDPASO","NBRPASO","NBRULTACTOR","NBRULTACTORPASO","PROCESO"])
   df = (
       df.withColumn("FECHORINICIOEVALUACION", parse_fecha_hora_esp(F.col("FECINICIOEVALUACION")))
         .withColumn("FECHORFINEVALUACION", parse_fecha_hora_esp(F.col("FECFINEVALUACION")))
         .withColumn("FECINICIOEVALUACION", F.to_date("FECHORINICIOEVALUACION"))
         .withColumn("FECFINEVALUACION", F.to_date("FECHORFINEVALUACION"))
         .withColumn("HORINICIOEVALUACION", F.date_format("FECHORINICIOEVALUACION", "HH:mm:ss"))
         .withColumn("HORFINEVALUACION", F.date_format("FECHORFINEVALUACION", "HH:mm:ss"))
         .withColumn("CODMESEVALUACION", F.date_format("FECINICIOEVALUACION", "yyyyMM").cast("string"))
   )
   # (opcional) normaliza CODSOLICITUD a string/trim por seguridad
   df = df.withColumn("CODSOLICITUD", F.trim(F.col("CODSOLICITUD").cast("string")))
   return df




def load_sf_productos_validos(spark, path_productos):
   df_raw = (
       spark.read.format("csv")
       .option("header", "true")
       .option("encoding", "ISO-8859-1")
       .load(path_productos)
   )
   df = df_raw.select(
       F.col("Nombre de la oportunidad").alias("CODSOLICITUD"),
       F.col("Nombre del Producto").alias("NBRPRODUCTO"),
       F.col("Etapa").alias("ETAPA"),
       F.col("Analista").alias("NBRANALISTA"),
       F.col("Tipo de Acción").alias("TIPACCION"),
       F.col("Fecha de creación").alias("FECCREACION"),
       F.col("Divisa de la oportunidad").alias("NBRDIVISA"),
       F.col("Monto/Línea Solicitud").alias("MTOSOLICITADO"),
       F.col("Monto/Línea aprobada").alias("MTOAPROBADO"),
       F.col("Monto Solicitado/Ofertado").alias("MTOOFERTADO"),
       F.col("Monto desembolsado").alias("MTODESEMBOLSADO"),
       F.col("Centralizado/Punto de Contacto").alias("CENTROATENCION")
   )
   df = df.withColumn("CODSOLICITUD", F.trim(F.col("CODSOLICITUD").cast("string")))
   df = df.withColumn(
       "FLG_CODSOLICITUD_VALIDO",
       F.when((F.length("CODSOLICITUD") == 11) & (F.col("CODSOLICITUD").startswith("O00")), 1).otherwise(0)
   )
   df = df.filter(F.col("FLG_CODSOLICITUD_VALIDO") == 1).drop("FLG_CODSOLICITUD_VALIDO")
   df = norm_col(df, ["NBRPRODUCTO","ETAPA","NBRANALISTA","TIPACCION","NBRDIVISA","CENTROATENCION"])
   df = (
       df.withColumn("FECCREACION_STR", F.trim(F.col("FECCREACION").cast("string")))
         .withColumn(
             "FECCREACION_DATE",
             F.coalesce(
                 F.to_date("FECCREACION_STR", "dd/MM/yyyy"),
                 F.to_date("FECCREACION_STR", "yyyy-MM-dd"),
                 F.to_date("FECCREACION_STR")
             )
         )
         .withColumn("FECCREACION", F.col("FECCREACION_DATE"))
         .withColumn("CODMESCREACION", F.date_format("FECCREACION", "yyyyMM").cast("string"))
         .drop("FECCREACION_STR","FECCREACION_DATE")
   )
   return df




def load_powerapps(spark, path_apps):
   df = (
       spark.read.format("csv")
       .option("header", "true")
       .option("sep", ";")
       .option("encoding", "utf-8")
       .option("ignoreLeadingWhiteSpace", "true")
       .option("ignoreTrailingWhiteSpace", "true")
       .load(path_apps)
       .select(
           F.col("CODMES").cast("string").alias("CODMES_APPS"),
           "CODSOLICITUD",
           "FECHORACREACION",
           "MATANALISTA",
           "PRODUCTO",
           "RESULTADOANALISTA",
           "MOTIVORESULTADOANALISTA",
           "MOTIVOMALADERIVACION",
           "SUBMOTIVOMALADERIVACION"
       )
   )
   df = df.withColumn("CODSOLICITUD", F.trim(F.col("CODSOLICITUD").cast("string")))
   w = Window.partitionBy("CODSOLICITUD").orderBy(F.col("FECHORACREACION").desc_nulls_last())
   df = df.withColumn("rn", F.row_number().over(w)).filter("rn=1").drop("rn")
   return df





def enrich_estados_con_organico(df_estados, df_org_tokens):
   bm_actor = match_persona_vs_organico(
       df_org_tokens=df_org_tokens,
       df_sf=df_estados,
       codmes_sf_col="CODMESEVALUACION",
       codsol_col="CODSOLICITUD",
       nombre_sf_col="NBRULTACTOR",
       min_tokens=3,
       min_ratio_sf=0.60
   )
   bm_paso = match_persona_vs_organico(
       df_org_tokens=df_org_tokens,
       df_sf=df_estados,
       codmes_sf_col="CODMESEVALUACION",
       codsol_col="CODSOLICITUD",
       nombre_sf_col="NBRULTACTORPASO",
       min_tokens=3,
       min_ratio_sf=0.60
   )
   df_enriq = (
       df_estados
       .join(
           bm_actor.select("CODMESEVALUACION","CODSOLICITUD","MATORGANICO","MATSUPERIOR"),
           ["CODMESEVALUACION","CODSOLICITUD"],
           "left"
       )
       .join(
           bm_paso.select(
               "CODMESEVALUACION","CODSOLICITUD",
               F.col("MATORGANICO").alias("MATORGANICOPASO"),
               F.col("MATSUPERIOR").alias("MATSUPERIORPASO"),
           ),
           ["CODMESEVALUACION","CODSOLICITUD"],
           "left"
       )
   )
   return df_enriq




def enrich_productos_con_organico(df_productos, df_org_tokens):
   bm_analista = match_persona_vs_organico(
       df_org_tokens=df_org_tokens,
       df_sf=df_productos,
       codmes_sf_col="CODMESCREACION",
       codsol_col="CODSOLICITUD",
       nombre_sf_col="NBRANALISTA",
       min_tokens=3,
       min_ratio_sf=0.60
   )
   df_enriq = (
       df_productos
       .join(
           bm_analista.select(
               "CODMESCREACION","CODSOLICITUD",
               F.col("MATORGANICO").alias("MATORGANICO_ANALISTA"),
               F.col("MATSUPERIOR").alias("MATSUPERIOR_ANALISTA")
           ),
           ["CODMESCREACION","CODSOLICITUD"],
           "left"
       )
   )
   return df_enriq



# 1) STAGING
df_org = load_organico(spark, BASE_DIR_ORGANICO)
df_org_tokens = build_org_tokens(df_org)
df_estados = load_sf_estados(spark, PATH_SF_ESTADOS)
df_productos = load_sf_productos_validos(spark, PATH_SF_PRODUCTOS)
df_apps = load_powerapps(spark, PATH_PA_SOLICITUDES)
# 2) ENRIQUECIMIENTO CON ORGANICO
df_estados_enriq = enrich_estados_con_organico(df_estados, df_org_tokens)
df_productos_enriq = enrich_productos_con_organico(df_productos, df_org_tokens)
