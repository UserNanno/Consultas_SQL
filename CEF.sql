from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DecimalType, DateType
)
from pyspark.sql.functions import col, upper, lpad, to_date
from pyspark.sql import functions as F
from pyspark.sql.window import Window

def quitar_tildes(col):
    return (F.regexp_replace(F.regexp_replace(F.regexp_replace(F.regexp_replace(F.regexp_replace(col,
        "[ÁÀÂÄáàâä]", "A"),"[ÉÈÊËéèêë]", "E"),"[ÍÌÎÏíìîï]", "I"),"[ÓÒÔÖóòôö]", "O"),"[ÚÙÛÜúùûü]", "U"))

-- ORGANICO


import re
from functools import reduce
from pyspark.sql import DataFrame

base_dir = "abfss://bcp-edv-rbmbdn@adlscu1lhclbackp05.dfs.core.windows.net/T72496/CARGA/ORGANICO/"
entries = dbutils.fs.ls(base_dir)
files = [e.path for e in entries if e.name.startswith("1n_Activos_2025") and e.name.endswith(".xlsx")]
assert files, "No se encontraron archivos 1n_Activos_2025*.xlsx"

excel_options = {
    "header": "true",
    "inferSchema": "true",
    "treatEmptyValuesAsNulls": "true",
    "timestampFormat": "yyyy-MM-dd HH:mm:ss",
    # "dataAddress": "'Hoja1'!A1",  # Si el header no está en la fila 1
}

dfs = []
for path in files:
    reader = spark.read.format("com.crealytics.spark.excel")
    for k, v in excel_options.items():
        reader = reader.option(k, v)
    df = reader.load(path)

    for c in df.columns:
        new_name = re.sub(r"\s+", " ", c.strip())
        if new_name != c:
            df = df.withColumnRenamed(c, new_name)

    # Elimina columnas sin nombre (_c0, _c1, ..., _c29)
    cols_clean = [c for c in df.columns if not re.match(r"^_c\d+$", c)]
    df = df.select(*cols_clean)

    dfs.append(df)

from pyspark.sql import functions as F
df_organico_raw = reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), dfs)


df_organico = (
    df_organico_raw
        .select(
            F.col("CODMES").alias("CODMES"),
            F.col("Matrícula").alias("MATORGANICO"),
            F.col("Nombre Completo").alias("NOMBRECOMPLETO"),
            F.col("Correo electronico").alias("CORREO"),
            F.col("Fecha Ingreso").alias("FECINGRESO"),
            F.col("Matrícula Superior").alias("MATSUPERIOR"),
        )
)


def norm_txt_spark(col):
    # upper + quitar tildes + trim
    return F.trim(quitar_tildes(F.upper(F.col(col))))



cols_norm = ["MATORGANICO", "NOMBRECOMPLETO", "MATSUPERIOR"]

for c in cols_norm:
    df_organico = df_organico.withColumn(c, norm_txt_spark(c))



df_organico = df_organico.withColumn(
    "MATSUPERIOR",
    F.regexp_replace(F.col("MATSUPERIOR"), r'^0(?=[A-Z]\d{5})', '')
)

df_organico = df_organico.withColumn(
    "FECINGRESO",
    F.to_date("FECINGRESO")
)

def limpiar_cesado(col):
    return F.trim(F.regexp_replace(F.col(col), r'\(CESADO\)', ''))

df_organico = df_organico.withColumn(
    "NOMBRECOMPLETO_CLEAN",
    limpiar_cesado("NOMBRECOMPLETO")
)


-- salesforce - estados

path_salesfoce_estados = "abfss://bcp-edv-rbmbdn@adlscu1lhclbackp05.dfs.core.windows.net/T72496/CARGA/SALESFORCE/INFORME_ESTADO/INFORME_ESTADO_*.csv"

df_salesforce_raw = (
    spark.read
        .format("csv")
        .option("header", "true")
        .option("encoding", "ISO-8859-1")
        .load(path_salesfoce_estados)
)

df_salesforce_estados = (
    df_salesforce_raw
        .select(
            F.col("Fecha de inicio del paso").alias("FECINICIOEVALUACION"),
            F.col("Fecha de finalización del paso").alias("FECFINEVALUACION"),
            F.col("Nombre del registro").alias("CODSOLICITUD"),
            F.col("Estado").alias("ESTADOSOLICITUD"),
            F.col("Paso: Nombre").alias("NBRPASO"),
            F.col("Último actor: Nombre completo").alias("NBRULTACTOR"),
            F.col("Último actor del paso: Nombre completo").alias("NBRULTACTORPASO"),
            F.col("Proceso de aprobación: Nombre").alias("PROCESO"),
        )
)

df_salesforce_estados = (
    df_salesforce_estados
        .withColumn("ESTADOSOLICITUD", F.upper(F.col("ESTADOSOLICITUD")))
        .withColumn("NBRPASO", F.upper(F.col("NBRPASO")))
        .withColumn("NBRULTACTOR", F.upper(F.col("NBRULTACTOR")))
        .withColumn("NBRULTACTORPASO", F.upper(F.col("NBRULTACTORPASO")))
        .withColumn("PROCESO", F.upper(F.col("PROCESO")))
)

df_salesforce_estados = (
    df_salesforce_estados
        .withColumn("NBRPASO", quitar_tildes("NBRPASO"))
        .withColumn("NBRULTACTOR", quitar_tildes("NBRULTACTOR"))
        .withColumn("NBRULTACTORPASO", quitar_tildes("NBRULTACTORPASO"))
        .withColumn("PROCESO", quitar_tildes("PROCESO"))
)

def parse_fecha_hora_esp_col(col):
    s = F.col(col).cast("string")
    s = F.regexp_replace(s, u'\u00A0', ' ')
    s = F.regexp_replace(s, u'\u202F', ' ')
    s = F.lower(F.trim(s))
    
    s = F.regexp_replace(s, r'\s+', ' ')
    
    # 2) Normalizar "a. m." / "p. m." -> AM / PM
    s = F.regexp_replace(s, r'(?i)a\W*m\W*', 'AM')
    s = F.regexp_replace(s, r'(?i)p\W*m\W*', 'PM')
    
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

df_salesforce_estados = (
    df_salesforce_estados
        .withColumn("FECINICIOEVALUACION", F.to_date("FECHORINICIOEVALUACION"))
        .withColumn("FECFINEVALUACION", F.to_date("FECHORFINEVALUACION"))

        .withColumn("HORINICIOEVALUACION", F.date_format("FECHORINICIOEVALUACION", "HH:mm:ss"))
        .withColumn("HORFINEVALUACION", F.date_format("FECHORFINEVALUACION", "HH:mm:ss"))

        .withColumn("CODMESEVALUACION",
                    F.date_format("FECINICIOEVALUACION", "yyyyMM"))
)

count_sf_original = df_salesforce_estados.count()
print("Registros originales SF:", count_sf_original)



from pyspark.sql import functions as F
from pyspark.sql.window import Window

df_organico = df_organico.withColumn("CODMES", F.col("CODMES").cast("string"))
df_salesforce_estados = df_salesforce_estados.withColumn("CODMESEVALUACION", F.col("CODMESEVALUACION").cast("string"))

df_org_nombres = (
    df_organico
      .withColumn(
          "TOKENS_NOMBRE_ORG",
          F.array_distinct(F.split(F.col("NOMBRECOMPLETO"), r"\s+"))
      )
      .withColumn("N_TOK_ORG", F.size("TOKENS_NOMBRE_ORG"))
)

COL_NOMBRE_SF = "NBRULTACTOR"

df_sf_nombres = (
    df_salesforce_estados
      .withColumn(
          "TOKENS_NOMBRE_SF",
          F.array_distinct(F.split(F.col(COL_NOMBRE_SF), r"\s+"))
      )
      .withColumn("N_TOK_SF", F.size("TOKENS_NOMBRE_SF"))
)

# Explode de tokens (una fila por token)
df_org_tokens = (
    df_org_nombres
      .select(
          "CODMES",
          "MATORGANICO",
          "MATSUPERIOR",
          "NOMBRECOMPLETO",
          "TOKENS_NOMBRE_ORG",
          "N_TOK_ORG"
      )
      .withColumn("TOKEN", F.explode("TOKENS_NOMBRE_ORG"))
)

df_sf_tokens = (
    df_sf_nombres
      .select(
          "CODMESEVALUACION",
          "CODSOLICITUD",
          COL_NOMBRE_SF,
          "TOKENS_NOMBRE_SF",
          "N_TOK_SF"
      )
      .withColumn("TOKEN", F.explode("TOKENS_NOMBRE_SF"))
)

df_join_tokens = (
    df_sf_tokens.alias("sf")
      .join(
          df_org_tokens.alias("org"),
          (F.col("sf.CODMESEVALUACION") == F.col("org.CODMES")) &
          (F.col("sf.TOKEN") == F.col("org.TOKEN")),
          "inner"
      )
)

# Score de similitud por solicitud + matrícula

match_cols = ["CODMESEVALUACION", "CODSOLICITUD", COL_NOMBRE_SF, "MATORGANICO", "MATSUPERIOR"]

df_match_scores = (
    df_join_tokens
      .groupBy(match_cols)
      .agg(
          F.countDistinct("sf.TOKEN").alias("TOKENS_MATCH"),
          F.first("sf.N_TOK_SF").alias("N_TOK_SF"),
          F.first("org.N_TOK_ORG").alias("N_TOK_ORG"),
      )
      .withColumn("RATIO_SF", F.col("TOKENS_MATCH") / F.col("N_TOK_SF"))
      .withColumn("RATIO_ORG", F.col("TOKENS_MATCH") / F.col("N_TOK_ORG"))
)

# Tolerancia
df_match_scores_filtrado = (
    df_match_scores
      .filter(
          (F.col("TOKENS_MATCH") >= 3) &
          (F.col("RATIO_SF") >= 0.60)
      )
)

w = Window.partitionBy("CODMESEVALUACION", "CODSOLICITUD").orderBy(
    F.col("TOKENS_MATCH").desc(),
    F.col("RATIO_SF").desc(),
    F.col("RATIO_ORG").desc(),
    F.col("MATORGANICO").asc()
)

df_best_match_ultactor = (
    df_match_scores_filtrado
      .withColumn("rn", F.row_number().over(w))
      .filter(F.col("rn") == 1)
      .drop("rn")
)



# Tokenizar nombres en SALESFORCE - NBRULTACTORPASO
COL_NOMBRE_SF_PASO = "NBRULTACTORPASO"

df_sf_nombres_paso = (
    df_salesforce_estados
      .withColumn(
          "TOKENS_NOMBRE_SF_PASO",
          F.array_distinct(F.split(F.col(COL_NOMBRE_SF_PASO), r"\s+"))
      )
      .withColumn("N_TOK_SF_PASO", F.size("TOKENS_NOMBRE_SF_PASO"))
)

df_sf_tokens_paso = (
    df_sf_nombres_paso
      .select(
          "CODMESEVALUACION",
          "CODSOLICITUD",
          COL_NOMBRE_SF_PASO,
          "TOKENS_NOMBRE_SF_PASO",
          "N_TOK_SF_PASO"
      )
      .withColumn("TOKEN", F.explode("TOKENS_NOMBRE_SF_PASO"))
)

# Join por MES + TOKEN
df_join_tokens_paso = (
    df_sf_tokens_paso.alias("sf")
      .join(
          df_org_tokens.alias("org"),
          (F.col("sf.CODMESEVALUACION") == F.col("org.CODMES")) &
          (F.col("sf.TOKEN") == F.col("org.TOKEN")),
          "inner"
      )
)

# Score para NBRULTACTORPASO
match_cols_paso = ["CODMESEVALUACION", "CODSOLICITUD", COL_NOMBRE_SF_PASO, "MATORGANICO", "MATSUPERIOR"]

df_match_scores_paso = (
    df_join_tokens_paso
      .groupBy(match_cols_paso)
      .agg(
          F.countDistinct("sf.TOKEN").alias("TOKENS_MATCH"),
          F.first("sf.N_TOK_SF_PASO").alias("N_TOK_SF"),
          F.first("org.N_TOK_ORG").alias("N_TOK_ORG"),
      )
      .withColumn("RATIO_SF", F.col("TOKENS_MATCH") / F.col("N_TOK_SF"))
      .withColumn("RATIO_ORG", F.col("TOKENS_MATCH") / F.col("N_TOK_ORG"))
)

df_match_scores_paso_filtrado = (
    df_match_scores_paso
      .filter(
          (F.col("TOKENS_MATCH") >= 3) &
          (F.col("RATIO_SF") >= 0.60)
      )
)

w_paso = Window.partitionBy("CODMESEVALUACION", "CODSOLICITUD").orderBy(
    F.col("TOKENS_MATCH").desc(),
    F.col("RATIO_SF").desc(),
    F.col("RATIO_ORG").desc(),
    F.col("MATORGANICO").asc()
)

df_best_match_paso = (
    df_match_scores_paso_filtrado
      .withColumn("rn", F.row_number().over(w_paso))
      .filter(F.col("rn") == 1)
      .drop("rn")
)


df_salesforce_enriq = (
    df_salesforce_estados
      # MATORGANICO y MATSUPERIOR del NBRULTACTOR
      .join(
          df_best_match_ultactor.select(
              "CODMESEVALUACION",
              "CODSOLICITUD",
              "MATORGANICO",
              "MATSUPERIOR"
          ),
          on=["CODMESEVALUACION", "CODSOLICITUD"],
          how="left"
      )
      # MATORGANICOPASO y MATSUPERIORPASO del NBRULTACTORPASO
      .join(
          df_best_match_paso.select(
              "CODMESEVALUACION",
              "CODSOLICITUD",
              F.col("MATORGANICO").alias("MATORGANICOPASO"),
              F.col("MATSUPERIOR").alias("MATSUPERIORPASO")
          ),
          on=["CODMESEVALUACION", "CODSOLICITUD"],
          how="left"
      )
)

count_sf_enriq = df_salesforce_enriq.count()
print("Registros enriquecidos:", count_sf_enriq)

SALESFORCE - PRODUCTOS

path_salesfoce_productos = "abfss://bcp-edv-rbmbdn@adlscu1lhclbackp05.dfs.core.windows.net/T72496/CARGA/SALESFORCE/INFORME_PRODUCTO/INFORME_PRODUCTO_*.csv"

df_salesforce_raw = (
    spark.read
        .format("csv")
        .option("header", "true")
        .option("encoding", "ISO-8859-1")
        .load(path_salesfoce_productos)
)


df_salesforce_productos = (
    df_salesforce_raw
        .select(
            F.col("Nombre de la oportunidad").alias("CODSOLICITUD"),
            F.col("Nombre del Producto").alias("NBRPRODUCTO"),
            F.col("Etapa").alias("ETAPA"),
            F.col("Analista de crédito").alias("NBRANALISTA"),
            F.col("Tipo de Acción").alias("TIPACCION"),
            F.col("Fecha de creación").alias("FECCREACION")
        )
)


df_salesforce_productos = (
    df_salesforce_productos
        .withColumn("NBRPRODUCTO", F.upper(F.col("NBRPRODUCTO")))
        .withColumn("ETAPA", F.upper(F.col("ETAPA")))
        .withColumn("NBRANALISTA", F.upper(F.col("NBRANALISTA")))
        .withColumn("TIPACCION", F.upper(F.col("TIPACCION")))
)


df_salesforce_productos = (
    df_salesforce_productos
        .withColumn("NBRPRODUCTO", quitar_tildes("NBRPRODUCTO"))
        .withColumn("ETAPA", quitar_tildes("ETAPA"))
        .withColumn("NBRANALISTA", quitar_tildes("NBRANALISTA"))
        .withColumn("TIPACCION", quitar_tildes("TIPACCION"))
)


df_salesforce_productos = (
    df_salesforce_productos
        .withColumn("FECCREACION_STR", F.trim(F.col("FECCREACION").cast("string")))
        # intentOS
        .withColumn(
            "FECCREACION_DATE",
            F.coalesce(
                F.to_date("FECCREACION_STR", "dd/MM/yyyy"),
                F.to_date("FECCREACION_STR", "yyyy-MM-dd"),
                F.to_date("FECCREACION_STR")                  # fallback
            )
        )
        .withColumn("CODMESCREACION", F.date_format("FECCREACION_DATE", "yyyyMM"))
)


df_salesforce_productos = (
    df_salesforce_productos
        .withColumn("FECCREACION", F.col("FECCREACION_DATE"))
        .drop("FECCREACION_STR", "FECCREACION_DATE")
)


count_sf_original = df_salesforce_productos.count()
print("Registros originales SF:", count_sf_original)


from pyspark.sql import functions as F
from pyspark.sql.window import Window

# ============================================
# 1) Asegurar que los meses tengan mismo tipo
# ============================================
df_organico = df_organico.withColumn("CODMES", F.col("CODMES").cast("string"))
df_salesforce_productos = df_salesforce_productos.withColumn("CODMESCREACION", F.col("CODMESCREACION").cast("string"))

# ============================================
# 2) Tokenizar nombres en ORGÁNICO
#    (si ya lo hiciste antes puedes reutilizar la misma lógica)
# ============================================
df_org_nombres = (
    df_organico
      .withColumn(
          "TOKENS_NOMBRE_ORG",
          F.array_distinct(F.split(F.col("NOMBRECOMPLETO"), r"\s+"))
      )
      .withColumn("N_TOK_ORG", F.size("TOKENS_NOMBRE_ORG"))
)

df_org_tokens = (
    df_org_nombres
      .select(
          "CODMES",
          "MATORGANICO",
          "MATSUPERIOR",
          "NOMBRECOMPLETO",
          "TOKENS_NOMBRE_ORG",
          "N_TOK_ORG"
      )
      .withColumn("TOKEN", F.explode("TOKENS_NOMBRE_ORG"))
)

# ============================================
# 3) Tokenizar NBRANALISTA en productos
# ============================================
COL_NOMBRE_SF_ANALISTA = "NBRANALISTA"

df_sf_analistas = (
    df_salesforce_productos
      .withColumn(
          "TOKENS_NOMBRE_SF",
          F.array_distinct(F.split(F.col(COL_NOMBRE_SF_ANALISTA), r"\s+"))
      )
      .withColumn("N_TOK_SF", F.size("TOKENS_NOMBRE_SF"))
)

df_sf_tokens_analista = (
    df_sf_analistas
      .select(
          "CODMESCREACION",
          "CODSOLICITUD",
          COL_NOMBRE_SF_ANALISTA,
          "TOKENS_NOMBRE_SF",
          "N_TOK_SF"
      )
      .withColumn("TOKEN", F.explode("TOKENS_NOMBRE_SF"))
)

# ============================================
# 4) Join por MES + TOKEN
# ============================================
df_join_tokens_analista = (
    df_sf_tokens_analista.alias("sf")
      .join(
          df_org_tokens.alias("org"),
          (F.col("sf.CODMESCREACION") == F.col("org.CODMES")) &
          (F.col("sf.TOKEN") == F.col("org.TOKEN")),
          "inner"
      )
)

# ============================================
# 5) Score y tolerancia (3 tokens y 60% del nombre SF)
# ============================================
match_cols_analista = [
    "CODMESCREACION",
    "CODSOLICITUD",
    COL_NOMBRE_SF_ANALISTA,
    "MATORGANICO",
    "MATSUPERIOR"
]

df_match_scores_analista = (
    df_join_tokens_analista
      .groupBy(match_cols_analista)
      .agg(
          F.countDistinct("sf.TOKEN").alias("TOKENS_MATCH"),
          F.first("sf.N_TOK_SF").alias("N_TOK_SF"),
          F.first("org.N_TOK_ORG").alias("N_TOK_ORG"),
      )
      .withColumn("RATIO_SF", F.col("TOKENS_MATCH") / F.col("N_TOK_SF"))
      .withColumn("RATIO_ORG", F.col("TOKENS_MATCH") / F.col("N_TOK_ORG"))
)

df_match_scores_analista_filtrado = (
    df_match_scores_analista
      .filter(
          (F.col("TOKENS_MATCH") >= 3) &
          (F.col("RATIO_SF") >= 0.60)
      )
)

# ============================================
# 6) Elegir el mejor MATORGANICO por solicitud y mes
# ============================================
w_analista = Window.partitionBy("CODMESCREACION", "CODSOLICITUD").orderBy(
    F.col("TOKENS_MATCH").desc(),
    F.col("RATIO_SF").desc(),
    F.col("RATIO_ORG").desc(),
    F.col("MATORGANICO").asc()
)

df_best_match_analista = (
    df_match_scores_analista_filtrado
      .withColumn("rn", F.row_number().over(w_analista))
      .filter(F.col("rn") == 1)
      .drop("rn")
)

# ============================================
# 7) Pegar MATORGANICO_ANALISTA y MATSUPERIOR_ANALISTA
# ============================================
df_productos_enriq = (
    df_salesforce_productos
      .join(
          df_best_match_analista.select(
              "CODMESCREACION",
              "CODSOLICITUD",
              F.col("MATORGANICO").alias("MATORGANICO_ANALISTA"),
              F.col("MATSUPERIOR").alias("MATSUPERIOR_ANALISTA")
          ),
          on=["CODMESCREACION", "CODSOLICITUD"],
          how="left"
      )
)


count_sf_original = df_productos_enriq.count()
print("Registros originales SF:", count_sf_original)
