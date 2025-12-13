from pyspark.sql import functions as F
from pyspark.sql.window import Window
import re
from functools import reduce

# ---------- PATHS ----------
BASE_DIR_ORGANICO = "abfss://bcp-edv-rbmbdn@adlscu1lhclbackp05.dfs.core.windows.net/T72496/CARGA/ORGANICO/"
PATH_SF_ESTADOS = "abfss://bcp-edv-rbmbdn@adlscu1lhclbackp05.dfs.core.windows.net/T72496/CARGA/SALESFORCE/INFORME_ESTADO/INFORME_ESTADO_*.csv"
PATH_SF_PRODUCTOS = "abfss://bcp-edv-rbmbdn@adlscu1lhclbackp05.dfs.core.windows.net/T72496/CARGA/SALESFORCE/INFORME_PRODUCTO/INFORME_PRODUCTO_*.csv"

# ---------- HELPERS GENERICOS ----------

def quitar_tildes(col):
    c = F.regexp_replace(col, "[ÁÀÂÄáàâä]", "A")
    c = F.regexp_replace(c, "[ÉÈÊËéèêë]", "E")
    c = F.regexp_replace(c, "[ÍÌÎÏíìîï]", "I")
    c = F.regexp_replace(c, "[ÓÒÔÖóòôö]", "O")
    c = F.regexp_replace(c, "[ÚÙÛÜúùûü]", "U")
    return c

def norm_txt_spark(col_name):
    return F.trim(quitar_tildes(F.upper(F.col(col_name))))

def limpiar_cesado(col_name):
    return F.trim(F.regexp_replace(F.col(col_name), r'\(CESADO\)', ''))

def parse_fecha_hora_esp_col(col_name):
    s = F.col(col_name).cast("string")
    s = F.regexp_replace(s, u'\u00A0', ' ')
    s = F.regexp_replace(s, u'\u202F', ' ')
    s = F.lower(F.trim(s))
    s = F.regexp_replace(s, r'\s+', ' ')
    s = F.regexp_replace(s, r'(?i)a\W*m\W*', 'AM')
    s = F.regexp_replace(s, r'(?i)p\W*m\W*', 'PM')
    return F.to_timestamp(s, 'dd/MM/yyyy hh:mm a')

def match_persona_vs_organico(
    df_org_tokens,
    df_sf,
    codmes_sf_col,
    codsol_col,
    nombre_sf_col,
    min_tokens=3,
    min_ratio_sf=0.60
):
    # Tokenizar nombre en SF
    df_sf_nombres = (
        df_sf
          .withColumn(
              "TOKENS_NOMBRE_SF",
              F.array_distinct(F.split(F.col(nombre_sf_col), r"\s+"))
          )
          .withColumn("N_TOK_SF", F.size("TOKENS_NOMBRE_SF"))
    )

    df_sf_tokens = (
        df_sf_nombres
          .select(
              F.col(codmes_sf_col),
              F.col(codsol_col),
              F.col(nombre_sf_col),
              F.col("TOKENS_NOMBRE_SF"),
              F.col("N_TOK_SF")
          )
          .withColumn("TOKEN", F.explode("TOKENS_NOMBRE_SF"))
    )

    # Join por MES + TOKEN
    df_join_tokens = (
        df_sf_tokens.alias("sf")
          .join(
              df_org_tokens.alias("org"),
              (F.col(f"sf.{codmes_sf_col}") == F.col("org.CODMES")) &
              (F.col("sf.TOKEN") == F.col("org.TOKEN")),
              "inner"
          )
    )

    match_cols = [
        codmes_sf_col,
        codsol_col,
        nombre_sf_col,
        "MATORGANICO",
        "MATSUPERIOR"
    ]

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

    df_match_filtrado = (
        df_match_scores
          .filter(
              (F.col("TOKENS_MATCH") >= min_tokens) &
              (F.col("RATIO_SF") >= min_ratio_sf)
          )
    )

    w = Window.partitionBy(codmes_sf_col, codsol_col).orderBy(
        F.col("TOKENS_MATCH").desc(),
        F.col("RATIO_SF").desc(),
        F.col("RATIO_ORG").desc(),
        F.col("MATORGANICO").asc()
    )

    df_best_match = (
        df_match_filtrado
          .withColumn("rn", F.row_number().over(w))
          .filter(F.col("rn") == 1)
          .drop("rn")
    )

    return df_best_match

entries = dbutils.fs.ls(BASE_DIR_ORGANICO)
files = [e.path for e in entries if e.name.startswith("1n_Activos_2025") and e.name.endswith(".xlsx")]
assert files, "No se encontraron archivos 1n_Activos_2025*.xlsx"

excel_options = {
    "header": "true",
    "inferSchema": "true",
    "treatEmptyValuesAsNulls": "true",
    "timestampFormat": "yyyy-MM-dd HH:mm:ss",
}

dfs_org = []
for path in files:
    reader = spark.read.format("com.crealytics.spark.excel")
    for k, v in excel_options.items():
        reader = reader.option(k, v)
    df_tmp = reader.load(path)

    for c in df_tmp.columns:
        new_name = re.sub(r"\s+", " ", c.strip())
        if new_name != c:
            df_tmp = df_tmp.withColumnRenamed(c, new_name)

    cols_clean = [c for c in df_tmp.columns if not re.match(r"^_c\d+$", c)]
    df_tmp = df_tmp.select(*cols_clean)

    dfs_org.append(df_tmp)

df_organico_raw = reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), dfs_org)

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

for c in ["MATORGANICO", "NOMBRECOMPLETO", "MATSUPERIOR"]:
    df_organico = df_organico.withColumn(c, norm_txt_spark(c))

df_organico = df_organico.withColumn(
    "MATSUPERIOR",
    F.regexp_replace(F.col("MATSUPERIOR"), r'^0(?=[A-Z]\d{5})', '')
)

df_organico = df_organico.withColumn("FECINGRESO", F.to_date("FECINGRESO"))
df_organico = df_organico.withColumn("NOMBRECOMPLETO_CLEAN", limpiar_cesado("NOMBRECOMPLETO"))
df_organico = df_organico.withColumn("CODMES", F.col("CODMES").cast("string"))

# Tokenización orgánico (para todos los matchings)
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

print("Organico listo:", df_organico.count())

