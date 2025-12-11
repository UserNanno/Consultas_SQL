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
