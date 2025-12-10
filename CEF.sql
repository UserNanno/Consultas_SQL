from pyspark.sql import functions as F
from pyspark.sql.window import Window

# ============================================
# 1) Asegurar que los meses tengan mismo tipo
# ============================================
df_organico = df_organico.withColumn("CODMES", F.col("CODMES").cast("string"))
df_salesforce_estados = df_salesforce_estados.withColumn("CODMESEVALUACION", F.col("CODMESEVALUACION").cast("string"))

# ============================================
# 2) Tokenizar nombres en ORGÁNICO
# ============================================
df_org_nombres = (
    df_organico
      .withColumn(
          "TOKENS_NOMBRE_ORG",
          F.array_distinct(F.split(F.col("NOMBRECOMPLETO"), r"\s+"))
      )
      .withColumn("N_TOK_ORG", F.size("TOKENS_NOMBRE_ORG"))
)

# ============================================
# 3) Tokenizar nombres en SALESFORCE - NBRULTACTOR
# ============================================
COL_NOMBRE_SF = "NBRULTACTOR"

df_sf_nombres = (
    df_salesforce_estados
      .withColumn(
          "TOKENS_NOMBRE_SF",
          F.array_distinct(F.split(F.col(COL_NOMBRE_SF), r"\s+"))
      )
      .withColumn("N_TOK_SF", F.size("TOKENS_NOMBRE_SF"))
)

# ============================================
# 4) Explode de tokens (una fila por token)
# ============================================
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

# ============================================
# 5) Join por MES + TOKEN
# ============================================
df_join_tokens = (
    df_sf_tokens.alias("sf")
      .join(
          df_org_tokens.alias("org"),
          (F.col("sf.CODMESEVALUACION") == F.col("org.CODMES")) &
          (F.col("sf.TOKEN") == F.col("org.TOKEN")),
          "inner"
      )
)

# ============================================
# 6) Score de similitud por solicitud + matrícula
# ============================================
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

# 7) Tolerancia
df_match_scores_filtrado = (
    df_match_scores
      .filter(
          (F.col("TOKENS_MATCH") >= 3) &
          (F.col("RATIO_SF") >= 0.60)
      )
)

# 8) Mejor MATORGANICO por solicitud/mes
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
















# ============================================
# 3B) Tokenizar nombres en SALESFORCE - NBRULTACTORPASO
# ============================================
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








df_salesforce_enriq.select(
    "CODMESEVALUACION", "CODSOLICITUD",
    "NBRULTACTOR", "MATORGANICO",
    "NBRULTACTORPASO", "MATORGANICOPASO"
).show(20, truncate=False)

