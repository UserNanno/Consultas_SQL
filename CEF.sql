from pyspark.sql import functions as F
from pyspark.sql.window import Window

# ============================================
# 1) Asegurar que los meses tengan mismo tipo
# ============================================
# Puede ser INT o STRING, lo importante es que ambos sean IGUALES.
# Aquí los dejo como STRING.
df_organico = df_organico.withColumn("CODMES", F.col("CODMES").cast("string"))
df_salesforce_estados = df_salesforce_estados.withColumn("CODMESEVALUACION", F.col("CODMESEVALUACION").cast("string"))

# ============================================
# 2) Tokenizar nombres en ORGÁNICO
# ============================================
# Usamos NOMBRECOMPLETO como tú pediste.
# TOKENS_NOMBRE_ORG será un array con cada palabra del nombre.
df_org_nombres = (
    df_organico
      .withColumn(
          "TOKENS_NOMBRE_ORG",
          F.array_distinct(F.split(F.col("NOMBRECOMPLETO"), r"\s+"))
      )
      .withColumn("N_TOK_ORG", F.size("TOKENS_NOMBRE_ORG"))
)

# ============================================
# 3) Tokenizar nombres en SALESFORCE
# ============================================
# Elige la columna que mejor represente a la persona.
COL_NOMBRE_SF = "NBRULTACTOR"  # cambia a "NBRULTACTORPASO" si prefieres

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
# Cada fila resultante es "este token del nombre SF coincide con este token del
# nombre ORG para el mismo mes".
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
# Agrupamos por solicitud, nombre SF y MATORGANICO, y contamos cuántos tokens
# coinciden.
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

# ============================================
# 7) Tolerancia mínima:
#    - al menos 3 tokens que coincidan
#    - al menos 60% del nombre de Salesforce coincide
# ============================================
df_match_scores_filtrado = (
    df_match_scores
      .filter(
          (F.col("TOKENS_MATCH") >= 3) &
          (F.col("RATIO_SF") >= 0.60)
      )
)

# ============================================
# 8) Elegir el MATORGANICO "más exacto" por solicitud y mes
# ============================================
# Puede haber homónimos: mismo CODMESEVALUACION, mismo nombre SF,
# pero varias matrículas. Aquí nos quedamos con la coincidencia más fuerte.
w = Window.partitionBy("CODMESEVALUACION", "CODSOLICITUD").orderBy(
    F.col("TOKENS_MATCH").desc(),  # primero, más tokens en común
    F.col("RATIO_SF").desc(),      # luego, mayor % del nombre SF
    F.col("RATIO_ORG").desc(),     # luego, mayor % del nombre ORG
    F.col("MATORGANICO").asc()     # desempate estable si todo lo demás empata
)

df_best_match = (
    df_match_scores_filtrado
      .withColumn("rn", F.row_number().over(w))
      .filter(F.col("rn") == 1)
      .drop("rn")
)

# df_best_match tiene:
# - 1 fila por CODMESEVALUACION + CODSOLICITUD
# - El MATORGANICO que mejor matchea por nombres en ese mes

# ============================================
# 9) Pegar MATORGANICO al df_salesforce_estados
# ============================================
# Esto se hace a nivel de solicitud (CODSOLICITUD) y mes: todas las filas
# repetidas (por diferente hora, etc.) heredan el mismo MATORGANICO.
df_salesforce_enriq = (
    df_salesforce_estados
      .join(
          df_best_match.select("CODMESEVALUACION", "CODSOLICITUD", "MATORGANICO", "MATSUPERIOR"),
          on=["CODMESEVALUACION", "CODSOLICITUD"],
          how="left"
      )
)
