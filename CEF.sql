from pyspark.sql import functions as F

df_estados = df_estados.withColumn(
    "ROW_ID",
    F.sha2(
        F.concat_ws("||",
            F.col("CODSOLICITUD").cast("string"),
            F.col("CODMESEVALUACION").cast("string"),
            F.col("NBRPASO").cast("string"),
            F.col("FECHORINICIOEVALUACION").cast("string"),
            F.col("FECHORFINEVALUACION").cast("string"),
            F.col("NBRULTACTOR").cast("string"),
            F.col("NBRULTACTORPASO").cast("string"),
        ),
        256
    )
)



df_best_match_ultactor = match_persona_vs_organico_row(
    df_org_tokens=df_org_tokens,
    df_sf=df_estados,                    # ya trae ROW_ID
    row_id_col="ROW_ID",
    codmes_sf_col="CODMESEVALUACION",
    nombre_sf_col="NBRULTACTOR"
)

df_best_match_paso = match_persona_vs_organico_row(
    df_org_tokens=df_org_tokens,
    df_sf=df_estados,
    row_id_col="ROW_ID",
    codmes_sf_col="CODMESEVALUACION",
    nombre_sf_col="NBRULTACTORPASO"
)

df_estados_enriq = (
    df_estados
      .join(
          df_best_match_ultactor.select(
              "ROW_ID",
              F.col("MATORGANICO").alias("MAT_ULTACTOR"),
              F.col("MATSUPERIOR").alias("MAT_SUP_ULTACTOR"),
          ),
          on="ROW_ID", how="left"
      )
      .join(
          df_best_match_paso.select(
              "ROW_ID",
              F.col("MATORGANICO").alias("MAT_ULTACTORPASO"),
              F.col("MATSUPERIOR").alias("MAT_SUP_ULTACTORPASO"),
          ),
          on="ROW_ID", how="left"
      )
)
