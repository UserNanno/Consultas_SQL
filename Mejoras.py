# =========================================================
# 4) df_analista_from_estados (MEJORADO)
#    - Prioriza paso base
#    - Si no hay analista en paso base, busca en otros pasos
#    - Prefiere MAT2 (actor del paso) cuando MAT1 es sup/ger
# =========================================================

# 4.1 Candidatos: todos los registros de Estados (para la solicitud)
df_candidates_all = (
    df_salesforce_enriq
      .withColumn("MAT1", F.col("MATORGANICO"))
      .withColumn("MAT2", F.col("MATORGANICOPASO"))
      .withColumn("ES_PASO_BASE", F.when(es_paso_base, F.lit(1)).otherwise(F.lit(0)))
)

# 4.2 Resolver "mat analista" por fila (misma regla que ya tienes):
#     - usar MAT1 si no es sup/ger
#     - si MAT1 es sup/ger, usar MAT2 si no es sup/ger
df_candidates_all = (
    df_candidates_all
      .withColumn(
          "MAT_ANALISTA_CAND",
          F.when(F.col("MAT1").isNotNull() & (~es_sup_o_ger(F.col("MAT1"))), F.col("MAT1"))
           .when(F.col("MAT2").isNotNull() & (~es_sup_o_ger(F.col("MAT2"))), F.col("MAT2"))
           .otherwise(F.lit(None).cast("string"))
      )
      .withColumn(
          "ORIGEN_CAND",
          F.when(F.col("MAT1").isNotNull() & (~es_sup_o_ger(F.col("MAT1"))), F.lit("ESTADOS_MAT1"))
           .when(F.col("MAT2").isNotNull() & (~es_sup_o_ger(F.col("MAT2"))), F.lit("ESTADOS_MAT2"))
           .otherwise(F.lit(None))
      )
)

# 4.3 Elegir el mejor candidato por solicitud:
#     1) primero registros del paso base (ES_PASO_BASE=1)
#     2) luego el evento más reciente
w_best = Window.partitionBy("CODSOLICITUD").orderBy(
    F.col("ES_PASO_BASE").desc(),
    F.col("FECHORINICIOEVALUACION").desc()
)

df_analista_from_estados = (
    df_candidates_all
      .filter(F.col("MAT_ANALISTA_CAND").isNotNull())
      .withColumn("rn", F.row_number().over(w_best))
      .filter(F.col("rn") == 1)
      .select(
          "CODSOLICITUD",
          F.lit(1).alias("FLG_EXISTE_PASO_BASE"),  # si quieres conservar tu flag original, ver nota abajo
          F.col("MAT_ANALISTA_CAND").alias("MAT_ANALISTA_ESTADOS"),
          F.col("ORIGEN_CAND").alias("ORIGEN_MAT_ANALISTA_ESTADOS")
      )
)
