# =========================================================
# 4) df_analista_from_estados: PASO BASE (Mat1->Mat2), evita sup/ger
#    - TC: APROBACION ... ANALISTA
#    - CEF: EVALUACION DE SOLICITUD
# =========================================================
es_paso_base = (is_tc & paso_tc_analista) | (is_cef & paso_cef_analista)

df_base_candidates = (
    df_salesforce_enriq
      .filter(es_paso_base)
      .withColumn("MAT1", F.col("MATORGANICO"))
      .withColumn("MAT2", F.col("MATORGANICOPASO"))
)

w_base = Window.partitionBy("CODSOLICITUD").orderBy(F.col("FECHORINICIOEVALUACION").desc())

df_base_latest = (
    df_base_candidates
      .withColumn("rn_base", F.row_number().over(w_base))
      .filter(F.col("rn_base") == 1)
      .select("CODSOLICITUD", "MAT1", "MAT2")
      .withColumn("FLG_EXISTE_PASO_BASE", F.lit(1))
)

df_analista_from_estados = (
    df_base_latest
      .withColumn(
          "MAT_ANALISTA_ESTADOS",
          F.when(F.col("MAT1").isNotNull() & (~es_sup_o_ger(F.col("MAT1"))), F.col("MAT1"))
           .when(F.col("MAT2").isNotNull() & (~es_sup_o_ger(F.col("MAT2"))), F.col("MAT2"))
           .otherwise(F.lit(None).cast("string"))
      )
      .withColumn(
          "ORIGEN_MAT_ANALISTA_ESTADOS",
          F.when(F.col("MAT1").isNotNull() & (~es_sup_o_ger(F.col("MAT1"))), F.lit("ESTADOS_MAT1"))
           .when(F.col("MAT2").isNotNull() & (~es_sup_o_ger(F.col("MAT2"))), F.lit("ESTADOS_MAT2"))
           .otherwise(F.lit(None))
      )
      .select(
          "CODSOLICITUD",
          "FLG_EXISTE_PASO_BASE",
          "MAT_ANALISTA_ESTADOS",
          "ORIGEN_MAT_ANALISTA_ESTADOS"
      )
)
