# =========================================================
# 4) df_analista_from_estados: MEJORADO
#    - Prioriza paso base
#    - Si no hay analista en paso base, busca en otros pasos
#    - Evita sup/ger (igual que antes)
# =========================================================
es_paso_base = (is_tc & paso_tc_analista) | (is_cef & paso_cef_analista)

df_candidates_all = (
    df_salesforce_enriq
      .withColumn("MAT1", F.col("MATORGANICO"))
      .withColumn("MAT2", F.col("MATORGANICOPASO"))
      .withColumn("ES_PASO_BASE", F.when(es_paso_base, F.lit(1)).otherwise(F.lit(0)))
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

w_best = Window.partitionBy("CODSOLICITUD").orderBy(
    F.col("ES_PASO_BASE").desc(),
    F.col("FECHORINICIOEVALUACION").desc(),
    F.col("FECHORFINEVALUACION").desc()
)

df_analista_from_estados = (
    df_candidates_all
      .filter(F.col("MAT_ANALISTA_CAND").isNotNull())
      .withColumn("rn", F.row_number().over(w_best))
      .filter(F.col("rn") == 1)
      .select(
          "CODSOLICITUD",
          F.col("MAT_ANALISTA_CAND").alias("MAT_ANALISTA_ESTADOS"),
          F.col("ORIGEN_CAND").alias("ORIGEN_MAT_ANALISTA_ESTADOS")
      )
)









df_flag_base = (
    df_salesforce_enriq
      .filter(F.col("NBRPASO") == "EVALUACION DE SOLICITUD")
      .select("CODSOLICITUD").distinct()
      .withColumn("EXISTE_BASE", F.lit(1))
)

df_flag_aprob = (
    df_salesforce_enriq
      .filter(F.col("NBRPASO") == "EVALUACION DE SOLICITUD APROBADOR")
      .select("CODSOLICITUD").distinct()
      .withColumn("EXISTE_APROBADOR", F.lit(1))
)

df_flag_paso_base = (
    df_flag_base
      .join(df_flag_aprob, on="CODSOLICITUD", how="left")
      .withColumn(
          "FLG_EXISTE_PASO_BASE",
          F.when((F.col("EXISTE_BASE") == 1) & (F.col("EXISTE_APROBADOR").isNull()), F.lit(1))
           .otherwise(F.lit(None))
      )
      .select("CODSOLICITUD", "FLG_EXISTE_PASO_BASE")
)





.join(df_analista_from_estados, on="CODSOLICITUD", how="left")



.join(df_flag_paso_base, on="CODSOLICITUD", how="left")
