# Paso base según tipo
es_paso_base = (is_tc & paso_tc_analista) | (is_cef & paso_cef_analista)
w_base = Window.partitionBy("CODSOLICITUD").orderBy(F.col("FECHORINICIOEVALUACION").desc())
df_base_latest = (
   df_estados_enriq
     .filter(es_paso_base)
     .withColumn("rn_base", F.row_number().over(w_base))
     .filter(F.col("rn_base") == 1)
     .select(
         "CODSOLICITUD",
         F.col("MATORGANICO").alias("MAT1_ESTADOS"),        # desde NBRULTACTOR enriquecido
         F.col("MATORGANICOPASO").alias("MAT2_ESTADOS"),    # desde NBRULTACTORPASO enriquecido
         F.col("FECHORINICIOEVALUACION").alias("TS_BASE_ESTADOS")
     )
)
df_matanalista_estados = (
   df_base_latest
     .withColumn(
         "MATANALISTA_ESTADOS",
         F.when(F.col("MAT1_ESTADOS").isNotNull() & (~es_sup_o_ger(F.col("MAT1_ESTADOS"))), F.col("MAT1_ESTADOS"))
          .when(F.col("MAT2_ESTADOS").isNotNull() & (~es_sup_o_ger(F.col("MAT2_ESTADOS"))), F.col("MAT2_ESTADOS"))
          .otherwise(F.lit(None).cast("string"))
     )
     .withColumn(
         "ORIGEN_MATANALISTA_ESTADOS",
         F.when(F.col("MAT1_ESTADOS").isNotNull() & (~es_sup_o_ger(F.col("MAT1_ESTADOS"))), F.lit("ESTADOS_MAT1"))
          .when(F.col("MAT2_ESTADOS").isNotNull() & (~es_sup_o_ger(F.col("MAT2_ESTADOS"))), F.lit("ESTADOS_MAT2"))
          .otherwise(F.lit(None))
     )
     .select("CODSOLICITUD", "MATANALISTA_ESTADOS", "ORIGEN_MATANALISTA_ESTADOS", "TS_BASE_ESTADOS")
)
# Fallback de productos (1 fila por solicitud)
df_matanalista_productos = (
   df_productos_enriq
     .select(
         "CODSOLICITUD",
         F.col("MATORGANICO_ANALISTA").alias("MATANALISTA_PRODUCTOS")
     )
     .dropDuplicates(["CODSOLICITUD"])
)
# Analista final (prioriza estados; fallback productos solo si estados no pudo)
df_matanalista_final = (
   df_matanalista_estados
     .join(df_matanalista_productos, on="CODSOLICITUD", how="left")
     .withColumn(
         "MATANALISTA_FINAL",
         F.coalesce(F.col("MATANALISTA_ESTADOS"), F.col("MATANALISTA_PRODUCTOS"))
     )
     .withColumn(
         "ORIGEN_MATANALISTA",
         F.when(F.col("MATANALISTA_ESTADOS").isNotNull(), F.col("ORIGEN_MATANALISTA_ESTADOS"))
          .when(F.col("MATANALISTA_PRODUCTOS").isNotNull(), F.lit("PRODUCTOS_NBRANALISTA"))
          .otherwise(F.lit(None))
     )
     .drop("MATANALISTA_ESTADOS", "ORIGEN_MATANALISTA_ESTADOS", "MATANALISTA_PRODUCTOS")
)
