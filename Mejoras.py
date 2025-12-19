df_final_solicitud = (
    df_last_estado
      .join(df_productos_base, on="CODSOLICITUD", how="left")
      .join(df_analista_from_estados, on="CODSOLICITUD", how="left")
      .join(df_flag_paso_base, on="CODSOLICITUD", how="left")
      .join(df_autonomia, on="CODSOLICITUD", how="left")
      .withColumn("FLG_FALTA_PASO_BASE", F.when(F.col("FLG_EXISTE_PASO_BASE").isNull(), 1).otherwise(0))
      .withColumn(
          "FLG_AUTONOMIA_SIN_PASO_BASE",
          F.when((F.col("ROL_AUTONOMIA").isNotNull()) & (F.col("FLG_EXISTE_PASO_BASE").isNull()), 1).otherwise(0)
      )
      # Analista final: Mat1/Mat2 (estados) y fallback a Mat3 (productos)
      .withColumn(
          "MAT_ANALISTA_FINAL",
          F.coalesce(F.col("MAT_ANALISTA_ESTADOS"), F.col("MATORGANICO_ANALISTA"))
      )
      .withColumn(
          "ORIGEN_MAT_ANALISTA",
          F.when(F.col("MAT_ANALISTA_ESTADOS").isNotNull(), F.col("ORIGEN_MAT_ANALISTA_ESTADOS"))
           .when(F.col("MATORGANICO_ANALISTA").isNotNull(), F.lit("PRODUCTOS_MAT3"))
           .otherwise(F.lit(None))
      )
      .withColumn("TS_ULTIMO_EVENTO", F.col("FECHORINICIOEVALUACION_ULTIMO"))
)
