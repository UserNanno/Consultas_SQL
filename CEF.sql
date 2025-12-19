df_final_solicitud = (
    df_last_estado
      .join(df_productos_base, on="CODSOLICITUD", how="left")
      .join(df_analista_from_estados, on="CODSOLICITUD", how="left")
      .join(df_autonomia, on="CODSOLICITUD", how="left")
      .withColumn("FLG_FALTA_PASO_BASE", F.when(F.col("FLG_EXISTE_PASO_BASE").isNull(), 1).otherwise(0))
      .withColumn(
          "FLG_AUTONOMIA_SIN_PASO_BASE",
          F.when((F.col("ROL_AUTONOMIA").isNotNull()) & (F.col("FLG_EXISTE_PASO_BASE").isNull()), 1).otherwise(0)
      )
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












w_paso_base = Window.partitionBy("CODSOLICITUD").orderBy(
    F.col("FECHORINICIOEVALUACION").desc(),
    F.col("FECHORFINEVALUACION").desc()
)

df_estado_paso_base = (
    df_salesforce_enriq
      .filter(es_paso_base)
      .withColumn("rn", F.row_number().over(w_paso_base))
      .filter(F.col("rn") == 1)
      .select(
          "CODSOLICITUD",
          F.col("ESTADOSOLICITUDPASO").alias("ESTADOSOLICITUD_PASO_BASE")
      )
)

df_final_solicitud = (
    df_final_solicitud
      .join(df_estado_paso_base, on="CODSOLICITUD", how="left")
      .withColumn(
          "ESTADOSOLICITUD_PASO",
          F.coalesce(F.col("ESTADOSOLICITUD_PASO_BASE"), F.col("ESTADOSOLICITUD_PASO"))
      )
      .drop("ESTADOSOLICITUD_PASO_BASE")
)














def to_num(col):
    return F.regexp_replace(F.col(col).cast("string"), ",", "").cast("double")

df_final_solicitud = df_final_solicitud.withColumn("MTOAPROBADO_NUM", to_num("MTOAPROBADO"))

aprob_analista = (F.col("ESTADOSOLICITUD_PASO") == "APROBADO")
mto = F.col("MTOAPROBADO_NUM")

rol_regla = (
    F.when(aprob_analista & (mto >= 240000), F.lit("GERENTE"))
     .when(aprob_analista & (mto >= 100000), F.lit("SUPERVISOR"))
     .otherwise(F.lit(None))
)

mat_aut_regla = (
    F.when(rol_regla == "GERENTE", F.lit("U17293"))
     .when(rol_regla == "SUPERVISOR", F.col("MATSUPERIOR_ANALISTA"))
)

df_final_solicitud = (
    df_final_solicitud
      .withColumn("ROL_AUTONOMIA",
          F.when(rol_regla.isNotNull(), rol_regla).otherwise(F.col("ROL_AUTONOMIA"))
      )
      .withColumn("MAT_AUTONOMIA",
          F.when(rol_regla.isNotNull(), mat_aut_regla).otherwise(F.col("MAT_AUTONOMIA"))
      )
      .withColumn("NBRPASO_AUTONOMIA",
          F.when(rol_regla == "GERENTE", F.lit("AUTONOMIA POR MONTO GERENTE"))
           .when(rol_regla == "SUPERVISOR", F.lit("AUTONOMIA POR MONTO SUPERVISOR"))
           .otherwise(F.col("NBRPASO_AUTONOMIA"))
      )
      .withColumn("FLG_AUTONOMIA_GERENTE",
          F.when(rol_regla == "GERENTE", F.lit(1))
           .when(rol_regla.isNotNull(), F.lit(0))
           .otherwise(F.col("FLG_AUTONOMIA_GERENTE"))
      )
      .withColumn("FLG_AUTONOMIA_SUPERVISOR",
          F.when(rol_regla == "SUPERVISOR", F.lit(1))
           .when(rol_regla.isNotNull(), F.lit(0))
           .otherwise(F.col("FLG_AUTONOMIA_SUPERVISOR"))
      )
      .withColumn("FLG_AUTONOMIA_ANALISTA",
          F.when(rol_regla.isNotNull(), F.lit(0)).otherwise(F.col("FLG_AUTONOMIA_ANALISTA"))
      )
      .withColumn("TS_AUTONOMIA",
          F.when(rol_regla.isNotNull(), F.col("TS_ULTIMO_EVENTO")).otherwise(F.col("TS_AUTONOMIA"))
      )
)












cond_incoherencia = (
    (F.col("ESTADOSOLICITUD_ULTIMO") == "PENDIENTE") &
    (F.col("ETAPA") == "DESESTIMADA") &
    (rol_regla.isNotNull())
)

df_final_solicitud = (
    df_final_solicitud
      .withColumn("ESTADOSOLICITUD_INICIAL", F.col("ESTADOSOLICITUD_ULTIMO"))
      .withColumn(
          "ESTADOSOLICITUD_ULTIMO",
          F.when(cond_incoherencia, F.lit("RECHAZADO"))
           .otherwise(F.col("ESTADOSOLICITUD_ULTIMO"))
      )
)













df_final_solicitud = (
    df_final_solicitud
      .join(
          df_powerapp.select(
              "CODSOLICITUD",
              F.col("MATANALISTA").alias("MATANALISTA_APPS"),
              F.col("RESULTADOANALISTA").alias("RESULTADOANALISTA_APPS"),
              F.col("PRODUCTO").alias("PRODUCTO_APPS"),
              "MOTIVORESULTADOANALISTA",
              "MOTIVOMALADERIVACION",
              "SUBMOTIVOMALADERIVACION"
          ).dropDuplicates(["CODSOLICITUD"]),
          on="CODSOLICITUD",
          how="left"
      )
      .withColumn("MAT_ANALISTA_FINAL", F.coalesce(F.col("MAT_ANALISTA_FINAL"), F.col("MATANALISTA_APPS")))
      .withColumn("ESTADOSOLICITUD_ULTIMO", F.coalesce(F.col("ESTADOSOLICITUD_ULTIMO"), F.col("RESULTADOANALISTA_APPS")))
      .withColumn("NBRPRODUCTO", F.coalesce(F.col("NBRPRODUCTO"), F.col("PRODUCTO_APPS")))
      .withColumn(
          "ORIGEN_MAT_ANALISTA",
          F.when(F.col("ORIGEN_MAT_ANALISTA").isNull() & F.col("MATANALISTA_APPS").isNotNull(), F.lit("APPS_MAT"))
           .otherwise(F.col("ORIGEN_MAT_ANALISTA"))
      )
      .drop("MATANALISTA_APPS", "RESULTADOANALISTA_APPS", "PRODUCTO_APPS")
)
