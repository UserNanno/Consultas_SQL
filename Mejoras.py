# =========================================================
# AJUSTE 1: ESTADOSOLICITUD_PASO = resultado del analista
# =========================================================

# ====== Estado del paso base (resultado del analista) ======
w_paso_base = Window.partitionBy("CODSOLICITUD").orderBy(
    F.col("FECHORINICIOEVALUACION").desc(),
    F.col("FECHORFINEVALUACION").desc()
)

df_estado_paso_base = (
    df_salesforce_enriq
      .filter(es_paso_base)  # ya lo tienes definido arriba
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










# =========================================================
# AJUSTE 2: Autonomía por monto (SUP / GERENTE)
# =========================================================


# ====== Regla de autonomía por monto ======
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
    F.when(rol_regla == "GERENTE", F.lit("U17293"))                 # 👈 gerente único hardcode
     .when(rol_regla == "SUPERVISOR", F.col("MATSUPERIOR_ANALISTA")) # 👈 supervisor directo
)

df_final_solicitud = (
    df_final_solicitud
      # Pisar autonomía si aplica regla
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












# =========================================================
# AJUSTE 3: Estado final RECHAZADO con trazabilidad
# =========================================================


cond_incoherencia = (
    (F.col("ESTADOSOLICITUD_ULTIMO") == "PENDIENTE") &
    (F.col("ETAPA") == "DESESTIMADA") &
    (rol_regla.isNotNull())
)

df_final_solicitud = (
    df_final_solicitud
      .withColumn("ESTADOSOLICITUD_INICIAL", F.col("ESTADOSOLICITUD_ULTIMO"))  # ✅ única columna nueva
      .withColumn(
          "ESTADOSOLICITUD_ULTIMO",
          F.when(cond_incoherencia, F.lit("RECHAZADO"))
           .otherwise(F.col("ESTADOSOLICITUD_ULTIMO"))
      )
)
