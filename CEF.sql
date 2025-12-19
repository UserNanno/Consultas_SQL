asi es mi df final

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

# Selección final SIN NOMBRES (todo por matrícula)
df_final_solicitud = df_final_solicitud.select(
    "CODSOLICITUD",
    "TIPO_PRODUCTO",

    # Productos
    "CODMESCREACION", "FECCREACION",
    "NBRPRODUCTO", "ETAPA", "TIPACCION",
    "NBRDIVISA", "MTOSOLICITADO", "MTOAPROBADO",
    "MTOOFERTADO", "MTODESEMBOLSADO", "CENTROATENCION",
    "MATORGANICO_ANALISTA", "MATSUPERIOR_ANALISTA",

    # Último estado (snapshot)
    "CODMESEVALUACION_ULTIMO", "PROCESO_ULTIMO",
    "NBRPASO_ULTIMO", "ESTADOSOLICITUD_ULTIMO", "ESTADOSOLICITUD_PASO",
    "FECHORINICIOEVALUACION_ULTIMO", "FECINICIOEVALUACION_ULTIMO", "HORINICIOEVALUACION_ULTIMO",
    "FECHORFINEVALUACION_ULTIMO", "FECFINEVALUACION_ULTIMO", "HORFINEVALUACION_ULTIMO",

    # Analista final
    "MAT_ANALISTA_FINAL", "ORIGEN_MAT_ANALISTA",
    "FLG_FALTA_PASO_BASE",

    # Autonomía
    "ROL_AUTONOMIA", "MAT_AUTONOMIA", "NBRPASO_AUTONOMIA",
    "FLG_AUTONOMIA_GERENTE", "FLG_AUTONOMIA_SUPERVISOR", "FLG_AUTONOMIA_ANALISTA",
    "FLG_AUTONOMIA_SIN_PASO_BASE",
    "TS_AUTONOMIA",

    # Control
    "TS_ULTIMO_EVENTO"
)



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
      .filter(es_paso_base)  # ya definido mas arriba
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
    F.when(rol_regla == "GERENTE", F.lit("U17293"))
     .when(rol_regla == "SUPERVISOR", F.col("MATSUPERIOR_ANALISTA"))
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
      .withColumn("ESTADOSOLICITUD_INICIAL", F.col("ESTADOSOLICITUD_ULTIMO")) 
      .withColumn(
          "ESTADOSOLICITUD_ULTIMO",
          F.when(cond_incoherencia, F.lit("RECHAZADO"))
           .otherwise(F.col("ESTADOSOLICITUD_ULTIMO"))
      )
)


df_final_solicitud.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("CATALOG_LHCL_PROD_BCP_EXPL.BCP_EDV_RBMBDN.TP_SOLICITUDES_CENTRALIZADO")
