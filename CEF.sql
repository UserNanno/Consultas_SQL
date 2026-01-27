from pyspark.sql import functions as F
from pyspark.sql.window import Window

# ------------------------------------------------------------------------------
# NUEVO: Snapshot "decisión analista" + timestamps de auditoría
# ------------------------------------------------------------------------------
def build_estado_analista_snapshot(df_estados_enriq):
    """
    Devuelve 1 fila por CODSOLICITUD con:
    - ESTADOSOLICITUDANALISTA: última decisión del analista (APROBADO/RECHAZADO);
      si nunca hubo decisión (solo PENDIENTE) => PENDIENTE
    - TS_DECISION_ANALISTA: timestamp del evento (inicio del paso) donde ocurrió esa decisión (si existe)
    - TS_ULTIMO_EVENTO_PASO_ANALISTA: timestamp del último evento del paso analista (incluye PENDIENTE)
    """

    # 1) Definir "paso de analista" (misma lógica que atribución)
    is_tc  = F.col("PROCESO").like("%APROBACION CREDITOS TC%")
    is_cef = F.col("PROCESO").isin(
        "CO SOLICITUD APROBACIONES TLMK",
        "SFCP APROBACIONES EDUCATIVO",
        "CO SOLICITUD APROBACIONES"
    )

    paso_tc_analista  = (F.col("NBRPASO") == "APROBACION DE CREDITOS ANALISTA")
    paso_cef_analista = (F.col("NBRPASO") == "EVALUACION DE SOLICITUD")

    es_paso_analista = (is_tc & paso_tc_analista) | (is_cef & paso_cef_analista)

    df_paso = df_estados_enriq.filter(es_paso_analista)

    # 2) Último evento del paso analista (incluye PENDIENTE)
    w_last_evt = Window.partitionBy("CODSOLICITUD").orderBy(
        F.col("FECHORINICIOEVALUACION").desc_nulls_last(),
        F.col("FECHORFINEVALUACION").desc_nulls_last()
    )

    df_last_evt = (
        df_paso
        .withColumn("rn_evt", F.row_number().over(w_last_evt))
        .filter(F.col("rn_evt") == 1)
        .select(
            "CODSOLICITUD",
            F.col("FECHORINICIOEVALUACION").alias("TS_ULTIMO_EVENTO_PASO_ANALISTA")
        )
    )

    # 3) Última decisión (APROBADO/RECHAZADO) ignorando PENDIENTE
    df_decisiones = df_paso.filter(F.col("ESTADOSOLICITUDPASO").isin("APROBADO", "RECHAZADO"))

    w_last_dec = Window.partitionBy("CODSOLICITUD").orderBy(
        F.col("FECHORINICIOEVALUACION").desc_nulls_last(),
        F.col("FECHORFINEVALUACION").desc_nulls_last()
    )

    df_last_dec = (
        df_decisiones
        .withColumn("rn_dec", F.row_number().over(w_last_dec))
        .filter(F.col("rn_dec") == 1)
        .select(
            "CODSOLICITUD",
            F.col("ESTADOSOLICITUDPASO").alias("ESTADOSOLICITUDANALISTA_DEC"),
            F.col("FECHORINICIOEVALUACION").alias("TS_DECISION_ANALISTA")
        )
    )

    # 4) Ensamble final:
    # - Si hay decisión => usa esa
    # - Si no hay decisión pero sí hay eventos del paso => PENDIENTE
    # - Si no hay ni siquiera eventos del paso => null
    out = (
        df_last_evt
        .join(df_last_dec, on="CODSOLICITUD", how="left")
        .withColumn(
            "ESTADOSOLICITUDANALISTA",
            F.coalesce(F.col("ESTADOSOLICITUDANALISTA_DEC"), F.lit("PENDIENTE"))
        )
        .drop("ESTADOSOLICITUDANALISTA_DEC")
    )

    return out


# ------------------------------------------------------------------------------
# INTEGRACIÓN en tu pipeline (mínimos cambios)
# ------------------------------------------------------------------------------

# ... (hasta tener df_estados_enriq listo)
# 12.2 Enriquecimiento con orgánico (matrículas)
df_estados_enriq   = enrich_estados_con_organico(df_estados, df_org_tokens)
df_productos_enriq = enrich_productos_con_organico(df_productos, df_org_tokens)

# NUEVO: snapshot de decisión analista + TS
df_estado_analista = build_estado_analista_snapshot(df_estados_enriq)

# 12.3 Snapshots (1 fila por solicitud)
df_last_estado = build_last_estado_snapshot(df_estados_enriq)
df_prod_snap   = build_productos_snapshot(df_productos_enriq)

# 12.3.1 (opcional pero recomendado): adjuntar campos analista al snapshot base
df_last_estado = df_last_estado.join(df_estado_analista, on="CODSOLICITUD", how="left")

# 12.4 Atribución analista final + origen (MAT1/MAT2/MAT3/MAT4)
df_matanalista = build_matanalista_final(df_estados_enriq, df_prod_snap)

# 12.5 Ensamble final (base = último estado)
df_final = (
    df_last_estado
    .join(df_matanalista, on="CODSOLICITUD", how="left")
    .join(df_prod_snap.select(
        "CODSOLICITUD",
        "NBRPRODUCTO","ETAPA","TIPACCION","NBRDIVISA",
        "MTOSOLICITADO","MTOAPROBADO","MTOOFERTADO","MTODESEMBOLSADO",
        "TS_PRODUCTOS"
    ), on="CODSOLICITUD", how="left")
)

# ... resto igual ...

# 12.10 Selección final: AGREGA LOS NUEVOS CAMPOS
df_final = df_final.select(
    "CODSOLICITUD",
    "PRODUCTO",
    "CODMESEVALUACION",

    "TS_BASE_ESTADOS",
    "MATANALISTA_FINAL",
    "ORIGEN_MATANALISTA",
    "MATSUPERIOR",

    "PROCESO",
    "ESTADOSOLICITUD",
    "ESTADOSOLICITUDPASO",
    "TS_ULTIMO_EVENTO_ESTADOS",
    "TS_FIN_ULTIMO_EVENTO_ESTADOS",
    "FECINICIOEVALUACION_ULT",
    "FECFINEVALUACION_ULT",

    # NUEVOS:
    "ESTADOSOLICITUDANALISTA",
    "TS_DECISION_ANALISTA",
    "TS_ULTIMO_EVENTO_PASO_ANALISTA",

    "NBRPRODUCTO",
    "ETAPA",
    "TIPACCION",
    "NBRDIVISA",
    "MTOSOLICITADO",
    "MTOAPROBADO",
    "MTOOFERTADO",
    "MTODESEMBOLSADO",
    "TS_PRODUCTOS",

    "MOTIVORESULTADOANALISTA",
    "MOTIVOMALADERIVACION",
    "SUBMOTIVOMALADERIVACION",
)
