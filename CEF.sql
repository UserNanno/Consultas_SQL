# =========================================================
# UNIVERSO SOLO DE ESTADOS (FIX CLAVE)
# =========================================================
df_universo = df_estados_enriq.select("CODSOLICITUD").distinct()

df_final_autonomias = (
   df_universo
     .join(df_matanalista_final, on="CODSOLICITUD", how="left")
     .join(df_autonomia,        on="CODSOLICITUD", how="left")
     .withColumn("FLGAUTONOMIA", F.coalesce(F.col("FLGAUTONOMIA"), F.lit(0)))
     .withColumn("FLGAUTONOMIAOBSERVADA", F.coalesce(F.col("FLGAUTONOMIAOBSERVADA"), F.lit(0)))
)

# =========================================================
# SNAPSHOTS (como ya lo tienes)
# =========================================================
df_last_estado = build_last_estado_snapshot(df_estados_enriq)
df_prod_snap   = build_productos_snapshot(df_productos_enriq)

# =========================================================
# ENSAMBLE FINAL (ANCLADO A ESTADOS)
# =========================================================
df_final = (
    df_last_estado
      .join(df_final_autonomias, on="CODSOLICITUD", how="left")
      .join(df_prod_snap,        on="CODSOLICITUD", how="left")
)

# PRODUCTO = TC/CEF desde PROCESO
df_final = add_producto_tipo(df_final)

# MATSUPERIOR desde ORGÁNICO usando MATANALISTA_FINAL y CODMESEVALUACION
df_final = add_matsuperior_from_organico(df_final, df_org)

# =========================================================
# SELECT FINAL
# =========================================================
df_final = df_final.select(
    "CODSOLICITUD",
    "PRODUCTO",
    "CODMESEVALUACION",

    "TS_BASE_ESTADOS",
    "MATANALISTA_FINAL",
    "ORIGEN_MATANALISTA",
    "MATSUPERIOR",

    "FLGAUTONOMIA",
    "FLGAUTONOMIAOBSERVADA",
    "NIVELAUTONOMIA",
    "MATAUTONOMIA",
    "PASO_AUTONOMIA",
    "TS_AUTONOMIA",

    "PROCESO",
    "ESTADOSOLICITUD",
    "ESTADOSOLICITUDPASO",
    "TS_ULTIMO_EVENTO_ESTADOS",
    "FECINICIOEVALUACION_ULT",

    "NBRPRODUCTO",
    "ETAPA",
    "TIPACCION",
    "NBRDIVISA",
    "MTOSOLICITADO",
    "MTOAPROBADO",
    "MTOOFERTADO",
    "MTODESEMBOLSADO",
    "TS_PRODUCTOS",
)


# Solicitudes que NO tienen estados (debería dar 0)
df_final.filter(F.col("CODMESEVALUACION").isNull()).count()

