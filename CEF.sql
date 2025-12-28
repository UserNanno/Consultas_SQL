# 12.1 Staging
df_org       = load_organico(spark, BASE_DIR_ORGANICO)
df_org_tokens= build_org_tokens(df_org)

df_estados   = load_sf_estados(spark, PATH_SF_ESTADOS)
df_productos = load_sf_productos_validos(spark, PATH_SF_PRODUCTOS)
df_apps      = load_powerapps(spark, PATH_PA_SOLICITUDES)

# 12.2 Enriquecimiento con orgánico (matrículas)
df_estados_enriq   = enrich_estados_con_organico(df_estados, df_org_tokens)
df_productos_enriq = enrich_productos_con_organico(df_productos, df_org_tokens)

# 12.3 Snapshots (1 fila por solicitud)
df_last_estado = build_last_estado_snapshot(df_estados_enriq)
df_prod_snap   = build_productos_snapshot(df_productos_enriq)

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

# 12.6 Producto (TC/CEF)
df_final = add_producto_tipo(df_final)

# 12.7 MATSUPERIOR por orgánico (mes + matrícula)
df_final = add_matsuperior_from_organico(df_final, df_org)

# 12.8 PowerApps fallback + motivos
df_final = apply_powerapps_fallback(df_final, df_apps)

# 12.9 Re-llenar MATSUPERIOR por si PowerApps completó MATANALISTA_FINAL
df_final = add_matsuperior_from_organico(df_final, df_org)

# 12.10 Selección final (sin autonomías)
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





AnalysisException: [AMBIGUOUS_REFERENCE] Reference `MATSUPERIOR` is ambiguous, could be: [`MATSUPERIOR`, `MATSUPERIOR`].
