# =========================================================
# CORRECCION AUTONOMIA POR MONTO APROBADO (REGLA FINAL)
# =========================================================
def parse_monto_to_double(col):
    """
    Convierte montos tipo '123,456.78' o '123.456,78' o '123456' a double de forma robusta.
    Si ya viene numérico, Spark lo castea.
    """
    s = F.trim(col.cast("string"))
    # Quitar espacios
    s = F.regexp_replace(s, r"\s+", "")
    # Mantener solo dígitos, coma y punto y signo
    s = F.regexp_replace(s, r"[^0-9,\.\-]", "")
    # Caso común en data LATAM: separador de miles '.' y decimal ',' => quitar miles y cambiar ',' por '.'
    # Ej: 1.234.567,89 -> 1234567.89
    s_latam = F.regexp_replace(s, r"\.", "")
    s_latam = F.regexp_replace(s_latam, r",", ".")
    # Caso US: separador de miles ',' y decimal '.' => quitar miles ','
    # Ej: 1,234,567.89 -> 1234567.89
    s_us = F.regexp_replace(s, r",", "")

    return F.coalesce(s_latam.cast("double"), s_us.cast("double"), s.cast("double"))


def apply_autonomia_monto_correction(df_final):
    """
    Regla (según memoria acordada):
    - Solo si la autonomía quedó en nivel ANALISTA.
    - Si MTOAPROBADO > 240000 => autonomía pasa a GERENTE (U17293).
    - Si MTOAPROBADO > 100000 => autonomía pasa a SUPERVISOR (MATSUPERIOR del MATANALISTA_FINAL).
    - Se aplica al final del pipeline, con producto/orgánico ya enriquecidos.
    """
    mto = parse_monto_to_double(F.col("MTOAPROBADO"))

    es_analista_aut = (F.col("NIVELAUTONOMIA") == "ANALISTA") | (rol_actor(F.col("MATAUTONOMIA")) == "ANALISTA")

    df_out = (
        df_final
        .withColumn("MTOAPROBADO_NUM", mto)
        .withColumn(
            "MATAUTONOMIA",
            F.when(es_analista_aut & (F.col("MTOAPROBADO_NUM") > F.lit(240000)), F.lit("U17293"))
             .when(es_analista_aut & (F.col("MTOAPROBADO_NUM") > F.lit(100000)), F.col("MATSUPERIOR"))
             .otherwise(F.col("MATAUTONOMIA"))
        )
        .withColumn(
            "NIVELAUTONOMIA",
            F.when(es_analista_aut & (F.col("MTOAPROBADO_NUM") > F.lit(240000)), F.lit("GERENTE"))
             .when(es_analista_aut & (F.col("MTOAPROBADO_NUM") > F.lit(100000)), F.lit("SUPERVISOR"))
             .otherwise(F.col("NIVELAUTONOMIA"))
        )
        .withColumn(
            "PASO_AUTONOMIA",
            F.when(es_analista_aut & (F.col("MTOAPROBADO_NUM") > F.lit(100000)), F.lit("REGLA_MONTO_APROBADO"))
             .otherwise(F.col("PASO_AUTONOMIA"))
        )
        # opcional: marcar timestamp si quieres trazabilidad (si no, déjalo como estaba)
        .withColumn(
            "TS_AUTONOMIA",
            F.when(es_analista_aut & (F.col("MTOAPROBADO_NUM") > F.lit(100000)) & F.col("TS_AUTONOMIA").isNull(), F.current_timestamp())
             .otherwise(F.col("TS_AUTONOMIA"))
        )
        .drop("MTOAPROBADO_NUM")
    )

    return df_out





# 8) PRODUCTO (TC/CEF) + MATSUPERIOR (desde orgánico usando MATANALISTA_FINAL + mes)
df_final = add_producto_tipo(df_final)
df_final = add_matsuperior_from_organico(df_final, df_org)

# 8.1) CORRECCION FINAL AUTONOMIA POR MONTO APROBADO
df_final = apply_autonomia_monto_correction(df_final)

# 9) POWERAPPS (fallback SOLO si sigue NULL) + motivos
df_final = apply_powerapps_fallback(df_final, df_apps)
