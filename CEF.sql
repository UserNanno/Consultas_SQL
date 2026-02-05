def apply_powerapps_fallback(df_final, df_apps):
    df_apps_sel = (
        df_apps
        .select(
            "CODSOLICITUD",
            F.col("MATANALISTA").alias("MATANALISTA_APPS"),
            F.col("RESULTADOANALISTA").alias("RESULTADOANALISTA_APPS"),
            F.col("PRODUCTO").alias("PRODUCTO_APPS"),
            "MOTIVORESULTADOANALISTA",
            "MOTIVOMALADERIVACION",
            "SUBMOTIVOMALADERIVACION",
        )
        .dropDuplicates(["CODSOLICITUD"])
    )

    df_out = df_final.join(df_apps_sel, on="CODSOLICITUD", how="left")

    cond_ambos_no_nulos = F.col("MATANALISTA_FINAL").isNotNull() & F.col("MATANALISTA_APPS").isNotNull()
    cond_no_coinciden   = F.col("MATANALISTA_FINAL") != F.col("MATANALISTA_APPS")
    cond_override       = cond_ambos_no_nulos & cond_no_coinciden

    df_out = (
        df_out
        .withColumn(
            "MATANALISTA_FINAL",
            F.when(cond_override, F.col("MATANALISTA_APPS"))                       # override
             .otherwise(F.coalesce(F.col("MATANALISTA_FINAL"), F.col("MATANALISTA_APPS")))  # fallback nulos
        )
        .withColumn(
            "ORIGEN_MATANALISTA",
            F.when(cond_override, F.lit("APPS_OVERRIDE"))
             .when(F.col("ORIGEN_MATANALISTA").isNull() & F.col("MATANALISTA_APPS").isNotNull(), F.lit("APPS_MAT"))
             .otherwise(F.col("ORIGEN_MATANALISTA"))
        )
        .withColumn("ESTADOSOLICITUDPASO", F.coalesce(F.col("ESTADOSOLICITUDPASO"), F.col("RESULTADOANALISTA_APPS")))
        .withColumn("NBRPRODUCTO",        F.coalesce(F.col("NBRPRODUCTO"),        F.col("PRODUCTO_APPS")))
        .drop("MATANALISTA_APPS", "RESULTADOANALISTA_APPS", "PRODUCTO_APPS")
    )

    return df_out
