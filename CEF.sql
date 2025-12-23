def match_persona_vs_organico(df_org_tokens, df_sf, codmes_sf_col, codsol_col, nombre_sf_col,
                              min_tokens=3, min_ratio_sf=0.60):

    df_sf_nombres = (
        df_sf
        .withColumn("TOKENS_NOMBRE_SF", F.array_distinct(F.split(F.col(nombre_sf_col), r"\s+")))
        .withColumn("N_TOK_SF", F.size("TOKENS_NOMBRE_SF"))
    )

    df_sf_tokens = (
        df_sf_nombres
        .select(codmes_sf_col, codsol_col, nombre_sf_col, "TOKENS_NOMBRE_SF", "N_TOK_SF")
        .withColumn("TOKEN", F.explode("TOKENS_NOMBRE_SF"))
    )

    df_join = (
        df_sf_tokens.alias("sf")
        .join(
            df_org_tokens.alias("org"),
            (F.col(f"sf.{codmes_sf_col}") == F.col("org.CODMES")) &
            (F.col("sf.TOKEN") == F.col("org.TOKEN")),
            "inner"
        )
    )

    match_cols = [codmes_sf_col, codsol_col, nombre_sf_col, "MATORGANICO", "MATSUPERIOR"]

    df_scores = (
        df_join
        .groupBy(match_cols)
        .agg(
            F.countDistinct("sf.TOKEN").alias("TOKENS_MATCH"),
            F.first("sf.N_TOK_SF").alias("N_TOK_SF"),
            F.first("org.N_TOK_ORG").alias("N_TOK_ORG"),
        )
        .withColumn("RATIO_SF", F.col("TOKENS_MATCH") / F.col("N_TOK_SF"))
        .withColumn("RATIO_ORG", F.col("TOKENS_MATCH") / F.col("N_TOK_ORG"))
        .filter((F.col("TOKENS_MATCH") >= min_tokens) & (F.col("RATIO_SF") >= min_ratio_sf))
    )

    # ✅ clave: best match por NOMBRE (no por solicitud)
    w = Window.partitionBy(codmes_sf_col, codsol_col, nombre_sf_col).orderBy(
        F.col("TOKENS_MATCH").desc(),
        F.col("RATIO_SF").desc(),
        F.col("RATIO_ORG").desc(),
        F.col("MATORGANICO").asc()
    )

    return (
        df_scores
        .withColumn("rn", F.row_number().over(w))
        .filter(F.col("rn") == 1)
        .drop("rn")
    )










def enrich_estados_con_organico(df_estados, df_org_tokens):
    bm_actor = match_persona_vs_organico(
        df_org_tokens=df_org_tokens,
        df_sf=df_estados,
        codmes_sf_col="CODMESEVALUACION",
        codsol_col="CODSOLICITUD",
        nombre_sf_col="NBRULTACTOR",
        min_tokens=3,
        min_ratio_sf=0.60
    )

    bm_paso = match_persona_vs_organico(
        df_org_tokens=df_org_tokens,
        df_sf=df_estados,
        codmes_sf_col="CODMESEVALUACION",
        codsol_col="CODSOLICITUD",
        nombre_sf_col="NBRULTACTORPASO",
        min_tokens=3,
        min_ratio_sf=0.60
    )

    df_enriq = (
        df_estados
        # ✅ join incluyendo el nombre del actor
        .join(
            bm_actor.select(
                "CODMESEVALUACION", "CODSOLICITUD", "NBRULTACTOR",
                "MATORGANICO", "MATSUPERIOR"
            ),
            on=["CODMESEVALUACION", "CODSOLICITUD", "NBRULTACTOR"],
            how="left"
        )
        # ✅ join incluyendo el nombre del actor del paso
        .join(
            bm_paso.select(
                "CODMESEVALUACION", "CODSOLICITUD", "NBRULTACTORPASO",
                F.col("MATORGANICO").alias("MATORGANICOPASO"),
                F.col("MATSUPERIOR").alias("MATSUPERIORPASO"),
            ),
            on=["CODMESEVALUACION", "CODSOLICITUD", "NBRULTACTORPASO"],
            how="left"
        )
    )

    return df_enriq
