def add_matsuperior_from_organico(df_final, df_org):
    df_org_key = (
        df_org
        .select(
            F.col("CODMES").cast("string").alias("CODMESEVALUACION"),
            F.col("MATORGANICO").alias("MATANALISTA_FINAL"),
            F.col("MATSUPERIOR").alias("MATSUPERIOR_ORG")
        )
        .dropDuplicates(["CODMESEVALUACION", "MATANALISTA_FINAL"])
    )

    df_out = (
        df_final
        .join(df_org_key, on=["CODMESEVALUACION", "MATANALISTA_FINAL"], how="left")
        .withColumn("MATSUPERIOR", F.coalesce(F.col("MATSUPERIOR"), F.col("MATSUPERIOR_ORG")))
        .drop("MATSUPERIOR_ORG")
    )
    return df_out



df_matanalista = df_matanalista.select(
    "CODSOLICITUD", "TS_BASE_ESTADOS", "MATANALISTA_FINAL", "ORIGEN_MATANALISTA"
)
