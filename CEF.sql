df_best_match_analista = match_persona_vs_organico(
    df_org_tokens=df_org_tokens,
    df_sf=df_salesforce_productos,
    codmes_sf_col="CODMESCREACION",
    codsol_col="CODSOLICITUD",
    nombre_sf_col="NBRANALISTA",
    min_tokens=3,
    min_ratio_sf=0.60
)

df_productos_enriq = (
    df_salesforce_productos
      .join(
          df_best_match_analista.select(
              "CODMESCREACION",
              "CODSOLICITUD",
              F.col("MATORGANICO").alias("MATORGANICO_ANALISTA"),
              F.col("MATSUPERIOR").alias("MATSUPERIOR_ANALISTA")
          ),
          on=["CODMESCREACION", "CODSOLICITUD"],
          how="left"
      )
)

print("Registros enriquecidos productos:", df_productos_enriq.count())
