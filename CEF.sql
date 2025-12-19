PATH_PA_SOLICITUDES = "abfss://bcp-edv-rbmbdn@adlscu1lhclbackp05.dfs.core.windows.net/T72496/CARGA/POWERAPPS/BASESOLICITUDES/POWERAPP_EDV.csv"

df_powerapp = (
    spark.read.format("csv")
      .option("header", "true")
      .option("sep", ";")
      .option("encoding", "utf-8")
      .option("ignoreLeadingWhiteSpace", "true")
      .option("ignoreTrailingWhiteSpace", "true")
      .load(PATH_PA_SOLICITUDES)
      .select(
          F.col("CODMES").cast("string").alias("CODMES_APPS"),
          "CODSOLICITUD",
          "FECHORACREACION",
          "FECCREACION",
          "HORACREACION",
          "MATANALISTA",
          "FECASIGNACION",
          "PRODUCTO",
          "RESULTADOANALISTA",
          "MOTIVORESULTADOANALISTA",
          "MOTIVOMALADERIVACION",
          "SUBMOTIVOMALADERIVACION"
      )
)

# Por seguridad: 1 fila por CODSOLICITUD (elige el más reciente por FECHORACREACION si viene)
w_pa = Window.partitionBy("CODSOLICITUD").orderBy(F.col("FECHORACREACION").desc_nulls_last())
df_powerapp = (
    df_powerapp
      .withColumn("rn", F.row_number().over(w_pa))
      .filter(F.col("rn") == 1)
      .drop("rn")
)




# =========================================================
# AJUSTE 4: Join PowerApps EDV (fallback + malas derivadas)
# =========================================================

df_final_solicitud = (
    df_final_solicitud
      .join(
          df_powerapp.select(
              "CODSOLICITUD",
              F.col("MATANALISTA").alias("MATANALISTA_APPS"),
              F.col("RESULTADOANALISTA").alias("RESULTADOANALISTA_APPS"),
              F.col("PRODUCTO").alias("PRODUCTO_APPS"),
              "MOTIVORESULTADOANALISTA",
              "MOTIVOMALADERIVACION",
              "SUBMOTIVOMALADERIVACION"
          ),
          on="CODSOLICITUD",
          how="left"
      )
      # fallback SOLO si SF es nulo
      .withColumn("MAT_ANALISTA_FINAL", F.coalesce(F.col("MAT_ANALISTA_FINAL"), F.col("MATANALISTA_APPS")))
      .withColumn("ESTADOSOLICITUD_ULTIMO", F.coalesce(F.col("ESTADOSOLICITUD_ULTIMO"), F.col("RESULTADOANALISTA_APPS")))
      .withColumn("NBRPRODUCTO", F.coalesce(F.col("NBRPRODUCTO"), F.col("PRODUCTO_APPS")))
      .withColumn(
          "ORIGEN_MAT_ANALISTA",
          F.when(F.col("ORIGEN_MAT_ANALISTA").isNull() & F.col("MATANALISTA_APPS").isNotNull(), F.lit("APPS_MAT"))
           .otherwise(F.col("ORIGEN_MAT_ANALISTA"))
      )
      .drop("MATANALISTA_APPS", "RESULTADOANALISTA_APPS", "PRODUCTO_APPS")
)




df_final_solicitud = df_final_solicitud.select(
    ...,
    "MOTIVORESULTADOANALISTA",
    "MOTIVOMALADERIVACION",
    "SUBMOTIVOMALADERIVACION",
    "ESTADOSOLICITUD_INICIAL",
    ...
)
