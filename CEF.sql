# FECHORACREACION_TS = timestamp real (para lógica)
df = df.withColumn("FECHORACREACION_TS", F.date_trunc("minute", F.col("CREATED_LIMA")))

# FECHORACREACION = string bonito para export
df = df.withColumn("FECHORACREACION", F.date_format(F.col("FECHORACREACION_TS"), "yyyy-MM-dd HH:mm:ss"))

# FECCREACION/HORACREACION desde el TS
df = (
    df.withColumn("FECCREACION", F.to_date("FECHORACREACION_TS"))
      .withColumn("HORACREACION", F.date_format("FECHORACREACION_TS", "HH:mm:ss"))
)


pick = [c for c in ["CODMES","CORREO","MATORGANICO","MATSUPERIOR"] if c in cols]

df_apps_enriq = (
    df_apps_key.alias("a")
    .join(df_org.alias("o"), on=["CODMES","CORREO"], how="left")
    .withColumnRenamed("MATORGANICO", "MATANALISTA")
)

cols_final = [
    "CODMES",
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
    "SUBMOTIVOMALADERIVACION",
]

df_apps_final = df_apps_enriq.select(*cols_final)
