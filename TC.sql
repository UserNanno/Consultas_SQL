entries = dbutils.fs.ls(BASE_DIR_ORGANICO)
files = [e.path for e in entries if e.name.startswith("1n_Activos_2025") and e.name.endswith(".xlsx")]
assert files, "No se encontraron archivos 1n_Activos_2025*.xlsx"

excel_options = {
    "header": "true",
    "inferSchema": "true",
    "treatEmptyValuesAsNulls": "true",
    "timestampFormat": "yyyy-MM-dd HH:mm:ss",
}

dfs_org = []
for path in files:
    reader = spark.read.format("com.crealytics.spark.excel")
    for k, v in excel_options.items():
        reader = reader.option(k, v)
    df_tmp = reader.load(path)

    for c in df_tmp.columns:
        new_name = re.sub(r"\s+", " ", c.strip())
        if new_name != c:
            df_tmp = df_tmp.withColumnRenamed(c, new_name)

    cols_clean = [c for c in df_tmp.columns if not re.match(r"^_c\d+$", c)]
    df_tmp = df_tmp.select(*cols_clean)

    dfs_org.append(df_tmp)

df_organico_raw = reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), dfs_org)

df_organico = (
    df_organico_raw
        .select(
            F.col("CODMES").alias("CODMES"),
            F.col("Matrícula").alias("MATORGANICO"),
            F.col("Nombre Completo").alias("NOMBRECOMPLETO"),
            F.col("Correo electronico").alias("CORREO"),
            F.col("Fecha Ingreso").alias("FECINGRESO"),
            F.col("Matrícula Superior").alias("MATSUPERIOR"),
        )
)

for c in ["MATORGANICO", "NOMBRECOMPLETO", "MATSUPERIOR"]:
    df_organico = df_organico.withColumn(c, norm_txt_spark(c))

df_organico = df_organico.withColumn(
    "MATSUPERIOR",
    F.regexp_replace(F.col("MATSUPERIOR"), r'^0(?=[A-Z]\d{5})', '')
)

df_organico = df_organico.withColumn("FECINGRESO", F.to_date("FECINGRESO"))
df_organico = df_organico.withColumn("NOMBRECOMPLETO_CLEAN", limpiar_cesado("NOMBRECOMPLETO"))
df_organico = df_organico.withColumn("CODMES", F.col("CODMES").cast("string"))

# Tokenización orgánico (para todos los matchings)
df_org_nombres = (
    df_organico
      .withColumn(
          "TOKENS_NOMBRE_ORG",
          F.array_distinct(F.split(F.col("NOMBRECOMPLETO"), r"\s+"))
      )
      .withColumn("N_TOK_ORG", F.size("TOKENS_NOMBRE_ORG"))
)

df_org_tokens = (
    df_org_nombres
      .select(
          "CODMES",
          "MATORGANICO",
          "MATSUPERIOR",
          "NOMBRECOMPLETO",
          "TOKENS_NOMBRE_ORG",
          "N_TOK_ORG"
      )
      .withColumn("TOKEN", F.explode("TOKENS_NOMBRE_ORG"))
)

print("Organico listo:", df_organico.count())
