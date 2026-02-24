def load_dicc_organico(spark, path_organico_csv):
    df = (
        spark.read.format("csv")
        .option("header", "true")
        .option("sep", ";")
        .option("encoding", "ISO-8859-1")  # si lo exportaste en utf-8 cámbialo
        .option("ignoreLeadingWhiteSpace", "true")
        .option("ignoreTrailingWhiteSpace", "true")
        .load(path_organico_csv)
        .select(
            F.col("CODMES").cast("string").alias("CODMES"),
            F.col("NOMBRE").alias("NOMBRE"),
            F.col("MATORGANICO").alias("MATORGANICO"),
            F.col("MATSUPERIOR").alias("MATSUPERIOR"),
            F.col("CORREO").alias("CORREO"),
            F.col("NBRCORTO").alias("NBRCORTO"),
            F.col("TOKENS_MATCH").alias("TOKENS_MATCH"),
            F.col("JACCARD").alias("JACCARD"),
        )
    )

    # Normaliza llaves
    df = (
        df.withColumn("CODMES", F.regexp_replace("CODMES", r"[^0-9]", ""))
          .withColumn("CODMES", F.when(F.length("CODMES") >= 6, F.substring("CODMES", 1, 6)).otherwise(F.col("CODMES")))
          .withColumn("NOMBRE", limpiar_cesado(norm_txt(F.col("NOMBRE"))))
    )

    # Dedup por (CODMES, NOMBRE) priorizando mejor match
    w = Window.partitionBy("CODMES", "NOMBRE").orderBy(
        F.col("TOKENS_MATCH").cast("int").desc_nulls_last(),
        F.col("JACCARD").cast("double").desc_nulls_last(),
        F.col("MATORGANICO").isNotNull().desc()
    )
    return df.withColumn("rn", F.row_number().over(w)).filter("rn=1").drop("rn")




def load_powerapps_export(spark, path_apps_export):
    df = (
        spark.read.format("csv")
        .option("header", "true")
        .option("sep", ";")
        .option("encoding", "utf-8")
        .option("ignoreLeadingWhiteSpace", "true")
        .option("ignoreTrailingWhiteSpace", "true")
        .load(path_apps_export)
        .select(
            F.col("CODMES").cast("string").alias("CODMES_APPS"),
            F.col("CODSOLICITUD"),
            F.col("FECHORACREACION"),
            F.col("MATANALISTA"),
            F.col("PRODUCTO"),
            F.col("RESULTADOANALISTA"),
            F.col("MOTIVORESULTADOANALISTA"),
            F.col("MOTIVOMALADERIVACION"),
            F.col("SUBMOTIVOMALADERIVACION"),
        )
    )

    df = (
        df.withColumn("CODSOLICITUD", F.trim(F.col("CODSOLICITUD").cast("string")))
          .withColumn("CODMES_APPS", F.regexp_replace("CODMES_APPS", r"[^0-9]", ""))
          .withColumn("CODMES_APPS", F.when(F.length("CODMES_APPS") >= 6, F.substring("CODMES_APPS", 1, 6)).otherwise(F.col("CODMES_APPS")))
    )

    df = norm_col(df, [
        "MATANALISTA", "PRODUCTO", "RESULTADOANALISTA",
        "MOTIVORESULTADOANALISTA", "MOTIVOMALADERIVACION", "SUBMOTIVOMALADERIVACION"
    ])

    # FECHORACREACION viene como 'yyyy-MM-dd HH:mm:ss'
    df = df.withColumn("TS_APPS", F.to_timestamp(F.col("FECHORACREACION"), "yyyy-MM-dd HH:mm:ss"))

    w = Window.partitionBy("CODSOLICITUD").orderBy(F.col("TS_APPS").desc_nulls_last())
    return df.withColumn("rn", F.row_number().over(w)).filter("rn=1").drop("rn")







def enrich_estados_con_dicc(df_estados, df_dicc):
    dicc = df_dicc.select(
        F.col("CODMES").alias("CODMESEVALUACION"),
        F.col("NOMBRE"),
        F.col("MATORGANICO"),
        F.col("MATSUPERIOR"),
    )

    df = (
        df_estados
        .withColumn("NBRULTACTOR", limpiar_cesado(F.col("NBRULTACTOR")))
        .withColumn("NBRULTACTORPASO", limpiar_cesado(F.col("NBRULTACTORPASO")))
    )

    df = df.join(
        dicc.withColumnRenamed("NOMBRE", "NBRULTACTOR")
            .withColumnRenamed("MATORGANICO", "MATORGANICO")
            .withColumnRenamed("MATSUPERIOR", "MATSUPERIOR"),
        on=["CODMESEVALUACION", "NBRULTACTOR"],
        how="left"
    )

    df = df.join(
        dicc.withColumnRenamed("NOMBRE", "NBRULTACTORPASO")
            .withColumnRenamed("MATORGANICO", "MATORGANICOPASO")
            .withColumnRenamed("MATSUPERIOR", "MATSUPERIORPASO"),
        on=["CODMESEVALUACION", "NBRULTACTORPASO"],
        how="left"
    )

    return df


def enrich_productos_con_dicc(df_productos, df_dicc):
    dicc = df_dicc.select(
        F.col("CODMES").alias("CODMESCREACION"),
        F.col("NOMBRE"),
        F.col("MATORGANICO"),
        F.col("MATSUPERIOR"),
    )

    df = (
        df_productos
        .withColumn("NBRANALISTA", limpiar_cesado(F.col("NBRANALISTA")))
        .withColumn("NBRANALISTAASIGNADO", limpiar_cesado(F.col("NBRANALISTAASIGNADO")))
    )

    df = df.join(
        dicc.withColumnRenamed("NOMBRE", "NBRANALISTA")
            .withColumnRenamed("MATORGANICO", "MATORGANICO_ANALISTA")
            .withColumnRenamed("MATSUPERIOR", "MATSUPERIOR_ANALISTA"),
        on=["CODMESCREACION", "NBRANALISTA"],
        how="left"
    )

    df = df.join(
        dicc.withColumnRenamed("NOMBRE", "NBRANALISTAASIGNADO")
            .withColumnRenamed("MATORGANICO", "MATORGANICO_ASIGNADO")
            .withColumnRenamed("MATSUPERIOR", "MATSUPERIOR_ASIGNADO"),
        on=["CODMESCREACION", "NBRANALISTAASIGNADO"],
        how="left"
    )

    return df



df_dicc     = load_dicc_organico(spark, BASE_DIR_ORGANICO)

df_estados   = load_sf_estados(spark, PATH_SF_ESTADOS)
df_productos = load_sf_productos_validos(spark, PATH_SF_PRODUCTOS)
df_apps      = load_powerapps_export(spark, PATH_PA_SOLICITUDES)

df_estados_enriq   = enrich_estados_con_dicc(df_estados, df_dicc)
df_atencion_analista = build_atencion_analista_primera_fmt(df_estados_enriq)
df_productos_enriq = enrich_productos_con_dicc(df_productos, df_dicc)




df_final = add_matsuperior_from_organico(df_final, df_dicc)
...
df_final = add_matsuperior_from_organico(df_final, df_dicc)
