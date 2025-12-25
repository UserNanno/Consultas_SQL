def load_sf_productos_validos(spark, path_productos):
    df_raw = (
        spark.read.format("csv")
        .option("header", "true")
        .option("encoding", "ISO-8859-1")
        .load(path_productos)
    )

    df = df_raw.select(
        F.col("Nombre de la oportunidad").alias("CODSOLICITUD"),
        F.col("Nombre del Producto").alias("NBRPRODUCTO"),
        F.col("Etapa").alias("ETAPA"),
        F.col("Analista").alias("NBRANALISTA"),  # (MAT3)
        F.col("Analista de crédito").alias("NBRANALISTAASIGNADO"),  # (MAT4) <-- NUEVO
        F.col("Tipo de Acción").alias("TIPACCION"),
        F.col("Fecha de creación").alias("FECCREACION"),
        F.col("Divisa de la oportunidad").alias("NBRDIVISA"),
        F.col("Monto/Línea Solicitud").alias("MTOSOLICITADO"),
        F.col("Monto/Línea aprobada").alias("MTOAPROBADO"),
        F.col("Monto Solicitado/Ofertado").alias("MTOOFERTADO"),
        F.col("Monto desembolsado").alias("MTODESEMBOLSADO"),
        F.col("Centralizado/Punto de Contacto").alias("CENTROATENCION")
    )

    df = df.withColumn("CODSOLICITUD", F.trim(F.col("CODSOLICITUD").cast("string")))

    df = df.withColumn(
        "FLG_CODSOLICITUD_VALIDO",
        F.when((F.length("CODSOLICITUD") == 11) & (F.col("CODSOLICITUD").startswith("O00")), 1).otherwise(0)
    )

    df = df.filter(F.col("FLG_CODSOLICITUD_VALIDO") == 1).drop("FLG_CODSOLICITUD_VALIDO")

    # Normaliza también el nuevo campo
    df = norm_col(df, ["NBRPRODUCTO","ETAPA","NBRANALISTA","NBRANALISTAASIGNADO","TIPACCION","NBRDIVISA","CENTROATENCION"])

    df = (
        df.withColumn("FECCREACION_STR", F.trim(F.col("FECCREACION").cast("string")))
          .withColumn(
              "FECCREACION_DATE",
              F.coalesce(
                  F.to_date("FECCREACION_STR", "dd/MM/yyyy"),
                  F.to_date("FECCREACION_STR", "yyyy-MM-dd"),
                  F.to_date("FECCREACION_STR")
              )
          )
          .withColumn("FECCREACION", F.col("FECCREACION_DATE"))
          .withColumn("CODMESCREACION", F.date_format("FECCREACION", "yyyyMM").cast("string"))
          .drop("FECCREACION_STR","FECCREACION_DATE")
    )

    return df













def enrich_productos_con_organico(df_productos, df_org_tokens):
    # MAT3: por NBRANALISTA
    bm_analista = match_persona_vs_organico(
        df_org_tokens=df_org_tokens,
        df_sf=df_productos,
        codmes_sf_col="CODMESCREACION",
        codsol_col="CODSOLICITUD",
        nombre_sf_col="NBRANALISTA",
        min_tokens=3,
        min_ratio_sf=0.60
    )

    # MAT4: por NBRANALISTAASIGNADO
    bm_asignado = match_persona_vs_organico(
        df_org_tokens=df_org_tokens,
        df_sf=df_productos,
        codmes_sf_col="CODMESCREACION",
        codsol_col="CODSOLICITUD",
        nombre_sf_col="NBRANALISTAASIGNADO",
        min_tokens=3,
        min_ratio_sf=0.60
    )

    df_enriq = (
        df_productos
        .join(
            bm_analista.select(
                "CODMESCREACION","CODSOLICITUD",
                F.col("MATORGANICO").alias("MATORGANICO_ANALISTA"),      # MAT3
                F.col("MATSUPERIOR").alias("MATSUPERIOR_ANALISTA")
            ),
            on=["CODMESCREACION","CODSOLICITUD"],
            how="left"
        )
        .join(
            bm_asignado.select(
                "CODMESCREACION","CODSOLICITUD",
                F.col("MATORGANICO").alias("MATORGANICO_ASIGNADO"),      # MAT4
                F.col("MATSUPERIOR").alias("MATSUPERIOR_ASIGNADO")
            ),
            on=["CODMESCREACION","CODSOLICITUD"],
            how="left"
        )
    )

    return df_enriq











# Snapshot productos 1 fila por CODSOLICITUD (para MAT3/MAT4)
w_prod_mat = Window.partitionBy("CODSOLICITUD").orderBy(F.col("FECCREACION").desc_nulls_last())

df_matanalista_productos = (
    df_productos_enriq
      .withColumn("rn", F.row_number().over(w_prod_mat))
      .filter(F.col("rn") == 1)
      .select(
          "CODSOLICITUD",
          F.col("MATORGANICO_ANALISTA").alias("MAT3_PRODUCTOS"),
          F.col("MATORGANICO_ASIGNADO").alias("MAT4_PRODUCTOS"),
      )
)

df_matanalista_final = (
    df_matanalista_estados
      .join(df_matanalista_productos, on="CODSOLICITUD", how="left")
      # Limpia MAT3/MAT4 si son sup/ger
      .withColumn("MAT3_OK", F.when(~es_sup_o_ger(F.col("MAT3_PRODUCTOS")), F.col("MAT3_PRODUCTOS")))
      .withColumn("MAT4_OK", F.when(~es_sup_o_ger(F.col("MAT4_PRODUCTOS")), F.col("MAT4_PRODUCTOS")))
      .withColumn(
          "MATANALISTA_FINAL",
          F.coalesce(
              F.col("MATANALISTA_ESTADOS"),   # MAT1/MAT2 ya filtrado por tu lógica
              F.col("MAT3_OK"),               # MAT3
              F.col("MAT4_OK")                # MAT4
          )
      )
      .withColumn(
          "ORIGEN_MATANALISTA",
          F.when(F.col("MATANALISTA_ESTADOS").isNotNull(), F.col("ORIGEN_MATANALISTA_ESTADOS"))
           .when(F.col("MAT3_OK").isNotNull(), F.lit("PRODUCTOS_MAT3_NBRANALISTA"))
           .when(F.col("MAT4_OK").isNotNull(), F.lit("PRODUCTOS_MAT4_NBRANALISTAASIGNADO"))
           .otherwise(F.lit(None))
      )
      .drop(
          "MATANALISTA_ESTADOS",
          "ORIGEN_MATANALISTA_ESTADOS",
          "MAT3_PRODUCTOS", "MAT4_PRODUCTOS",
          "MAT3_OK", "MAT4_OK"
      )
)
