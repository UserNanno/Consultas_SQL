df_sf_productos_raw = (
    spark.read
        .format("csv")
        .option("header", "true")
        .option("encoding", "ISO-8859-1")
        .load(PATH_SF_PRODUCTOS)
)

df_salesforce_productos = (
    df_sf_productos_raw
        .select(
            F.col("Nombre de la oportunidad").alias("CODSOLICITUD"),
            F.col("Nombre del Producto").alias("NBRPRODUCTO"),
            F.col("Etapa").alias("ETAPA"),
            F.col("Analista de crédito").alias("NBRANALISTA"),
            F.col("Tipo de Acción").alias("TIPACCION"),
            F.col("Fecha de creación").alias("FECCREACION")
        )
)

df_salesforce_productos = (
    df_salesforce_productos
        .withColumn("NBRPRODUCTO", norm_txt_spark("NBRPRODUCTO"))
        .withColumn("ETAPA", norm_txt_spark("ETAPA"))
        .withColumn("NBRANALISTA", norm_txt_spark("NBRANALISTA"))
        .withColumn("TIPACCION", norm_txt_spark("TIPACCION"))
)

df_salesforce_productos = (
    df_salesforce_productos
        .withColumn("FECCREACION_STR", F.trim(F.col("FECCREACION").cast("string")))
        .withColumn(
            "FECCREACION_DATE",
            F.coalesce(
                F.to_date("FECCREACION_STR", "dd/MM/yyyy"),
                F.to_date("FECCREACION_STR", "yyyy-MM-dd"),
                F.to_date("FECCREACION_STR")
            )
        )
        .withColumn("CODMESCREACION", F.date_format("FECCREACION_DATE", "yyyyMM"))
        .withColumn("FECCREACION", F.col("FECCREACION_DATE"))
        .drop("FECCREACION_STR", "FECCREACION_DATE")
)

df_salesforce_productos = df_salesforce_productos.withColumn(
    "CODMESCREACION", F.col("CODMESCREACION").cast("string")
)

print("Registros originales SF productos:", df_salesforce_productos.count())
