path_salesfoce_productos = "abfss://bcp-edv-rbmbdn@adlscu1lhclbackp05.dfs.core.windows.net/T72496/CARGA/SALESFORCE/INFORME_PRODUCTO/INFORME_PRODUCTO_*.csv"


df_salesforce_raw = (
    spark.read
        .format("csv")
        .option("header", "true")
        .option("encoding", "ISO-8859-1")
        .load(path_salesfoce_productos)
)


df_salesforce_productos = (
    df_salesforce_raw
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
        .withColumn("NBRPRODUCTO", F.upper(F.col("NBRPRODUCTO")))
        .withColumn("ETAPA", F.upper(F.col("ETAPA")))
        .withColumn("NBRANALISTA", F.upper(F.col("NBRANALISTA")))
        .withColumn("TIPACCION", F.upper(F.col("TIPACCION")))
)



df_salesforce_productos = (
    df_salesforce_productos
        .withColumn("NBRPRODUCTO", quitar_tildes("NBRPRODUCTO"))
        .withColumn("ETAPA", quitar_tildes("ETAPA"))
        .withColumn("NBRANALISTA", quitar_tildes("NBRANALISTA"))
        .withColumn("TIPACCION", quitar_tildes("TIPACCION"))
)

df_salesforce_productos = (
    df_salesforce_productos
        .withColumn("CODMESCREACION", F.date_format("FECCREACION", "yyyyMM"))
)
