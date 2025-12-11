df_salesforce_productos = (
    df_salesforce_raw
        .select(
            F.col("Nombre de la oportunidad").alias("CODOPORTUNIDAD"),
            F.col("Nombre del Producto").alias("NBRPRODUCTO"),
            F.col("Etapa").alias("ETAPA"),
            F.col("Analista de crédito").alias("NBRANALISTA"),
            F.col("Tipo de Acción").alias("TIPACCION"),
            F.col("Fecha de creación").alias("FECCREACION")
        )
)

# 1) Normalizar textos (NO tocar FECCREACION aquí)
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

# 2) Parsear fecha de creación (similar a como hicimos con estados)
df_salesforce_productos = (
    df_salesforce_productos
        .withColumn("FECHORCREACION", parse_fecha_hora_esp_col("FECCREACION"))
        .withColumn("FECCREACION", F.to_date("FECHORCREACION"))
        .withColumn("CODMESCREACION", F.date_format("FECCREACION", "yyyyMM"))
)
