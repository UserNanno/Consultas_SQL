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

# =========================================================
# ✅ NUEVO: Limpiar y validar CODSOLICITUD
# =========================================================
df_salesforce_productos = (
    df_salesforce_productos
        .withColumn("CODSOLICITUD_CLEAN", F.trim(F.col("CODSOLICITUD").cast("string")))
        .withColumn(
            "FLG_CODSOLICITUD_VALIDO",
            F.when(
                (F.length("CODSOLICITUD_CLEAN") == 11) &
                (F.col("CODSOLICITUD_CLEAN").startswith("O00")),
                F.lit(1)
            ).otherwise(F.lit(0))
        )
)

df_salesforce_productos_validos = df_salesforce_productos.filter(F.col("FLG_CODSOLICITUD_VALIDO") == 1)
df_salesforce_productos_invalidos = df_salesforce_productos.filter(F.col("FLG_CODSOLICITUD_VALIDO") == 0)

print("Productos validos:", df_salesforce_productos_validos.count())
print("Productos invalidos:", df_salesforce_productos_invalidos.count())

# Si quieres, quedarte ya con CODSOLICITUD limpio:
df_salesforce_productos_validos = (
    df_salesforce_productos_validos
        .drop("CODSOLICITUD")
        .withColumnRenamed("CODSOLICITUD_CLEAN", "CODSOLICITUD")
)

# =========================================================
# Normalización texto (solo en válidos)
# =========================================================
df_salesforce_productos_validos = (
    df_salesforce_productos_validos
        .withColumn("NBRPRODUCTO", norm_txt_spark("NBRPRODUCTO"))
        .withColumn("ETAPA", norm_txt_spark("ETAPA"))
        .withColumn("NBRANALISTA", norm_txt_spark("NBRANALISTA"))
        .withColumn("TIPACCION", norm_txt_spark("TIPACCION"))
)

# =========================================================
# Parseo fecha y CODMESCREACION (solo en válidos)
# =========================================================
df_salesforce_productos_validos = (
    df_salesforce_productos_validos
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

df_salesforce_productos_validos = df_salesforce_productos_validos.withColumn(
    "CODMESCREACION", F.col("CODMESCREACION").cast("string")
)

print("Registros SF productos (validos):", df_salesforce_productos_validos.count())
