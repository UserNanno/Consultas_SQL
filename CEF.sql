df_organico_raw = (
    spark.read
        .format("com.crealytics.spark.excel")
        .option("header", "true")
        .option("inferSchema", "true")
        .load("abfss://.../1n_Activos_2025*.xlsx")  # tu ruta con wildcard
)




from pyspark.sql import functions as F

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


def norm_txt_spark(col):
    # upper + quitar tildes + trim
    return F.trim(quitar_tildes(F.upper(F.col(col))))

cols_norm = ["MATORGANICO", "NOMBRECOMPLETO", "MATSUPERIOR"]

for c in cols_norm:
    df_organico = df_organico.withColumn(c, norm_txt_spark(c))




df_organico = df_organico.withColumn(
    "MATSUPERIOR",
    F.regexp_replace(F.col("MATSUPERIOR"), r'^0(?=[A-Z]\d{5})', '')
)



df_organico = df_organico.withColumn(
    "FECINGRESO",
    F.to_date("FECINGRESO")  # o con formato explícito si hace falta
)



def limpiar_cesado(col):
    return F.trim(F.regexp_replace(F.col(col), r'\(CESADO\)', ''))

df_organico = df_organico.withColumn(
    "NOMBRECOMPLETO_CLEAN",
    limpiar_cesado("NOMBRECOMPLETO")
)




df_salesforce_estados = df_salesforce_estados.withColumn(
    "NBRANALISTA_CLEAN",
    limpiar_cesado("NBRULTACTOR")  # o la columna que corresponda al analista
)

df_salesforce_estados = df_salesforce_estados.withColumn(
    "NBRANALISTA_CLEAN",
    norm_txt_spark("NBRANALISTA_CLEAN")  # upper + quitar tildes + trim
)
