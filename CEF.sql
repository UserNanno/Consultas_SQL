from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DecimalType, DateType
)
from pyspark.sql.functions import col, upper, lpad, to_date
from pyspark.sql import functions as F
from pyspark.sql.window import Window


import re
from functools import reduce
from pyspark.sql import DataFrame

base_dir = "abfss://bcp-edv-rbmbdn@adlscu1lhclbackp05.dfs.core.windows.net/T72496/CARGA/ORGANICO/"
entries = dbutils.fs.ls(base_dir)
files = [e.path for e in entries if e.name.startswith("1n_Activos_2025") and e.name.endswith(".xlsx")]
assert files, "No se encontraron archivos 1n_Activos_2025*.xlsx"

excel_options = {
    "header": "true",
    "inferSchema": "true",
    "treatEmptyValuesAsNulls": "true",
    "timestampFormat": "yyyy-MM-dd HH:mm:ss",
    # "dataAddress": "'Hoja1'!A1",  # Si el header no está en la fila 1
}

dfs = []
for path in files:
    reader = spark.read.format("com.crealytics.spark.excel")
    for k, v in excel_options.items():
        reader = reader.option(k, v)
    df = reader.load(path)

    for c in df.columns:
        new_name = re.sub(r"\s+", " ", c.strip())
        if new_name != c:
            df = df.withColumnRenamed(c, new_name)

    # 2) Elimina columnas sin nombre (_c0, _c1, ..., _c29)
    cols_clean = [c for c in df.columns if not re.match(r"^_c\d+$", c)]
    df = df.select(*cols_clean)

    dfs.append(df)

# 3) Une por nombre de columnas
from pyspark.sql import functions as F
df_organico_raw = reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), dfs)

def quitar_tildes(col):
    return (F.regexp_replace(F.regexp_replace(F.regexp_replace(F.regexp_replace(F.regexp_replace(col,
        "[ÁÀÂÄáàâä]", "A"),"[ÉÈÊËéèêë]", "E"),"[ÍÌÎÏíìîï]", "I"),"[ÓÒÔÖóòôö]", "O"),"[ÚÙÛÜúùûü]", "U"))



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





path_salesfoce_estados = "abfss://bcp-edv-rbmbdn@adlscu1lhclbackp05.dfs.core.windows.net/T72496/CARGA/SALESFORCE/INFORME_ESTADO/INFORME_ESTADO_*.csv"



df_salesforce_raw = (
    spark.read
        .format("csv")
        .option("header", "true")
        .option("encoding", "ISO-8859-1")
        .load(path_salesfoce_estados)
)


df_salesforce_estados = (
    df_salesforce_raw
        .select(
            F.col("Fecha de inicio del paso").alias("FECINICIOEVALUACION"),
            F.col("Fecha de finalización del paso").alias("FECFINEVALUACION"),
            F.col("Nombre del registro").alias("CODSOLICITUD"),
            F.col("Estado").alias("ESTADOSOLICITUD"),
            F.col("Paso: Nombre").alias("NBRPASO"),
            F.col("Último actor: Nombre completo").alias("NBRULTACTOR"),
            F.col("Último actor del paso: Nombre completo").alias("NBRULTACTORPASO"),
            F.col("Proceso de aprobación: Nombre").alias("PROCESO"),
        )
)


df_salesforce_estados = (
    df_salesforce_estados
        .withColumn("ESTADOSOLICITUD", F.upper(F.col("ESTADOSOLICITUD")))
        .withColumn("NBRPASO", F.upper(F.col("NBRPASO")))
        .withColumn("NBRULTACTOR", F.upper(F.col("NBRULTACTOR")))
        .withColumn("NBRULTACTORPASO", F.upper(F.col("NBRULTACTORPASO")))
        .withColumn("PROCESO", F.upper(F.col("PROCESO")))
)


df_salesforce_estados = (
    df_salesforce_estados
        .withColumn("NBRPASO", quitar_tildes("NBRPASO"))
        .withColumn("NBRULTACTOR", quitar_tildes("NBRULTACTOR"))
        .withColumn("NBRULTACTORPASO", quitar_tildes("NBRULTACTORPASO"))
        .withColumn("PROCESO", quitar_tildes("PROCESO"))
)



def parse_fecha_hora_esp_col(col):
    s = F.col(col).cast("string")
    s = F.regexp_replace(s, u'\u00A0', ' ')
    s = F.regexp_replace(s, u'\u202F', ' ')
    s = F.lower(F.trim(s))
    
    s = F.regexp_replace(s, r'\s+', ' ')
    
    # 2) Normalizar "a. m." / "p. m." -> AM / PM
    s = F.regexp_replace(s, r'(?i)a\W*m\W*', 'AM')
    s = F.regexp_replace(s, r'(?i)p\W*m\W*', 'PM')
    
    return F.to_timestamp(s, 'dd/MM/yyyy hh:mm a')



df_salesforce_estados = (
    df_salesforce_estados
        .withColumn(
            "FECHORINICIOEVALUACION",
            parse_fecha_hora_esp_col("FECINICIOEVALUACION")
        )
        .withColumn(
            "FECHORFINEVALUACION",
            parse_fecha_hora_esp_col("FECFINEVALUACION")
        )
)




df_salesforce_estados = (
    df_salesforce_estados
        # EXTRAER FECHA
        .withColumn("FECINICIOEVALUACION", F.to_date("FECHORINICIOEVALUACION"))
        .withColumn("FECFINEVALUACION", F.to_date("FECHORFINEVALUACION"))

        # EXTRAER HORA (en string HH:mm:ss)
        .withColumn("HORINICIOEVALUACION", F.date_format("FECHORINICIOEVALUACION", "HH:mm:ss"))
        .withColumn("HORFINEVALUACION", F.date_format("FECHORFINEVALUACION", "HH:mm:ss"))

        # CODMESEVALUACION = YYYYMM
        .withColumn("CODMESEVALUACION",
                    F.date_format("FECINICIOEVALUACION", "yyyyMM"))
)
