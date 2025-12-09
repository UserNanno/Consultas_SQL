from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DecimalType, DateType
)
from pyspark.sql.functions import col, upper, lpad, to_date
from pyspark.sql import functions as F


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


def quitar_tildes(col):
    return (F.regexp_replace(F.regexp_replace(F.regexp_replace(F.regexp_replace(F.regexp_replace(col,
        "[ÁÀÂÄáàâä]", "A"),"[ÉÈÊËéèêë]", "E"),"[ÍÌÎÏíìîï]", "I"),"[ÓÒÔÖóòôö]", "O"),"[ÚÙÛÜúùûü]", "U"))



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



df_repetidas = (
    df_salesforce_estados
        .groupBy("CODSOLICITUD")
        .count()
        .withColumnRenamed("count", "REPETICIONES")
)


df_repetidas_solo = df_repetidas.filter(F.col("REPETICIONES") > 1)

