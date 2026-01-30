from pyspark.sql import functions as F

audit_estados = (
    df_estados.groupBy("CODMESEVALUACION")
    .agg(
        F.count("*").alias("rows_estados"),
        F.countDistinct("CODSOLICITUD").alias("sol_unicas_estados"),
        (F.count("*") - F.countDistinct("CODSOLICITUD")).alias("rows_extra_por_duplicados")
    )
    .orderBy("CODMESEVALUACION")
)

audit_last = (
    df_last_estado.groupBy("CODMESEVALUACION")
    .agg(
        F.count("*").alias("rows_last"),
        F.countDistinct("CODSOLICITUD").alias("sol_unicas_last")
    )
    .orderBy("CODMESEVALUACION")
)

audit = audit_estados.join(audit_last, "CODMESEVALUACION", "left")
audit.show()



# Comparación de solicitudes únicas por mes entre fuentes vs final
comp = (
    df_estados.groupBy("CODMESEVALUACION")
      .agg(F.countDistinct("CODSOLICITUD").alias("sol_unicas_estados"))
      .join(
          df_last_estado.groupBy("CODMESEVALUACION").agg(F.countDistinct("CODSOLICITUD").alias("sol_unicas_final")),
          on="CODMESEVALUACION", how="left"
      )
      .orderBy("CODMESEVALUACION")
)

comp.show()





















df_estados.groupBy("CODMESEVALUACION").count().orderBy("CODMESEVALUACION").show()

+----------------+-----+
|CODMESEVALUACION|count|
+----------------+-----+
|          202501|19310|
|          202502|18229|
|          202503|16800|
|          202504|16095|
|          202505|16406|
|          202506|16143|
|          202507|14655|
|          202508|15209|
|          202509|18543|
|          202510|18006|
|          202511|15633|
|          202512|14156|
|          202601|12878|
+----------------+-----+

df_last_estado.groupBy("CODMESEVALUACION").count().orderBy("CODMESEVALUACION").show()

+----------------+-----+
|CODMESEVALUACION|count|
+----------------+-----+
|          202501|18417|
|          202502|17127|
|          202503|15893|
|          202504|15201|
|          202505|15390|
|          202506|15148|
|          202507|13723|
|          202508|14183|
|          202509|16920|
|          202510|16765|
|          202511|14320|
|          202512|12806|
|          202601|11927|
+----------------+-----+

df_final.groupBy("CODMESEVALUACION").count().orderBy("CODMESEVALUACION").show()

+----------------+-----+
|CODMESEVALUACION|count|
+----------------+-----+
|          202501|18417|
|          202502|17127|
|          202503|15893|
|          202504|15201|
|          202505|15390|
|          202506|15148|
|          202507|13723|
|          202508|14183|
|          202509|16920|
|          202510|16765|
|          202511|14320|
|          202512|12806|
|          202601|11927|
+----------------+-----+

df_final.filter("CODMESEVALUACION is null").count()

0



