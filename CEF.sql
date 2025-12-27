df = df.withColumn("TS_APPS", parse_fecha_hora_esp(F.col("FECHORACREACION")))
w = Window.partitionBy("CODSOLICITUD").orderBy(F.col("TS_APPS").desc_nulls_last())
df = df.withColumn("rn", F.row_number().over(w)).filter("rn=1").drop("rn")
