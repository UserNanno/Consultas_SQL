from pyspark.sql import functions as F

df_dbg = df_sf_estados_raw.withColumn("FILE", F.input_file_name())

df_dbg.filter(F.col("FILE").like("%INFORME_ESTADO_4%")) \
  .select("FILE", "Nombre del registro", "Paso: Nombre", "Estado del paso") \
  .filter(F.col("Nombre del registro")=="O0018721853") \
  .show(truncate=False)
