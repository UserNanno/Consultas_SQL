from pyspark.sql import functions as F

df_dbg = df_sf_estados_raw.withColumn("FILE", F.input_file_name())

df_dbg.filter(F.col("Nombre del registro") == "O0018721853") \
  .select(
      "FILE",
      "Nombre del registro",
      "Paso: Nombre",
      "Estado",
      "Estado del paso",
      "Fecha de inicio del paso",
      "Fecha de finalización del paso"
  ) \
  .show(truncate=False)
