display(df_sf_estados_raw \
    .filter(F.col("Nombre del registro") == "O0018721853") \
    .show(truncate=False))
