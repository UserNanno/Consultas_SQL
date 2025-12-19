df_salesforce_estados \
    .filter(F.col("CODSOLICITUD") == "O0018721853") \
    .show(truncate=False)
