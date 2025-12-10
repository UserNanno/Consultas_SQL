df_salesforce_enriq.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .partitionBy("CODMESEVALUACION") \
    .saveAsTable("CATALOG_LHCL_PROD_BCP.BCP_EDV_RBMBDN.HM_LEADS_BDI")




df_salesforce_enriq.filter(F.col("MATORGANICO").isNull()).count()



df_salesforce_enriq \
    .filter(F.col("MATORGANICO").isNull()) \
    .select(
        "CODMESEVALUACION",
        "CODSOLICITUD",
        "NBRULTACTOR",
        "NBRULTACTORPASO"
    ) \
    .show(50, truncate=False)
