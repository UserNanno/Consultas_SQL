df.write \
  .format("delta") \
  .mode("overwrite") \
  .option("overwriteSchema", "true") \
  .saveAsTable("CATALOG_LHCL_PROD_BCP.BCP_EDV_RBMBDN.HM_LEADS_BDI")
