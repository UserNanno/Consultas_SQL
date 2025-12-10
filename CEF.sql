# === 4. CREATE TABLA DELTA ===
(df.write.format("delta")
   .partitionBy("CODLOTEOFERTA")
   .mode("overwrite")
   .save(tabla_delta_path))


spark.sql(f"""
    CREATE TABLE IF NOT EXISTS CATALOG_LHCL_PROD_BCP.BCP_EDV_RBMBDN.HM_LEADS_BDI
    USING DELTA
    LOCATION '{tabla_delta_path}'
""")
