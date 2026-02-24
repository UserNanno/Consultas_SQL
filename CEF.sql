from pyspark import StorageLevel

df_out = df_final.persist(StorageLevel.DISK_ONLY)
_ = df_out.count()

(df_out.write
  .format("delta")
  .mode("overwrite")
  .saveAsTable("CATALOG_LHCL_PROD_BCP_EXPL.BCP_EDV_RBMBDN.TP_SOLICITUDES_CENTRALIZADO")
)

df_out.unpersist()
