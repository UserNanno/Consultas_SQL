
# En una celda del notebook:
%pip install --upgrade databricks-sql-connector
dbutils.library.restartPython()  # o reinicia el cluster/kernel manualmente




import databricks.sql as dbsql

conn = dbsql.connect(
    server_hostname="adb-6238163592670798.18.azuredatabricks.net",
    http_path="/sql/1.0/warehouses/510f1d9e2d1b2250",
    access_token="TU_TOKEN_AQUI"
)
cur = conn.cursor()
cur.execute("""
SELECT codmesevaluacion, codevaluacionsolicitud, codinternocomputacional
FROM catalog_lhcl_prod_bcp.bcp_ddv_rbmrbmper_modelogestion_vu.md_evaluacionsolicitudcredito
LIMIT 10
""")
print(cur.fetchall())
cur.close()
conn.close()
