CREATE TABLE CATALOG_LHCL_PROD_BCP_EXPL.BCP_EDV_SEGPER.T70725_CLIENTES_REDEEMERS
AS
SELECT *
FROM read_files(
 'abfss://bcp-edv-segper@adlscu1lhclbackp05.dfs.core.windows.net/T70725/EXPORTS/ClientesRedeemers/',
 format => 'parquet'
);

[CF_USE_DELTA_FORMAT] Reading from a Delta table is not supported with this syntax. If you would like to consume data from Delta, please refer to the docs: read a Delta table (https://docs.microsoft.com/azure/databricks/delta/tutorial#read), or read a Delta table as a stream source (https://docs.microsoft.com/azure/databricks/structured-streaming/delta-lake#table-streaming-reads-and-writes). The streaming source from Delta is already optimized for incremental consumption of data. SQLSTATE: 42000
