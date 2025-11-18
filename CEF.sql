CREATE TABLE CATALOG_LHCL_PROD_BCP.BCP_EDV_RBMBDN.T72496_BASEOPORTUNIDADES
LOCATION 'abfss://bcp-edv-rbmbdn@adlscu1lhclbackp05.dfs.core.windows.net/T72496/TABLAS_DELTA/BASE'
AS
SELECT * FROM read_files(
  'abfss://bcp-edv-rbmbdn@adlscu1lhclbackp05.dfs.core.windows.net/T72496/CARGA/BASE/BaseInicial.csv',
  format => 'csv',
  header => true,
  delimiter => ',',
  mode => 'FAILFAST'
);
