CREATE TABLE IF NOT EXISTS catalog_lhcl_prod_bcp.bcp_edv_rbmper.T57182_SOLICITUDES_HIST (
  CODMES                INT,
  CODINTERNOCOMPUTACIONAL STRING,
  TIPO_LEAD             STRING,
  TIPO_EVALUACION       STRING,
  TIPO_CANAL            STRING,
  CANAL_F               STRING,
  RESULTADO_FINAL       STRING,
  RESULTADO_FINAL_UNICO STRING,
  FLG_MTOSOLICITUD      INT,
  FLG_MTOVENTA          INT,
  RESULTADO_CDA         STRING,
  DESCAMPANA            STRING,
  DECISION              STRING,
  C                     BIGINT,
  MTODESEMBOLSO         DOUBLE,
  MTOSOLICITADO         DOUBLE,
  NUM_SOLICITUDES       BIGINT,
  MTO_OFERTA            DOUBLE
)
USING DELTA
PARTITIONED BY (CODMES)
LOCATION 'abfss://bcp-edv-rbmper@adlscu1lhclbackp05.dfs.core.windows.net/data/in/T57182/SOLICITUDES_HIST'
;
