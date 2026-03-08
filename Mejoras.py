select cast(date_format(FECTIPCAMBIO,"yyyMM") as int) as codmes, 
                        FECTIPCAMBIO,
                        CODMONEDAORIGEN,
                        CODMONEDADESTINO,
                        MTOCAMBIOMONEDAORIGENMONEDADESTINO
                    from    catalog_lhcl_prod_bcp.bcp_udv_int_vu.H_TIPOCAMBIO
                    where   FECTIPCAMBIO = (select max(FECTIPCAMBIO) as FECTIPCAMBIO
                        from    catalog_lhcl_prod_bcp.bcp_udv_int_vu.H_TIPOCAMBIO
                        where   cast(date_format(FECTIPCAMBIO,"yyyMM") as int)={codmes} 
                        AND     CODMONEDADESTINO='0001' 
                        AND     codapp='GLM')
                    and     CODMONEDADESTINO='0001' 
                    and     codapp='GLM'



NO PUEDO HACER ESTO DE ATRAS ALGO ASI COMO HAGO EN OTRA QUERY?

WITH
TP_FECHA_CLIENTE AS (
  SELECT CODMESCAMPANIAANALISISMERCADO, MAX(FECRUTINA) AS FECRUTINA
  FROM CATALOG_LHCL_PROD_BCP.BCP_DDV_CRM_FUNNELCONVERSION_VU.HD_CLIENTEANALISISMERCADO
  WHERE CODMESCAMPANIAANALISISMERCADO BETWEEN '202501' AND '202506'
  GROUP BY CODMESCAMPANIAANALISISMERCADO
),
