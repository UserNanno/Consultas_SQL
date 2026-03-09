En spark:

leads_consolidado = spark.sql(f"""Select round(a.CODMES,0) AS CODMES,\
                    a.codinternocomputacional,\
                    a.codsubsegmento,\
                    round(a.tipventa,0) as tipventa,\
                    round(a.tipoferta,0) as tipoferta,\
                    a.descampana as descampanalead,\
                    a.codcondicioncliente,\
                    round(a.mtofinalofertadosol,1) as mtofinalofertadosol,\
                    round(a.NUMPLAZO,0) as plazo,\
                    a.PCTTASAEFECTIVAANUAL as tea,\
                    case when a.tipventa in (6) and a.tipoferta in (3) and a.codcondicioncliente in ('APR', 'PRE') then 1\
                         when a.tipventa in (6) and a.tipoferta in (3) and a.codcondicioncliente = 'OPT' then 2\
                         when a.tipventa in (6) and a.tipoferta in (41) and a.codcondicioncliente in ('APR', 'PRE') then 3\
                         when a.tipventa in (6) and a.tipoferta in (38) and a.codcondicioncliente in ('APR', 'PRE') then 4\
                         when a.tipventa in (6) and a.tipoferta in (89,98) then 5\
                         when a.tipventa in (6) and a.tipoferta in (3) and a.codcondicioncliente = 'INV' then 6\
                         when a.tipventa in (6) and a.tipoferta in (187) then 7\
                         when a.tipventa in (110) and a.tipoferta in (3) then 8\
                         when a.tipventa in (110) and a.tipoferta in (41) then 9\
                         when a.tipventa in (110) and a.tipoferta in (38) then 10\
                         else 11 end as prioridad_lead,\
                    case when a.tipventa in (6) and a.tipoferta in (3) and a.codcondicioncliente in ('APR', 'PRE') then 'LD APR'\
                         when a.tipventa in (6) and a.tipoferta in (3) and a.codcondicioncliente = 'OPT' then 'LD OPT'\
                         when a.tipventa in (6) and a.tipoferta in (89,98) then 'LD SHD'\
                         when a.tipventa in (6) and a.tipoferta in (41) and a.codcondicioncliente in ('APR', 'PRE') then 'CDD APR'\
                         when a.tipventa in (6) and a.tipoferta in (187) then 'CDD INS'\
                         when a.tipventa in (6) and a.tipoferta in (38) and a.codcondicioncliente in ('APR', 'PRE') then 'REE APR'\
                         when a.tipventa in (6) and a.tipoferta in (3) and a.codcondicioncliente = 'INV' then 'LD INV'\
                         when a.tipventa in (110) and a.tipoferta in (3) then 'LD CONV'\
                         when a.tipventa in (110) and a.tipoferta in (41) then 'CDD CONV'\
                         when a.tipventa in (110) and a.tipoferta in (38) then 'REE CONV'\
                         when a.tipventa in (127) and a.tipoferta in (3) then 'LD MULTI'\
                         else 'OTRO' end as LEAD_CEF\
                    from    (SELECT 
                            CASE        when tipventa in (6) and tipoferta in (3) and codcondicioncliente = 'INV' then 'CEF LD Invitado'
                                        when tipventa in (6) and tipoferta in (3) and codcondicioncliente = 'PRE' then 'CEF LD Preaprobado'
                                        when tipventa in (6) and tipoferta in (3) and codcondicioncliente = 'APR' then 'CEF LD 100% Aprobado'
                                        when tipventa in (6) and tipoferta in (3) and codcondicioncliente = 'OPT' then 'CEF LD Optimus'
                                        when tipventa in (6) and tipoferta in (89,98) then 'CEF Shield'
                                        when tipventa in (6) and tipoferta in (41) and codcondicioncliente = 'PRE' then 'CEF CDD Preaprobado'
                                        when tipventa in (6) and tipoferta in (41) and codcondicioncliente = 'APR' then 'CEF CDD 100% Aprobado'
                                        when tipventa in (6) and tipoferta in (187) then 'CEF CDD Insuperable'
                                        when tipventa in (6) and tipoferta in (38) and codcondicioncliente = 'PRE' then 'CEF RE Preaprobado'
                                        when tipventa in (6) and tipoferta in (38) and codcondicioncliente = 'APR' then 'CEF RE 100% Aprobado'
                                        when tipventa in (110) and tipoferta in (3) then 'CEF Convenio LD'
                                        when tipventa in (110) and tipoferta in (41) then 'CEF Convenio CDD'
                                        when tipventa in (110) and tipoferta in (38) then 'CEF Convenio RE'
                            END AS PRODUCTO,A.*,
                                    ROW_NUMBER() OVER (PARTITION BY CODMES, CODCLAVECIC, TIPVENTA, TIPOFERTA ORDER BY FECINICIOVIGENCIA) AS N 
                            FROM catalog_lhcl_prod_bcp.bcp_edv_rbmper.hm_bdi A
                            ) A WHERE N = 1 AND
                            CODPAUTARBM <> 41 AND  (A.TIPVENTA = 6 OR A.TIPVENTA = 110)   
                            AND DESCAMPANA NOT IN ('CREDITO PERSONAL, VENTA AMPLIACION PLAZO', 'VENTA SKIP') AND  PRODUCTO IS NOT NULL
                            AND to_date(FECINICIOVIGENCIA,'yyyyMM') =date_trunc('MM',to_date(cast({codmes} AS STRING), 'yyyyMM'))\
                    """)


validacion_1=leads_consolidado.groupBy("codinternocomputacional", "LEAD_CEF").count().withColumnRenamed("count","conteo")
validacion_conteo=validacion_1.filter((validacion_1.conteo>1))
print(validacion_conteo.count())

if validacion_conteo.count()>0:
    print("Ejecutando limpieza de duplicados")
    windowSpec = Window.partitionBy("codinternocomputacional", "LEAD_CEF").orderBy("codinternocomputacional")
    
    # Agregar una columna de número de fila para cada grupo de duplicados
    leads_consolidado = leads_consolidado.withColumn("row_number", row_number().over(windowSpec))
    
    # Filtrar solo los registros con row_number igual a 1
    leads_consolidado = leads_consolidado.filter("row_number = 1").drop("row_number")

else:
    print('sin duplicados')

#display(leads_consolidado.limit(2))
leads_consolidado.createOrReplaceTempView("df_leads_consolidado")




leads_unicos = spark.sql(f""" select  *
                from    (
                        select  A.*,
                                ROW_NUMBER() OVER(PARTITION BY codinternocomputacional ORDER BY prioridad_lead) AS RN
                        from    df_leads_consolidado  A)
                where   RN = 1 """)

#display(leads_unicos.limit(2))
leads_unicos.createOrReplaceTempView("df_leads_unicos")





lUEGO EN SQL ARMÉ HASTA AHORA ESTO:

WITH
HD_CONSOLIDADOCAMPANIACRM AS (
	SELECT
		CODMESCAMPANIA,
		CODINTERNOCOMPUTACIONAL,
		CODPRODUCTOVENTACRM,
		TIPOFERTACRM,
		DESCAMPANIACRM,
		CODCONDICIONCLIRBM,
		MTOOFERTASOL,
		CTDMESPLAZO,
		PCTTASAEFECTIVAANUAL,
		(CASE
			WHEN CODPRODUCTOVENTACRM = '0006' AND TIPOFERTACRM = '0003' AND CODCONDICIONCLIRBM = 'INV' THEN 'CEF LD INVITADO'
			WHEN CODPRODUCTOVENTACRM = '0006' AND TIPOFERTACRM = '0003' AND CODCONDICIONCLIRBM = 'PRE' THEN 'CEF LD PREAPROBADO'
			WHEN CODPRODUCTOVENTACRM = '0006' AND TIPOFERTACRM = '0003' AND CODCONDICIONCLIRBM = 'APR' THEN 'CEF LD 100% APROBADO'
			WHEN CODPRODUCTOVENTACRM = '0006' AND TIPOFERTACRM = '0003' AND CODCONDICIONCLIRBM = 'OPT' THEN 'CEF LD OPTIMUS'
			WHEN CODPRODUCTOVENTACRM = '0006' AND TIPOFERTACRM = '0098' THEN 'CEF SHIELD'
			WHEN CODPRODUCTOVENTACRM = '0006' AND TIPOFERTACRM = '0041' AND CODCONDICIONCLIRBM = 'PRE' THEN 'CEF CDD PREAPROBADO'
			WHEN CODPRODUCTOVENTACRM = '0006' AND TIPOFERTACRM = '0041' AND CODCONDICIONCLIRBM = 'APR' THEN 'CEF CDD 100% APROBADO'
			WHEN CODPRODUCTOVENTACRM = '0006' AND TIPOFERTACRM = '0187' THEN 'CEF CDD INSUPERABLE'
			WHEN CODPRODUCTOVENTACRM = '0006' AND TIPOFERTACRM = '0038' AND CODCONDICIONCLIRBM = 'PRE' THEN 'CEF RE PREAPROBADO'
			WHEN CODPRODUCTOVENTACRM = '0006' AND TIPOFERTACRM = '0038' AND CODCONDICIONCLIRBM = 'APR' THEN 'CEF RE 100% APROBADO'
			WHEN CODPRODUCTOVENTACRM = '0110' AND TIPOFERTACRM = '0003' THEN 'CEF CONVENIO LD'
			WHEN CODPRODUCTOVENTACRM = '0110' AND TIPOFERTACRM = '0041' THEN 'CEF CONVENIO CDD'
			WHEN CODPRODUCTOVENTACRM = '0110' AND TIPOFERTACRM = '0038' THEN 'CEF CONVENIO RE'
			WHEN CODPRODUCTOVENTACRM = '0127' AND TIPOFERTACRM = '0003' THEN 'CEF MULTI LD'
		END) AS PRODUCTO,
		ROW_NUMBER() OVER (
			PARTITION BY CODMESCAMPANIA, CODINTERNOCOMPUTACIONAL, CODPRODUCTOVENTACRM, TIPOFERTACRM
			ORDER BY FECINICIOVIGENCIACAMPANIA
		) AS N
	FROM CATALOG_LHCL_PROD_BCP.BCP_DDV_CRM_CAMPANIAMASIVA_VU.HD_CONSOLIDADOCAMPANIACRM
	WHERE CODPRODUCTOVENTACRM IN ('0006', '0110', '0127') AND TIPOFERTACRM IN ('0003', '0038', '0041', '0098', '0187')
),
HD_CONSOLIDADOCAMPANIACRM_UNICOS AS (
	SELECT
		CODMESCAMPANIA,
		CODINTERNOCOMPUTACIONAL,
		CODPRODUCTOVENTACRM,
		TIPOFERTACRM,
		DESCAMPANIACRM,
		CODCONDICIONCLIRBM,
		MTOOFERTASOL,
		CTDMESPLAZO,
		PCTTASAEFECTIVAANUAL,
		(CASE
			WHEN CODPRODUCTOVENTACRM = '0006' AND TIPOFERTACRM = '0003' AND CODCONDICIONCLIRBM IN ('APR', 'PRE') THEN 1
			WHEN CODPRODUCTOVENTACRM = '0006' AND TIPOFERTACRM = '0003' AND CODCONDICIONCLIRBM = 'OPT' THEN 2
			WHEN CODPRODUCTOVENTACRM = '0006' AND TIPOFERTACRM = '0041' AND CODCONDICIONCLIRBM IN ('APR', 'PRE') THEN 3
			WHEN CODPRODUCTOVENTACRM = '0006' AND TIPOFERTACRM = '0038' AND CODCONDICIONCLIRBM IN ('APR', 'PRE') THEN 4
			WHEN CODPRODUCTOVENTACRM = '0006' AND TIPOFERTACRM = '0098' THEN 5
			WHEN CODPRODUCTOVENTACRM = '0006' AND TIPOFERTACRM = '0003' AND CODCONDICIONCLIRBM = 'INV' THEN 6
			WHEN CODPRODUCTOVENTACRM = '0006' AND TIPOFERTACRM = '0187' THEN 7
			WHEN CODPRODUCTOVENTACRM = '0110' AND TIPOFERTACRM = '0003' THEN 8
			WHEN CODPRODUCTOVENTACRM = '0110' AND TIPOFERTACRM = '0041' THEN 9
			WHEN CODPRODUCTOVENTACRM = '0110' AND TIPOFERTACRM = '0038' THEN 10
			WHEN CODPRODUCTOVENTACRM = '0127' AND TIPOFERTACRM = '0003' THEN 11
			ELSE 12
		END) AS PRIORIDADLEAD,
		(CASE
			WHEN CODPRODUCTOVENTACRM = '0006' AND TIPOFERTACRM = '0003' AND CODCONDICIONCLIRBM IN ('APR', 'PRE') THEN 'LD APROBADO'
			WHEN CODPRODUCTOVENTACRM = '0006' AND TIPOFERTACRM = '0003' AND CODCONDICIONCLIRBM = 'OPT' THEN 'LD OPT'
			WHEN CODPRODUCTOVENTACRM = '0006' AND TIPOFERTACRM = '0041' AND CODCONDICIONCLIRBM IN ('APR', 'PRE') THEN 'CDD APROBADO'
			WHEN CODPRODUCTOVENTACRM = '0006' AND TIPOFERTACRM = '0038' AND CODCONDICIONCLIRBM IN ('APR', 'PRE') THEN 'REE APROBADO'
			WHEN CODPRODUCTOVENTACRM = '0006' AND TIPOFERTACRM = '0098' THEN 'LD SHIELD'
			WHEN CODPRODUCTOVENTACRM = '0006' AND TIPOFERTACRM = '0003' AND CODCONDICIONCLIRBM = 'INV' THEN 'LD INV'
			WHEN CODPRODUCTOVENTACRM = '0006' AND TIPOFERTACRM = '0187' THEN 'CDD INS'
			WHEN CODPRODUCTOVENTACRM = '0110' AND TIPOFERTACRM = '0003' THEN 'LD CONVENIO'
			WHEN CODPRODUCTOVENTACRM = '0110' AND TIPOFERTACRM = '0041' THEN 'CDD CONVENIO'
			WHEN CODPRODUCTOVENTACRM = '0110' AND TIPOFERTACRM = '0038' THEN 'REE CONVENIO'
			WHEN CODPRODUCTOVENTACRM = '0127' AND TIPOFERTACRM = '0003' THEN 'LD MULTI'
			ELSE 'OTRO'
		END) AS LEADCEF
	FROM HD_CONSOLIDADOCAMPANIACRM
	WHERE N = 1 AND PRODUCTO IS NOT NULL
),
HD_CONSOLIDADOCAMPANIACRM_CONSOLIDADO AS (
	SELECT
		*
	FROM HD_CONSOLIDADOCAMPANIACRM_UNICOS
	QUALIFY ROW_NUMBER() OVER (
		PARTITION BY CODMESCAMPANIA, CODINTERNOCOMPUTACIONAL, LEADCEF
		ORDER BY CODINTERNOCOMPUTACIONAL
	) = 1
)
SELECT *
FROM HD_CONSOLIDADOCAMPANIACRM_CONSOLIDADO
