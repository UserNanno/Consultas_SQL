El equivalente de esto 

solicitudes_consolidado = spark.sql(f"""Select codmesevaluacion,
                codevaluacionsolicitud as codevaluacion,
                codidevaluacion as codevaluacionlargo,
                fecevaluacion,
                codinternocomputacional,
                --codinternocomputacionalconyuge,
                --case when codinternocomputacionalconyuge is not null then 'CAS'
                     --else 'SOL' end as estadocivil,
                numsolicitudevaluacion as numsolicitudprestamo,
                codsecuencialdecisionrbm as numdecision,
                desdecisionevaluacion as decision,
                desreglapauta as decisionregla,
                destipdecisionresultadorbm as resultadocda,
                desevaluacion as flujoevaluacion,
                mtocemevaluacion as mtocapacidadmaxendeudamiento,
                descampaniasolicitud as descampana,
                destipevaluacionsolicitudcredito as descampanaagrupado,
                descanalventarbmper as canalventarbm,
                
                case when descanalventarbmper = 'SALEFORCE' then SUBSTR(trim(numsolicitudevaluacion),5,7)
                     when descanalventarbmper = 'LOANS' then SUBSTR(trim(numsolicitudevaluacion),3,8)
                     else trim(numsolicitudevaluacion) end as numsolicitudcorto,

                case when destipdecisionresultadorbm = 'Approve' then 1
                     when destipdecisionresultadorbm = 'Decline' then 3
                     else 2 end as resultadojerarq,

                case when descanalventarbmper = 'BANCA MÓVIL' and desevaluacion = 'df_Regular' then '0.No valido'
                     when descampaniasolicitud = 'Reactivo' then '1.Reactivo'
                     else '2.No Reactivo' end as evaluacionreactivo,

                case when destipevaluacionsolicitudcredito in ('Regular LD', 'Cuotealo') and descampaniasolicitud in ('100% Aprobado', 'Pre-Aprobado') then 'LD APR'
                     when destipevaluacionsolicitudcredito = 'Regular LD' and descampaniasolicitud = 'Convenio' then 'LD CONV'
                     when destipevaluacionsolicitudcredito = 'Regular LD' and descampaniasolicitud = 'Invitado' then 'LD INV'
                     when destipevaluacionsolicitudcredito in ('Regular LD', 'Cuotealo') and descampaniasolicitud = 'CEF Shield' then 'LD SHD'

                     when destipevaluacionsolicitudcredito = 'Compra de Deuda' and descampaniasolicitud in ('100% Aprobado', 'Pre-Aprobado') then 'CDD APR'
                     when destipevaluacionsolicitudcredito = 'Compra de Deuda' and descampaniasolicitud = 'Convenio' then 'CDD CONV'
                     
                     when destipevaluacionsolicitudcredito = 'LD + Consolidación' and descampaniasolicitud in ('100% Aprobado', 'Pre-Aprobado') then 'REE APR'
                     when destipevaluacionsolicitudcredito = 'LD + Consolidación' and descampaniasolicitud = 'Convenio' then 'REE CONV'

                     when destipevaluacionsolicitudcredito = 'LD + Compra de Deuda + Consolidación' and descampaniasolicitud in ('100% Aprobado', 'Pre-Aprobado') then 'CONS APR'
                     when destipevaluacionsolicitudcredito = 'LD + Compra de Deuda + Consolidación' and descampaniasolicitud = 'Convenio' then 'CONS CONV'

                     else 'REACTIVO' end as LEAD_CEF
        from    catalog_lhcl_prod_bcp.bcp_ddv_rbmrbmper_modelogestion_vu.md_evaluacionsolicitudcredito
        where   tipproductosolicitudrbm = 'CC'
        and     descanalventarbmper not in ('YAPE', 'OTROS')
        and     codmesevaluacion = {codmes}""")

validacion_1=solicitudes_consolidado.groupBy("codinternocomputacional", "codevaluacion").count().withColumnRenamed("count","conteo")
validacion_conteo=validacion_1.filter((validacion_1.conteo>1))
print(validacion_conteo.count())

if validacion_conteo.count()>0:
    print("Ejecutando limpieza de duplicados")
    windowSpec = Window.partitionBy("codinternocomputacional", "codevaluacion").orderBy("codinternocomputacional")
    
    # Agregar una columna de número de fila para cada grupo de duplicados
    solicitudes_consolidado = solicitudes_consolidado.withColumn("row_number", row_number().over(windowSpec))
    
    # Filtrar solo los registros con row_number igual a 1
    solicitudes_consolidado = solicitudes_consolidado.filter("row_number = 1").drop("row_number")

else:
    print('sin duplicados')

#display(solicitudes_consolidado.limit(2))
solicitudes_consolidado.createOrReplaceTempView("df_solicitudes_consolidado")

es esto entonces no?

WITH
SOLICITUDES_CEF AS (
	SELECT
		CODMESEVALUACION,
		CODEVALUACIONSOLICITUD,
		CODIDEVALUACION,
		FECEVALUACION,
		CODINTERNOCOMPUTACIONAL,
		NUMSOLICITUDEVALUACION,
		CODSECUENCIALDECISIONRBM,
		DESDECISIONEVALUACION,
		DESREGLAPAUTA,
		DESTIPDECISIONRESULTADORBM,
		DESEVALUACION,
		MTOCEMEVALUACION,
		DESCAMPANIASOLICITUD,
		DESTIPEVALUACIONSOLICITUDCREDITO,
		DESCANALVENTARBMPER,
		(CASE
			WHEN DESCANALVENTARBMPER = 'SALEFORCE' THEN SUBSTR(TRIM(NUMSOLICITUDEVALUACION), 5, 7)
			WHEN DESCANALVENTARBMPER = 'LOANS' THEN SUBSTR(TRIM(NUMSOLICITUDEVALUACION), 3, 8)
			ELSE TRIM(NUMSOLICITUDEVALUACION)
		END) AS NUMSOLICITUDCORTO,
		(CASE
			WHEN DESTIPDECISIONRESULTADORBM = 'Approve' THEN 1
			WHEN DESTIPDECISIONRESULTADORBM = 'Decline' THEN 3
			ELSE 2
		END) AS RESULTADOJERARQUIA,
		(CASE
			WHEN DESCANALVENTARBMPER = 'BANCA MÓVIL' AND DESEVALUACION = 'df_Regular' THEN 'NO VALIDO'
			WHEN DESCAMPANIASOLICITUD = 'Reactivo' THEN 'REACTIVO'
			ELSE 'NO REACTIVO'
		END) AS EVALUACIONREACTIVO,
		(CASE
			WHEN DESTIPEVALUACIONSOLICITUDCREDITO IN ('REGULAR LD', 'CUOTEALO') AND DESCAMPANIASOLICITUD IN ('100% Aprobado', 'Pre-Aprobado') THEN 'LD APROBADO'
			WHEN DESTIPEVALUACIONSOLICITUDCREDITO IN ('REGULAR LD', 'CUOTEALO') AND DESCAMPANIASOLICITUD = 'CEF Shield' THEN 'LD SHIELD'
			WHEN DESTIPEVALUACIONSOLICITUDCREDITO = 'REGULAR LD' AND DESCAMPANIASOLICITUD = 'Convenio' THEN 'LD CONVENIO'
			WHEN DESTIPEVALUACIONSOLICITUDCREDITO = 'COMPRA DE DEUDA' AND DESCAMPANIASOLICITUD IN ('100% Aprobado', 'Pre-Aprobado') THEN 'CDD APROBADO'
			WHEN DESTIPEVALUACIONSOLICITUDCREDITO = 'COMPRA DE DEUDA' AND DESCAMPANIASOLICITUD = 'Convenio' THEN 'CDD CONVENIO'
			WHEN DESTIPEVALUACIONSOLICITUDCREDITO = 'LD + CONSOLIDACION' AND DESCAMPANIASOLICITUD IN ('100% Aprobado', 'Pre-Aprobado') THEN 'REE APROBADO'
			WHEN DESTIPEVALUACIONSOLICITUDCREDITO = 'LD + CONSOLIDACION' AND DESCAMPANIASOLICITUD = 'Convenio' THEN 'REE CONVENIO'
			WHEN DESTIPEVALUACIONSOLICITUDCREDITO = 'LD + COMPRA DE DEUDA + CONSOLIDACION' AND DESCAMPANIASOLICITUD IN ('100% Aprobado', 'Pre-Aprobado') THEN 'CONS APROBADO'
			WHEN DESTIPEVALUACIONSOLICITUDCREDITO = 'LD + COMPRA DE DEUDA + CONSOLIDACION' AND DESCAMPANIASOLICITUD = 'Convenio' THEN 'CONS CONVENIO'
			ELSE 'REACTIVO'
		END) AS LEAD_CEF
	FROM CATALOG_LHCL_PROD_BCP.BCP_DDV_RBMRBMPER_MODELOGESTION_VU.MD_EVALUACIONSOLICITUDCREDITO
	WHERE TIPPRODUCTOSOLICITUDRBM = 'CC' AND DESCANALVENTARBMPER NOT IN ('YAPE', 'OTROS') AND CODINTERNOCOMPUTACIONAL IS NOT NULL AND CODINTERNOCOMPUTACIONAL != ''
),
BASE_RN AS (
	SELECT
		*,
		ROW_NUMBER() OVER (
			PARTITION BY CODMESEVALUACION, CODINTERNOCOMPUTACIONAL, CODEVALUACIONSOLICITUD
			ORDER BY FECEVALUACION DESC, CODIDEVALUACION DESC
		) AS RN
	FROM SOLICITUDES_CEF
)
SELECT
	CODMESEVALUACION,
	CODEVALUACIONSOLICITUD,
	CODIDEVALUACION,
	FECEVALUACION,
	CODINTERNOCOMPUTACIONAL,
	NUMSOLICITUDEVALUACION,
	CODSECUENCIALDECISIONRBM,
	DESDECISIONEVALUACION,
	DESREGLAPAUTA,
	DESTIPDECISIONRESULTADORBM,
	DESEVALUACION,
	MTOCEMEVALUACION,
	DESCAMPANIASOLICITUD,
	DESTIPEVALUACIONSOLICITUDCREDITO,
	DESCANALVENTARBMPER,
	NUMSOLICITUDCORTO,
	RESULTADOJERARQUIA,
	EVALUACIONREACTIVO,
	LEAD_CEF
FROM BASE_RN
WHERE RN = 1;
