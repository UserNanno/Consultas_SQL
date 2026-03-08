LO SIGUIENTE QUE HACEN EN SPARK ES ESTO:

ventas_consolidado = spark.sql(f"""select cast(date_format(A.fecapertura,"yyyMM") as int) as CODMES_VTA,
                CAST (A.fecapertura AS DATE) AS FECAPERTURA_VTA, 
                CAST (A.fecdesembolso AS DATE) AS FECDESEMBOLSO_VTA,
                A.codclavecta AS codclavecta_VTA,
                A.codsolicitud as CODSOLICITUDPRESTAMO_VTA,
                A.codclavepartycli AS codclavepartycli_VTA,
                c.codclaveunicocli AS codclaveunicocli_VTA,
                c.codinternocomputacional AS codinternocomputacional_VTA,
                A.CODPRODUCTO AS CODPRODUCTO_VTA,
                A.mtodesembolsado AS mtodesembolsado_VTA,
                A.CODMONEDA AS CODMONEDA_VTA,
                CASE WHEN A.CODMONEDA <> '0001' THEN A.mtodesembolsado*B.MTOCAMBIOMONEDAORIGENMONEDADESTINO ELSE A.mtodesembolsado END MTODESEMBOLSADOSOL_VTA,

                CASE WHEN SUBSTRING(TRIM(A.codsolicitud),1,2)='CX' THEN SUBSTR(trim(A.codsolicitud),3,8) --LOANS
                     WHEN SUBSTRING(TRIM(A.codsolicitud),1,2)='DX' THEN SUBSTR(trim(A.codsolicitud),3,8) --LOANS
                     WHEN A.codsolicitud LIKE '2________' THEN SUBSTR(trim(A.codsolicitud),3,7) --SLF
                     WHEN A.codsolicitud LIKE '      2________' THEN SUBSTR(trim(A.codsolicitud),3,7) --SLF
                     WHEN A.codsolicitud LIKE 'O%' THEN SUBSTR(trim(A.codsolicitud),3,7) --SLF
                     ELSE trim(A.codsolicitud) END codsolicitudcorto_VTA, --BMO y CUOTEALO

                CASE WHEN A.codproducto='CPEFIA' THEN 'CUOTEALO'
                     WHEN SUBSTRING(TRIM(A.codsolicitud),1,2)='PE' THEN 'CUOTEALO'
                     WHEN SUBSTRING(TRIM(A.codsolicitud),1,2)='YP' THEN 'YAPE'
                     WHEN SUBSTRING(TRIM(A.codsolicitud),1,2)='JB' THEN 'BANCA MÓVIL'
                     WHEN A.codproducto='CPEYAP' THEN 'YAPE'
                     WHEN SUBSTRING(TRIM(A.codsolicitud),1,2)='CX' THEN 'LOANS'
                     WHEN SUBSTRING(TRIM(A.codsolicitud),1,2)='DX' THEN 'LOANS'
                     WHEN A.codsolicitud LIKE '2________' THEN 'SALEFORCE'
                     WHEN A.codsolicitud LIKE '      2________' THEN 'SALEFORCE'
                     WHEN A.codsolicitud LIKE 'O%' THEN 'SALEFORCE'
                     ELSE 'OTROS' END CANAL_MDPREST_VTA
                from (SELECT * 
                      FROM catalog_lhcl_prod_bcp.bcp_udv_int_vu.M_CUENTACREDITOPERSONAL 
                      where cast(date_format(fecapertura,"yyyMM") as int) >= {codmes}
                      and   cast(date_format(fecapertura,"yyyMM") as int) <= {codmes_1}
                      and   flgregeliminadofuente = 'N'
                      and   codproducto IN ('CPEEFM','CPECMC','CPEDPP','CPECEM','CPEECV','CPEGEN','CPEADH','CPEFIA'/*, 'CPEGHC'*/)) A
                left join df_tipo_cambio B
                ON  cast(date_format(A.fecapertura,"yyyMM") as int)=B.CODMES
                AND  A.CODMONEDA = B.CODMONEDAORIGEN
                left join  catalog_lhcl_prod_bcp.bcp_udv_int_vu.M_CLIENTE C
                ON   A.codclavepartycli = C.codclavepartycli""")

validacion_1=ventas_consolidado.groupBy("codclavecta_VTA").count().withColumnRenamed("count","conteo")
validacion_conteo=validacion_1.filter((validacion_1.conteo>1))
print(validacion_conteo.count())

if validacion_conteo.count()>0:
    print("Ejecutando limpieza de duplicados")
    windowSpec = Window.partitionBy("codclavecta_VTA").orderBy("codclavecta_VTA")
    
    # Agregar una columna de número de fila para cada grupo de duplicados
    ventas_consolidado = ventas_consolidado.withColumn("row_number", row_number().over(windowSpec))
    
    # Filtrar solo los registros con row_number igual a 1
    ventas_consolidado = ventas_consolidado.filter("row_number = 1").drop("row_number")

else:
    print('sin duplicados')

#display(ventas_consolidado.limit(2))
ventas_consolidado.createOrReplaceTempView("df_ventas_consolidado")




PARA LO CUAL YA TENEMOS LO DEL TIPO DE CAMBIO QUE LO GUARDE EN UNA TABLA

CREATE TABLE CATALOG_LHCL_PROD_BCP_EXPL.BCP_EDV_RBMBDN.T72496_TIPOCAMBIO
AS
WITH
TP_FECHA_TIPOCAMBIO AS (
	SELECT
		CAST(date_format(FECTIPCAMBIO, 'yyyyMM') AS INT) AS CODMES,
		MAX(FECTIPCAMBIO) AS FECTIPCAMBIO
	FROM CATALOG_LHCL_PROD_BCP.BCP_UDV_INT_VU.H_TIPOCAMBIO
	WHERE CAST(DATE_FORMAT(FECTIPCAMBIO, 'yyyMM') AS INT) >= 202301 AND CODMONEDADESTINO = '0001' AND CODAPP = 'GLM'
	GROUP BY CAST(date_format(FECTIPCAMBIO, 'yyyyMM') AS INT)
),
TP_DATA_TIPOCAMBIO AS (
	SELECT
		CAST(date_format(FECTIPCAMBIO, 'yyyyMM') AS INT) AS CODMES,
		FECTIPCAMBIO,
		CODMONEDAORIGEN,
		CODMONEDADESTINO,
		MTOCAMBIOMONEDAORIGENMONEDADESTINO
	FROM CATALOG_LHCL_PROD_BCP.BCP_UDV_INT_VU.H_TIPOCAMBIO
	WHERE CAST(DATE_FORMAT(FECTIPCAMBIO, 'yyyMM') AS INT) >= 202301 AND CODMONEDADESTINO = '0001' AND CODAPP = 'GLM'
),
TIPO_CAMBIO AS (
	SELECT
		A.CODMES,
		A.FECTIPCAMBIO,
		A.CODMONEDAORIGEN,
		A.CODMONEDADESTINO,
		A.MTOCAMBIOMONEDAORIGENMONEDADESTINO
	FROM TP_DATA_TIPOCAMBIO A
	INNER JOIN TP_FECHA_TIPOCAMBIO B ON (A.CODMES = B.CODMES AND A.FECTIPCAMBIO = B.FECTIPCAMBIO)
),
TIPO_CAMBIO_UNICO AS (
	SELECT
		CODMES,
		FECTIPCAMBIO,
		CODMONEDAORIGEN,
		CODMONEDADESTINO,
		MTOCAMBIOMONEDAORIGENMONEDADESTINO
	FROM (
		SELECT
			*,
			ROW_NUMBER() OVER (
				PARTITION BY CODMES, CODMONEDAORIGEN
				ORDER BY FECTIPCAMBIO DESC, CODMONEDAORIGEN
			) AS RN
		FROM TIPO_CAMBIO
	)
	WHERE RN = 1
)
SELECT
	CODMES,
	FECTIPCAMBIO,
	CODMONEDAORIGEN,
	CODMONEDADESTINO,
	MTOCAMBIOMONEDAORIGENMONEDADESTINO
FROM TIPO_CAMBIO_UNICO



AHORA PARA USARLO EN ESTA NUEVA SECCION, HAGO ESTO

SELECT
	CAST(date_format(A.FECAPERTURA, 'yyyyMM') AS INT) AS CODMES,
	CAST(A.FECAPERTURA AS DATE) AS FECAPERTURA,
	CAST(A.FECDESEMBOLSO AS DATE) AS FECDESEMBOLSO,
	A.CODCLAVECTA,
	A.CODSOLICITUD,
	A.CODCLAVEPARTYCLI,
	C.CODCLAVEUNICOCLI,
	UPPER(TRIM(C.CODINTERNOCOMPUTACIONAL)) AS CODINTERNOCOMPUTACIONAL,
	A.CODPRODUCTO,
	A.MTODESEMBOLSADO,
	A.CODMONEDA,
	(CASE
		WHEN A.CODMONEDA <> '0001' THEN A.MTODESEMBOLSADO * B.MTOCAMBIOMONEDAORIGENMONEDADESTINO
		ELSE A.MTODESEMBOLSADO
	END) AS MTODESEMBOLSADOSOL,
	(CASE
		WHEN SUBSTRING(TRIM(A.CODSOLICITUD), 1, 2) = 'CX' THEN SUBSTR(TRIM(A.CODSOLICITUD), 3, 8)
		WHEN SUBSTRING(TRIM(A.CODSOLICITUD), 1, 2) = 'DX' THEN SUBSTR(TRIM(A.CODSOLICITUD), 3, 8)
		WHEN A.CODSOLICITUD LIKE '2________' THEN SUBSTR(TRIM(A.CODSOLICITUD), 3, 7)
		WHEN A.CODSOLICITUD LIKE '      2________' THEN SUBSTR(TRIM(A.CODSOLICITUD), 3, 7)
		WHEN A.CODSOLICITUD LIKE '0%' THEN SUBSTR(TRIM(A.CODSOLICITUD), 3, 7)
		ELSE TRIM(A.CODSOLICITUD)
	END) AS CODSOLICITUDCORTO,
	(CASE
		WHEN A.CODPRODUCTO = 'CPEFIA' THEN 'CUOTEALO'
		WHEN A.CODPRODUCTO = 'CPEYAP' THEN 'YAPE'
		WHEN SUBSTRING(TRIM(A.CODSOLICITUD), 1, 2) = 'PE' THEN 'CUOTEALO'
		WHEN SUBSTRING(TRIM(A.CODSOLICITUD), 1, 2) = 'YP' THEN 'YAPE'
		WHEN SUBSTRING(TRIM(A.CODSOLICITUD), 1, 2) = 'JB' THEN 'BANCA MÓVIL'
		WHEN SUBSTRING(TRIM(A.CODSOLICITUD), 1, 2) = 'CX' THEN 'LOANS'
		WHEN SUBSTRING(TRIM(A.CODSOLICITUD), 1, 2) = 'DX' THEN 'LOANS'
		WHEN A.CODSOLICITUD LIKE '2________' THEN 'SALEFORCE'
		WHEN A.CODSOLICITUD LIKE '      2________' THEN 'SALEFORCE'
		WHEN A.CODSOLICITUD LIKE '0%' THEN 'SALEFORCE'
		ELSE 'OTROS'
	END) AS CANAL
FROM CATALOG_LHCL_PROD_BCP.BCP_UDV_INT_VU.M_CUENTACREDITOPERSONAL A
LEFT JOIN CATALOG_LHCL_PROD_BCP_EXPL.BCP_EDV_RBMBDN.T72496_TIPOCAMBIO B ON (CAST(date_format(A.FECAPERTURA, 'yyyyMM') AS INT) = B.CODMES AND A.CODMONEDA = B.CODMONEDAORIGEN)
LEFT JOIN CATALOG_LHCL_PROD_BCP.BCP_UDV_INT_VU.M_CLIENTE C ON (A.CODCLAVEPARTYCLI = C.CODCLAVEPARTYCLI)
WHERE A.FLGREGELIMINADOFUENTE = 'N' AND A.CODPRODUCTO IN ('CPEEFM','CPECMC','CPEDPP','CPECEM','CPEECV','CPEGEN','CPEADH','CPEFIA');


QUE ME FALTARIA? 

