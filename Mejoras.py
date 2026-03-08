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
)


OKEY AHORA QUE TENGO ESTO, EN SPARK HACEN ESTO:

tipo_cambio = spark.sql(f"""select cast(date_format(FECTIPCAMBIO,"yyyMM") as int) as codmes, 
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
                    and     codapp='GLM'""")
 
validacion_1=tipo_cambio.groupBy("CODMONEDAORIGEN").count().withColumnRenamed("count","conteo")
validacion_conteo=validacion_1.filter((validacion_1.conteo>1))
print(validacion_conteo.count())

if validacion_conteo.count()>0:
    print("Ejecutando limpieza de duplicados")
    windowSpec = Window.partitionBy("CODMONEDAORIGEN").orderBy("CODMONEDAORIGEN")
    
    # Agregar una columna de número de fila para cada grupo de duplicados
    tipo_cambio = tipo_cambio.withColumn("row_number", row_number().over(windowSpec))
    
    # Filtrar solo los registros con row_number igual a 1
    tipo_cambio = tipo_cambio.filter("row_number = 1").drop("row_number")

else:
    print('sin duplicados')

#display(tipo_cambio.where("CODMONEDAORIGEN='1001'"))
tipo_cambio.createOrReplaceTempView("df_tipo_cambio")
