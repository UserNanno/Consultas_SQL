Mira tengo esto

%pip install --upgrade databricks-sql-connector
dbutils.library.restartPython()  # o reinicia el cluster/kernel manualmente


from databricks import sql
import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.window import Window

codmes = 202501
codmes_1 = 202502


conn = sql.connect(
    server_hostname = "adb-6238163592670798.18.azuredatabricks.net",
    http_path = "/sql/1.0/warehouses/510f1d9e2d1b2250",
    access_token = ""
)



query = f"""
    SELECT
        codmesevaluacion,
        codevaluacionsolicitud AS codevaluacion,
        codidevaluacion AS codevaluacionlargo,
        fecevaluacion,
        codinternocomputacional,
        numsolicitudevaluacion AS numsolicitudprestamo,
        codsecuencialdecisionrbm AS numdecision,
        desdecisionevaluacion AS decision,
        desreglapauta AS decisionregla,
        destipdecisionresultadorbm AS resultadocda,
        desevaluacion AS flujoevaluacion,
        mtocemevaluacion AS mtocapacidadmaxendeudamiento,
        descampaniasolicitud AS descampana,
        destipevaluacionsolicitudcredito AS descampanaagrupado,
        descanalventarbmper AS canalventarbm,

        CASE
            WHEN descanalventarbmper = 'SALEFORCE' THEN SUBSTR(TRIM(numsolicitudevaluacion), 5, 7)
            WHEN descanalventarbmper = 'LOANS'     THEN SUBSTR(TRIM(numsolicitudevaluacion), 3, 8)
            ELSE TRIM(numsolicitudevaluacion)
        END AS numsolicitudcorto,

        CASE
            WHEN destipdecisionresultadorbm = 'Approve' THEN 1
            WHEN destipdecisionresultadorbm = 'Decline' THEN 3
            ELSE 2
        END AS resultadojerarq,

        CASE
            WHEN descanalventarbmper = 'BANCA MÓVIL' AND desevaluacion = 'df_Regular' THEN '0.No valido'
            WHEN descampaniasolicitud = 'Reactivo' THEN '1.Reactivo'
            ELSE '2.No Reactivo'
        END AS evaluacionreactivo,

        CASE
            WHEN destipevaluacionsolicitudcredito IN ('Regular LD', 'Cuotealo') AND descampaniasolicitud IN ('100% Aprobado', 'Pre-Aprobado') THEN 'LD APR'
            WHEN destipevaluacionsolicitudcredito = 'Regular LD' AND descampaniasolicitud = 'Convenio' THEN 'LD CONV'
            WHEN destipevaluacionsolicitudcredito = 'Regular LD' AND descampaniasolicitud = 'Invitado' THEN 'LD INV'
            WHEN destipevaluacionsolicitudcredito IN ('Regular LD', 'Cuotealo') AND descampaniasolicitud = 'CEF Shield' THEN 'LD SHD'
            WHEN destipevaluacionsolicitudcredito = 'Compra de Deuda' AND descampaniasolicitud IN ('100% Aprobado', 'Pre-Aprobado') THEN 'CDD APR'
            WHEN destipevaluacionsolicitudcredito = 'Compra de Deuda' AND descampaniasolicitud = 'Convenio' THEN 'CDD CONV'
            WHEN destipevaluacionsolicitudcredito = 'LD + Consolidación' AND descampaniasolicitud IN ('100% Aprobado', 'Pre-Aprobado') THEN 'REE APR'
            WHEN destipevaluacionsolicitudcredito = 'LD + Consolidación' AND descampaniasolicitud = 'Convenio' THEN 'REE CONV'
            WHEN destipevaluacionsolicitudcredito = 'LD + Compra de Deuda + Consolidación' AND descampaniasolicitud IN ('100% Aprobado', 'Pre-Aprobado') THEN 'CONS APR'
            WHEN destipevaluacionsolicitudcredito = 'LD + Compra de Deuda + Consolidación' AND descampaniasolicitud = 'Convenio' THEN 'CONS CONV'
            ELSE 'REACTIVO'
        END AS LEAD_CEF

    FROM catalog_lhcl_prod_bcp.bcp_ddv_rbmrbmper_modelogestion_vu.md_evaluacionsolicitudcredito
    WHERE tipproductosolicitudrbm = 'CC'
      AND descanalventarbmper NOT IN ('YAPE', 'OTROS')
      AND codmesevaluacion = {codmes}
"""



cursor = conn.cursor()


cursor.execute(query)
rows = cursor.fetchall()
columns = [c[0] for c in cursor.description]
pdf = pd.DataFrame(rows, columns=columns)


solicitudes_consolidado = spark.createDataFrame(pdf)


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


validacion = spark.sql(f""" 
                        select  canalventarbm, count(*), count(distinct codinternocomputacional) 
                        from    df_solicitudes_consolidado
                        group by canalventarbm 
""")

#display(validacion)


solicitudes_unicos = spark.sql(f""" select  *
                from    (
                        select  A.*,
                                ROW_NUMBER() OVER(PARTITION BY codinternocomputacional, numsolicitudprestamo ORDER BY resultadojerarq, fecevaluacion desc) AS RN
                        from    df_solicitudes_consolidado  A
                        where   evaluacionreactivo <> '0.No valido')
                where   RN = 1 """)

#display(solicitudes_unicos.limit(2))
solicitudes_unicos.createOrReplaceTempView("df_solicitudes_unicos")



query2 = f"""select cast(date_format(FECTIPCAMBIO,'yyyyMM') as int) as codmes, 
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
                    and     codapp='GLM'"""



conn = sql.connect(
    server_hostname = "adb-6238163592670798.18.azuredatabricks.net",
    http_path = "/sql/1.0/warehouses/510f1d9e2d1b2250",
    access_token = ""
)


cursor.execute(query2)
rows = cursor.fetchall()
columns = [c[0] for c in cursor.description]
pdf = pd.DataFrame(rows, columns=columns)


tipo_cambio = spark.createDataFrame(pdf)


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





query3 = f"""select cast(date_format(A.fecapertura,"yyyMM") as int) as CODMES_VTA,
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
                ON   A.codclavepartycli = C.codclavepartycli"""





cursor.execute(query3)
rows = cursor.fetchall()
columns = [c[0] for c in cursor.description]
pdf = pd.DataFrame(rows, columns=columns)


ServerOperationError: [TABLE_OR_VIEW_NOT_FOUND] The table or view `df_tipo_cambio` cannot be found. Verify the spelling and correctness of the schema and catalog.
If you did not qualify the name with a schema, verify the current_schema() output, or qualify the name with the correct schema and catalog.
To tolerate the error on drop use DROP VIEW IF EXISTS or DROP TABLE IF EXISTS. SQLSTATE: 42P01; line 38 pos 26
