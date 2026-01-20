from pyspark.sql import SparkSession
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import pandas as pd
import os 
from pyspark.sql.functions import sum, avg
from pyspark.sql.functions import months_between, to_date
from pyspark.sql.functions import col
from pyspark.sql.functions import row_number, desc, asc
from pyspark.sql.functions import row_number, min,collect_list,regexp_replace,max,sum, avg, count, months_between, to_date,countDistinct,row_number, desc, asc,date_format,expr,udf,col, when, lit, isnull,date_add,add_months

mi_ruta='abfss://bcp-edv-rbmper@adlscu1lhclbackp05.dfs.core.windows.net/data/in/T57182/'
spark = SparkSession.builder \
    .appName("Ejemplo Legacy Time Parse Policy") \
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
    .getOrCreate()

import requests
import json
import time
from datetime import datetime
from dateutil.relativedelta import relativedelta
from concurrent.futures import ThreadPoolExecutor
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, expr, sum as spark_sum, avg, lit
)
from pyspark.sql.types import DecimalType, DoubleType
from pyspark.sql import Window
from pyspark.sql.functions import floor, concat, lit, when, sum as spark_sum, col, coalesce
# Configuración
DATABRICKS_INSTANCE = "https://adb-6238163592670798.18.azuredatabricks.net"
SQL_WAREHOUSE_ID = "36b5483debcd5868"
DATABRICKS_TOKEN = "XXXX"    



=================================================================================================================================================================




# Rango de meses establecido
codmes = 202512
codmes_1 = 202601 #Mes posterior
#mes_creacion_tabla = 202409  # Mes específico para crear la tabla

# Nombre de la tabla
nombre_tabla_leads = f"catalog_lhcl_prod_bcp.bcp_edv_rbmper.T57182_CEF_LD_LEADS_{codmes}"

# Función para ejecutar consultas SQL
def ejecutar_consulta(sql_query):
    url = f"{DATABRICKS_INSTANCE}/api/2.0/sql/statements"
    headers = {"Authorization": f"Bearer {DATABRICKS_TOKEN}", "Content-Type": "application/json"}
    payload = {"statement": sql_query, "warehouse_id": SQL_WAREHOUSE_ID, "disposition": "EXTERNAL_LINKS"}
    response = requests.post(url, headers=headers, json=payload)
    if response.status_code == 200:
        return response.json().get("statement_id")
    else:
        print("Error en la ejecución:", response.text)
        return None
    
# Verificar el estado de consulta
def verificar_estado_consulta(statement_id):
    url_status = f"{DATABRICKS_INSTANCE}/api/2.0/sql/statements/{statement_id}"
    headers = {"Authorization": f"Bearer {DATABRICKS_TOKEN}"}
    while True:
        status_response = requests.get(url_status, headers=headers)
        if status_response.status_code == 200:
            state = status_response.json().get("status", {}).get("state")
            if state == "SUCCEEDED":
                return True
            elif state == "FAILED":
                print("Error en la consulta:", status_response.json().get("status", {}).get("error", {}))
                return False
            else:
                time.sleep(2)
        else:
            print("Error al verificar el estado de la consulta:", status_response.text)
            return False

# Función para verificar si la tabla existe
def tabla_existe(nombre_tabla):
    try:
        spark.sql(f"DESCRIBE TABLE {nombre_tabla}")
        return True
    except AnalysisException:
        return False
    
print(nombre_tabla_leads)



=================================================================================================================================================================


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



=================================================================================================================================================================




validacion = spark.sql(f""" 
                        select  canalventarbm, count(*), count(distinct codinternocomputacional) 
                        from    df_solicitudes_consolidado
                        group by canalventarbm 
""")

#display(validacion)





=================================================================================================================================================================




solicitudes_unicos = spark.sql(f""" select  *
                from    (
                        select  A.*,
                                ROW_NUMBER() OVER(PARTITION BY codinternocomputacional, numsolicitudprestamo ORDER BY resultadojerarq, fecevaluacion desc) AS RN
                        from    df_solicitudes_consolidado  A
                        where   evaluacionreactivo <> '0.No valido')
                where   RN = 1 """)

#display(solicitudes_unicos.limit(2))
solicitudes_unicos.createOrReplaceTempView("df_solicitudes_unicos")





=================================================================================================================================================================




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



=================================================================================================================================================================




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





=================================================================================================================================================================





validacion = spark.sql(f""" 
            select  CANAL_MDPREST_VTA, count(*), count(distinct codclavepartycli_VTA), SUM(mtodesembolsado_VTA)
            from    df_ventas_consolidado
            group by CANAL_MDPREST_VTA 
""")

#display(validacion)




=================================================================================================================================================================




ventas_solicitudes = spark.sql(f"""select *
                from    df_solicitudes_unicos A
                left join df_ventas_consolidado B
                ON 	A.numsolicitudcorto = B.codsolicitudcorto_VTA
                AND 	A.canalventarbm = B.CANAL_MDPREST_VTA
                AND     TRIM(A.codinternocomputacional) = TRIM(B.codinternocomputacional_VTA)
                left join (select   cast(date_format(fecsolicitud,"yyyMM") as int) as codmes_SLF,
                                    fecsolicitud AS fecsolicitud_SLF,
                                    horsolicitud AS horsolicitud_SLF,
                                    codsolicitud AS codsolicitud_SLF,
                                    codinternocomputacional AS codinternocomputacional_SLF,
                                    tipestadosolicitud AS tipestadosolicitud_SLF,
                                    destipestadosolicitud AS destipestadosolicitud_SLF,
                                    flgdesembolsosolicitud AS flgdesembolsosolicitud_SLF,
                                    fecultestadosolicitud AS fecultestadosolicitud_SLF,
                                    codproducto AS codproducto_SLF,
                                    codappsolicitud AS codappsolicitud_SLF,
                                    flgventacef AS flgventacef_SLF,
                                    flgcompradeuda AS flgcompradeuda_SLF,
                                    flgcompradeudaadjunta AS flgcompradeudaadjunta_SLF,
                                    mtosolicitadocdd as mtosolicitadocdd_SLF,
                                    destipresultado AS destipresultado_SLF,
                                    desproducto AS desproducto_SLF,

                                    codmoneda AS codmoneda_SLF,
                                    numdiapagocuota AS numdiapagocuota_SLF,

                                    mtosolicitado AS mtosolicitado_SLF,
                                    ctdplazosolicitado AS ctdplazosolicitado_SLF,
                                    pcttasaefectivaanualsolicitud as teasolicitud_SLF,
                                    mtocuotamensualsolicitado AS mtocuotamensualsolicitado_SLF,

                                    mtoaprobado AS mtoaprobado_SLF,
                                    ctdplazoaprobado AS ctdplazoaprobado_SLF,
                                    pcttasaefectivaanualaprobada as teaaprobado_SLF,
                                    mtocuotamensualaprobado AS mtocuotamensualaprobado_SLF,
                                    
                                    codmatriculacolaboradoranalista AS codmatriculacolaboradoranalista_SLF,
                                    codmatriculacolaboradorexceptuador AS codmatriculacolaboradorexceptuador_SLF,
                                    codsectorista AS codsectorista_SLF,
                                    codmatriculacolaboradorautonomocentralizado AS codmatriculacolaboradorautonomocentralizado_SLF,
                                    codmatriculacolaboradorvendedor AS codmatriculacolaboradorvendedor_SLF,
                                    codmatriculacolaboradoraprobador AS codmatriculacolaboradoraprobador_SLF,

                                    tipevaluacionsolicitud AS tipevaluacionsolicitud_SLF,
                                    destipevaluacionsolicitud AS destipevaluacionsolicitud_SLF,
                                    tipformapago AS tipformapago_SLF,
                                    destipformapago AS destipformapago_SLF,
                                    tipestadojustificacionsolicitud AS tipestadojustificacionsolicitud_SLF,
                                    feciniciocalif as feciniciocalif_SLF,
                                    destipestadojustificacionsolicitud AS destipestadojustificacionsolicitud_SLF,
                                    destipventacda AS destipventacda_SLF,

                                    flgexcep AS flgexcep_SLF,
                                    nbrtipexcep AS nbrtipexcep_SLF,
                                    nbrexcep as nbrexcep_SLF,

                                    fecdesembolso AS fecdesembolso_SLF,
                                    fecabonoefectivo AS fecabonoefectivo_SLF,
                                    flgnuevactaabono AS flgnuevactaabono_SLF,
                                    pcttasacostoefectivo as tcea_SLF,
                                    pcttasasegurodesgravamen AS pcttasasegurodesgravamen_SLF,
                                    numcotizacionpricing AS numcotizacionpricing_SLF,
                                    destipetapa AS destipetapa_SLF

                            from 	catalog_lhcl_prod_bcp.bcp_udv_int_vu.m_solicitudcreditoconsumo /*aceptadas y flag compra de deuda (CDD+REE+Consolidaciones)*/
                            where 	cast(date_format(fecsolicitud,"yyyMM") as int)={codmes}
                            ) C
                ON 			cast(date_format(A.fecevaluacion,"yyyMM") as int) = C.codmes_SLF
                AND 		A.numsolicitudprestamo = C.CODSOLICITUD_SLF
                """)

#display(ventas_solicitudes.where("codclavecta_VTA is not null").limit(10))
ventas_solicitudes.createOrReplaceTempView("df_ventas_solicitudes")




=================================================================================================================================================================



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



=================================================================================================================================================================



leads_unicos = spark.sql(f""" select  *
                from    (
                        select  A.*,
                                ROW_NUMBER() OVER(PARTITION BY codinternocomputacional ORDER BY prioridad_lead) AS RN
                        from    df_leads_consolidado  A)
                where   RN = 1 """)

#display(leads_unicos.limit(2))
leads_unicos.createOrReplaceTempView("df_leads_unicos")




=================================================================================================================================================================






leads_consolidado_41 = spark.sql(f"""Select CODMES,\
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
                            AND to_date(FECINICIOVIGENCIA,'yyyyMM') =date_trunc('MM',to_date(cast({codmes} AS STRING), 'yyyyMM')
                            )\
                    """)

validacion_1=leads_consolidado_41.groupBy("codinternocomputacional", "LEAD_CEF").count().withColumnRenamed("count","conteo")
validacion_conteo=validacion_1.filter((validacion_1.conteo>1))
print(validacion_conteo.count())

if validacion_conteo.count()>0:
    print("Ejecutando limpieza de duplicados")
    windowSpec = Window.partitionBy("codinternocomputacional", "LEAD_CEF").orderBy("codinternocomputacional")
    
    # Agregar una columna de número de fila para cada grupo de duplicados
    leads_consolidado_41 = leads_consolidado_41.withColumn("row_number", row_number().over(windowSpec))
    
    # Filtrar solo los registros con row_number igual a 1
    leads_consolidado_41 = leads_consolidado_41.filter("row_number = 1").drop("row_number")

else:
    print('sin duplicados')

#display(leads_consolidado_41.limit(2))
leads_consolidado_41.createOrReplaceTempView("df_leads_consolidado_41")




=================================================================================================================================================================





leads_unicos_41 = spark.sql(f""" select  *
                from    (
                        select  A.*,
                                ROW_NUMBER() OVER(PARTITION BY codinternocomputacional ORDER BY prioridad_lead) AS RN
                        from    df_leads_consolidado_41  A)
                where   RN = 1 """)

#display(leads_unicos_41.limit(2))
leads_unicos_41.createOrReplaceTempView("df_leads_unicos_41")






=================================================================================================================================================================





solicitudes_lead = spark.sql(f"""select a.*,
                            coalesce(b.CODMES, c.CODMES) as codmes_lead,
                            coalesce(b.tipventa, c.tipventa) as tipventa,
                            coalesce(b.tipoferta, c.tipoferta)  as tipoferta,
                            coalesce(b.descampanalead, c.descampanalead) as descampanalead,
                            coalesce(b.codcondicioncliente, c.codcondicioncliente) as codcondicioncliente,
                            coalesce(b.mtofinalofertadosol, c.mtofinalofertadosol) as mtofinalofertadosol,
                            coalesce(b.plazo, c.plazo) as plazo,
                            coalesce(b.tea, c.tea) as tea
                    from (select m.* from df_ventas_solicitudes m where LEAD_CEF not in ('REACTIVO', 'CONS APR', 'CONS CONV')) a
                    left join df_leads_unicos b
                    on    trim(a.codinternocomputacional) = trim(b.codinternocomputacional)
                    and   a.LEAD_CEF = b.LEAD_CEF
                    left join df_leads_unicos_41 c
                    on    trim(a.codinternocomputacional) = trim(c.codinternocomputacional)
                    union all
                    select  x.*,
                            coalesce(y.CODMES, z.CODMES) as CODMES,
                            coalesce(y.tipventa, z.tipventa) as tipventa,
                            coalesce(y.tipoferta, z.tipoferta) as tipoferta,
                            coalesce(y.descampanalead, z.descampanalead) as descampanalead,
                            coalesce(y.codcondicioncliente, z.codcondicioncliente) as codcondicioncliente,
                            coalesce(y.mtofinalofertadosol, z.mtofinalofertadosol) as mtofinalofertadosol,
                            coalesce(y.plazo, z.plazo) as plazo,
                            coalesce(y.tea, z.tea) as tea
                    from (select o.* from df_ventas_solicitudes o where LEAD_CEF in ('REACTIVO', 'CONS APR', 'CONS CONV')) x
                    left join df_leads_unicos y
                    on    trim(x.codinternocomputacional) = trim(y.codinternocomputacional)
                    left join df_leads_unicos_41 z
                    on    trim(x.codinternocomputacional) = trim(z.codinternocomputacional)""")

#display(solicitudes_lead.limit(6))
solicitudes_lead.createOrReplaceTempView("df_solicitudes_lead")




=================================================================================================================================================================




spark.sql(f"DROP TABLE IF EXISTS catalog_lhcl_prod_bcp.bcp_edv_rbmper.T57182_SP_Solicitudes_{codmes}")
dbutils.fs.rm(f"{mi_ruta}/SOLICITUDES_SP/SOLICITUDES/{codmes}",recurse=True) 





=================================================================================================================================================================





solicitudes_lead.write.format("delta").partitionBy("canalventarbm").save(f"{mi_ruta}/SOLICITUDES_SP/SOLICITUDES/{codmes}")

ruta = f"""CREATE TABLE catalog_lhcl_prod_bcp.bcp_edv_rbmper.T57182_SP_Solicitudes_{codmes} USING DELTA LOCATION '{mi_ruta}/SOLICITUDES_SP/SOLICITUDES/{codmes}'"""
spark.sql(ruta)




FIN PRIMER NOTEBOOK
=================================================================================================================================================================


INICIO SEGUNDO NOTEBOOK


from pyspark.sql import SparkSession
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import pandas as pd
import os 
from pyspark.sql.functions import sum, avg
from pyspark.sql.functions import months_between, to_date
from pyspark.sql.functions import col
from pyspark.sql.functions import row_number, desc, asc
from pyspark.sql.functions import row_number, min,collect_list,regexp_replace,max,sum, avg, count, months_between, to_date,countDistinct,row_number, desc, asc,date_format,expr,udf,col, when, lit, isnull,date_add,add_months

mi_ruta='abfss://bcp-edv-rbmper@adlscu1lhclbackp05.dfs.core.windows.net/data/in/T57182/'
spark = SparkSession.builder \
    .appName("Ejemplo Legacy Time Parse Policy") \
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
    .getOrCreate()

import requests
import json
import time
from datetime import datetime
from dateutil.relativedelta import relativedelta
from concurrent.futures import ThreadPoolExecutor
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, expr, sum as spark_sum, avg, lit
)
from pyspark.sql.types import DecimalType, DoubleType
from pyspark.sql import Window
from pyspark.sql.functions import floor, concat, lit, when, sum as spark_sum, col, coalesce
# Configuración
DATABRICKS_INSTANCE = "https://adb-6238163592670798.18.azuredatabricks.net"
SQL_WAREHOUSE_ID = "36b5483debcd5868"
DATABRICKS_TOKEN = "XXXX"      





=================================================================================================================================================================



%sql
CREATE OR REPLACE VIEW catalog_lhcl_prod_bcp.bcp_edv_rbmper.T57182_Solicitudes as
select * from catalog_lhcl_prod_bcp.bcp_edv_rbmper.T32611_SP_Solicitudes_202510
union all
select * from catalog_lhcl_prod_bcp.bcp_edv_rbmper.T32611_SP_Solicitudes_202509
union all
select * from catalog_lhcl_prod_bcp.bcp_edv_rbmper.T32611_SP_Solicitudes_202508
union all
select * from catalog_lhcl_prod_bcp.bcp_edv_rbmper.T32611_SP_Solicitudes_202507
union all
select * from catalog_lhcl_prod_bcp.bcp_edv_rbmper.S97084_SP_Solicitudes_202506
union all
select * from catalog_lhcl_prod_bcp.bcp_edv_rbmper.s97084_sp_solicitudes_202505
union all
select * from catalog_lhcl_prod_bcp.bcp_edv_rbmper.s97084_sp_solicitudes_202504
union all
select * from catalog_lhcl_prod_bcp.bcp_edv_rbmper.s97084_sp_solicitudes_202503
union all
select * from catalog_lhcl_prod_bcp.bcp_edv_rbmper.s97084_sp_solicitudes_202502
union all
select * from catalog_lhcl_prod_bcp.bcp_edv_rbmper.s97084_sp_solicitudes_202501
union all
select * from catalog_lhcl_prod_bcp.bcp_edv_rbmper.s97084_sp_solicitudes_202412
union all
select * from catalog_lhcl_prod_bcp.bcp_edv_rbmper.s97084_sp_solicitudes_202411
union all
select * from catalog_lhcl_prod_bcp.bcp_edv_rbmper.s97084_sp_solicitudes_202410
union all
select * from catalog_lhcl_prod_bcp.bcp_edv_rbmper.s97084_sp_solicitudes_202409
union all
select * from catalog_lhcl_prod_bcp.bcp_edv_rbmper.s97084_sp_solicitudes_202408
union all
select * from catalog_lhcl_prod_bcp.bcp_edv_rbmper.s97084_sp_solicitudes_202407
union all
select * from catalog_lhcl_prod_bcp.bcp_edv_rbmper.s97084_sp_solicitudes_202406
union all
select * from catalog_lhcl_prod_bcp.bcp_edv_rbmper.s97084_sp_solicitudes_202405
union all
select * from catalog_lhcl_prod_bcp.bcp_edv_rbmper.s97084_sp_solicitudes_202404
union all
select * from catalog_lhcl_prod_bcp.bcp_edv_rbmper.s97084_sp_solicitudes_202403
union all
select * from catalog_lhcl_prod_bcp.bcp_edv_rbmper.s97084_sp_solicitudes_202402
union all
select * from catalog_lhcl_prod_bcp.bcp_edv_rbmper.s97084_sp_solicitudes_202401








=================================================================================================================================================================





%sql
drop view if exists catalog_lhcl_prod_bcp.bcp_edv_rbmper.T57182_Solicitudes




=================================================================================================================================================================





%sql
CREATE OR REPLACE VIEW catalog_lhcl_prod_bcp.bcp_edv_rbmper.T57182_Solicitudes as
select * from catalog_lhcl_prod_bcp.bcp_edv_rbmper.T57182_SP_Solicitudes_202512
union all
select * from catalog_lhcl_prod_bcp.bcp_edv_rbmper.T32611_SP_Solicitudes_202511
union all
select * from catalog_lhcl_prod_bcp.bcp_edv_rbmper.T57182_SP_Solicitudes_202510
union all
select * from catalog_lhcl_prod_bcp.bcp_edv_rbmper.T57182_SP_Solicitudes_202509
union all
select * from catalog_lhcl_prod_bcp.bcp_edv_rbmper.T57182_SP_Solicitudes_202508
union all
select * from catalog_lhcl_prod_bcp.bcp_edv_rbmper.T57182_SP_Solicitudes_202507
union all
select * from catalog_lhcl_prod_bcp.bcp_edv_rbmper.T57182_SP_Solicitudes_202506
union all
select * from catalog_lhcl_prod_bcp.bcp_edv_rbmper.T57182_sp_solicitudes_202505
union all
select * from catalog_lhcl_prod_bcp.bcp_edv_rbmper.T57182_sp_solicitudes_202504
union all
select * from catalog_lhcl_prod_bcp.bcp_edv_rbmper.T57182_sp_solicitudes_202503
union all
select * from catalog_lhcl_prod_bcp.bcp_edv_rbmper.T57182_sp_solicitudes_202502
union all
select * from catalog_lhcl_prod_bcp.bcp_edv_rbmper.T57182_sp_solicitudes_202501
union all
select * from catalog_lhcl_prod_bcp.bcp_edv_rbmper.T57182_sp_solicitudes_202412
union all
select * from catalog_lhcl_prod_bcp.bcp_edv_rbmper.T57182_sp_solicitudes_202411
union all
select * from catalog_lhcl_prod_bcp.bcp_edv_rbmper.T57182_sp_solicitudes_202410
union all
select * from catalog_lhcl_prod_bcp.bcp_edv_rbmper.T57182_sp_solicitudes_202409
union all
select * from catalog_lhcl_prod_bcp.bcp_edv_rbmper.T57182_sp_solicitudes_202408
union all
select * from catalog_lhcl_prod_bcp.bcp_edv_rbmper.T57182_sp_solicitudes_202407
union all
select * from catalog_lhcl_prod_bcp.bcp_edv_rbmper.T57182_sp_solicitudes_202406
union all
select * from catalog_lhcl_prod_bcp.bcp_edv_rbmper.T57182_sp_solicitudes_202405
union all
select * from catalog_lhcl_prod_bcp.bcp_edv_rbmper.T57182_sp_solicitudes_202404
union all
select * from catalog_lhcl_prod_bcp.bcp_edv_rbmper.T57182_sp_solicitudes_202403
union all
select * from catalog_lhcl_prod_bcp.bcp_edv_rbmper.T57182_sp_solicitudes_202402
union all
select * from catalog_lhcl_prod_bcp.bcp_edv_rbmper.T57182_sp_solicitudes_202401






=================================================================================================================================================================







%sql
SELECT * 
FROM catalog_lhcl_prod_bcp.bcp_edv_rbmper.T57182_Solicitudes
LIMIT 2;





=================================================================================================================================================================






%sql
CREATE OR REPLACE VIEW catalog_lhcl_prod_bcp.bcp_edv_rbmper.T57182_SEGMENTO as
selecT distinct codmesevaluacion CODMES,numsolicitudevaluacion,codinternocomputacional,
case when codsegmentobancario='CON' AND codsubsegmentoconsumo='A' THEN 'Consumo A'
     when codsegmentobancario='CON' AND codsubsegmentoconsumo='B' THEN 'Consumo B'
     when codsegmentobancario='CON' AND codsubsegmentoconsumo='C' THEN 'Consumo C'
     WHEN codsegmentobancario IN ('ENA','BEX') THEN 'AFLUENTE'
     ELSE 'OTROS' END SEGMENTO -- SELECt 
from    catalog_lhcl_prod_bcp.bcp_ddv_rbmrbmper_modelogestion_vu.md_evaluacionsolicitudcredito a
where tipproductosolicitudrbm='CC' AND descanalventarbmper not in ('YAPE', 'OTROS') AND codproductosolicitudrbm='CCEF' 
--- left join catalog_lhcl_prod_bcp.bcp_ddv_rbmrbmper_seguimientoproducto_vu.md_seguimientocosechacreditoefectivo b on  a.codmesevaluacion=b.codmescosecha
--- SELECT*FROM catalog_lhcl_prod_bcp.bcp_ddv_rbmrbmper_reportlogevaluacion_vu.md_clienteevaluacioncreditorbm







=================================================================================================================================================================





%sql
DROP VIEW IF EXISTS catalog_lhcl_prod_bcp.bcp_edv_rbmper.T57182_SEGMENTO 
-- catalog_lhcl_prod_bcp.bcp_edv_rbmspeedboatcefneg_001_v.T57182_SOLICITUDES








=================================================================================================================================================================



%sql
CREATE OR REPLACE VIEW catalog_lhcl_prod_bcp.bcp_edv_rbmper.T57182_BASE as
SELECT DISTINCT
codmesevaluacion,fecevaluacion,codinternocomputacional,
CASE WHEN A.canalventarbm in ('SALEFORCE') THEN 'SALESFORCE' ELSE canalventarbm END as TIPO_CANAL,
CASE WHEN A.canalventarbm in ('SALEFORCE') THEN 'SALESFORCE' else 'DIGITAL' END CANAL_F,
'PUNTO DE CONTACTO' tipo_evaluacion,
CASE WHEN A.MTODESEMBOLSADOSOL_VTA IS NOT NULL then '1. Aprobado'
     WHEN A.resultadocda IN ('Approve') then '1. Aprobado'   
ELSE '2. Denegado' END resultado_final,
CASE WHEN a.descampanalead is not null then '1. Con lead'
ELSE '2. Sin lead' END AS TIPO_LEAD,
mtosolicitado_SLF,COALESCE(MTODESEMBOLSADOSOL_VTA,0) MTODESEMBOLSADOSOL_VTA ,resultadocda,descampana,decision
FROM  catalog_lhcl_prod_bcp.bcp_edv_rbmper.T57182_Solicitudes a
LEFT JOIN catalog_lhcl_prod_bcp.bcp_edv_rbmper.T57182_CENTRALIZADO c ON a.numsolicitudprestamo = c.numsolicitud
WHERE (a.canalventarbm = 'SALEFORCE' AND c.numsolicitud IS NULL) OR a.canalventarbm <> 'SALEFORCE'

/*WHERE (canalventarbm = 'SALEFORCE' AND a.numsolicitudprestamo NOT IN (SELECT DISTINCT numsolicitud 
      FROM catalog_lhcl_prod_bcp.bcp_edv_rbmper.T57182_CENTRALIZADO) ) OR canalventarbm <> 'SALEFORCE' */

UNION ALL   


select distinct 
A.CODMES codmesevaluacion,B.fecevaluacion,TRIM(A.cliente) codinternocomputacional,
'SALEFORCE' tipo_canal,'SALEFORCE' canal_f,'CENTRALIZADO' tipo_evaluacion,
CASE WHEN A.MTO_DESEM >0 then '1. Aprobado'
     WHEN A.ESTADO_FIN='ACEPTADAS' then '1. Aprobado'
ELSE '2. Denegado' END resultado_final,
CASE WHEN A.flg_campana='con_lead' THEN '1. Con lead'
ELSE '2. Sin lead' END AS TIPO_LEAD,
B.mtosolicitado_SLF,COALESCE(MTO_DESEM,0) MTODESEMBOLSADOSOL_VTA,B.resultadocda,campana AS descampana,B.decision
FROM catalog_lhcl_prod_bcp.bcp_edv_rbmper.T57182_CENTRALIZADO a 
left join catalog_lhcl_prod_bcp.bcp_edv_rbmper.T57182_Solicitudes b on trim(numsolicitudprestamo)=trim(a.numsolicitud) and a.codmes=b.codmesevaluacion
where a.codmes>='202401' and PRODUCTO='CC' 












=================================================================================================================================================================





%sql
DROP TABLE IF EXISTS   catalog_lhcl_prod_bcp.bcp_edv_rbmspeedboatcefneg_001_v.T57182_SOLICITUDES








=================================================================================================================================================================





%sql
CREATE TABLE catalog_lhcl_prod_bcp.bcp_edv_rbmspeedboatcefneg_001_v.T57182_SOLICITUDES
SELECT distinct  A.*, B.SEGMENTO
FROM catalog_lhcl_prod_bcp.bcp_edv_rbmper.T57182_BASE A
LEFT JOIN  catalog_lhcl_prod_bcp.bcp_edv_rbmper.T57182_SEGMENTO B ON B.CODMES=A.codmesevaluacion AND TRIM(A.codinternocomputacional)=TRIM(B.codinternocomputacional)




=================================================================================================================================================================







%sql
CREATE OR REPLACE VIEW catalog_lhcl_prod_bcp.bcp_edv_rbmper.T57182_BASE1 AS
--CREATE TABLE catalog_lhcl_prod_bcp.bcp_edv_rbmspeedboatcefneg_001_v.T57182_BASE1
select a.codmesevaluacion,
      a.fecevaluacion,
      a.codinternocomputacional,
      case 
      when D.codinternocomputacional  is not null then '1. Venta'
      when B.codinternocomputacional is  null then '2. Aprobado' 
      when C.codinternocomputacional  is  null then '3. Denegado'
      else '4. Mix'
      end as resultado_final_unico,
      TIPO_EVALUACION,
      TIPO_CANAL,
      CANAL_F,
      TIPO_LEAD,
      resultado_final,SEGMENTO,
      case when mtosolicitado_SLF is null OR mtosolicitado_SLF=0 then 0 else 1 end as flg_mtosolicitud,
      case WHEN MTODESEMBOLSADOSOL_VTA IS NULL OR MTODESEMBOLSADOSOL_VTA = 0 THEN 0 else 1 end as flg_mtoventa,
      resultadocda,
      descampana,
      decision,
      MTODESEMBOLSADOSOL_VTA,
      mtosolicitado_SLF,
      COUNT(1) OVER (PARTITION BY a.codinternocomputacional, a.codmesevaluacion) AS num_solicitudes
      from  catalog_lhcl_prod_bcp.bcp_edv_rbmspeedboatcefneg_001_v.T57182_SOLICITUDES a
         left join
          (select distinct codmesevaluacion,codinternocomputacional from  catalog_lhcl_prod_bcp.bcp_edv_rbmspeedboatcefneg_001_v.T57182_SOLICITUDES where resultado_final='2. Denegado') b on a.codmesevaluacion=b.codmesevaluacion and a.codinternocomputacional=b.codinternocomputacional
         left join
          (select distinct codmesevaluacion,codinternocomputacional from  catalog_lhcl_prod_bcp.bcp_edv_rbmspeedboatcefneg_001_v.T57182_SOLICITUDES where resultado_final='1. Aprobado') c on a.codmesevaluacion=c.codmesevaluacion and a.codinternocomputacional=c.codinternocomputacional
         left join
          (select distinct codmesevaluacion,codinternocomputacional from  catalog_lhcl_prod_bcp.bcp_edv_rbmspeedboatcefneg_001_v.T57182_SOLICITUDES where MTODESEMBOLSADOSOL_VTA > 0) d on a.codmesevaluacion=d.codmesevaluacion and a.codinternocomputacional=d.codinternocomputacional


=================================================================================================================================================================



%sql
CREATE OR REPLACE VIEW catalog_lhcl_prod_bcp.bcp_edv_rbmper.T57182_BASE2 AS
   select A.*,
          1 AS RK
     from catalog_lhcl_prod_bcp.bcp_edv_rbmper.T57182_BASE1 A
    where MTODESEMBOLSADOSOL_VTA >0

union all

     select a.*,
            ROW_NUMBER() OVER ( PARTITION BY a.codmesevaluacion, a.codinternocomputacional ORDER BY a.fecevaluacion desc) RK
       from catalog_lhcl_prod_bcp.bcp_edv_rbmper.T57182_BASE1 A
  LEFT JOIN ( select DISTINCT
                     codmesevaluacion,
                     codinternocomputacional
                from catalog_lhcl_prod_bcp.bcp_edv_rbmper.T57182_BASE1
               where MTODESEMBOLSADOSOL_VTA >0 ) B 
         ON A.codmesevaluacion=B.codmesevaluacion
        AND A.codinternocomputacional=B.codinternocomputacional
      WHERE B.codinternocomputacional is null






=================================================================================================================================================================




#######Numero de clientes con multiples solicitudes Solo Venta,aprobadas, rechazadas, Mix
mi_ruta='abfss://bcp-edv-rbmper@adlscu1lhclbackp05.dfs.core.windows.net/data/in/T57182/'
df=spark.sql("""
with 
tmp_result as (
  SELECT RESULTADO, MAX_BY(CLASIFICACION, CLASIFICACION) AS CLASIFICACION
    FROM catalog_lhcl_prod_bcp.bcp_edv_rbmper.Reglas
    GROUP BY RESULTADO 
)
  select codmesevaluacion CODMES,
         TIPO_LEAD,
         TIPO_EVALUACION,
         TIPO_CANAL,
         CANAL_F,
         resultado_final,
         resultado_final_unico,SEGMENTO,
         case when mtosolicitado_SLF is null OR mtosolicitado_SLF=0 then 0 else 1 end as flg_mtosolicitud,
         case WHEN MTODESEMBOLSADOSOL_VTA IS NULL OR MTODESEMBOLSADOSOL_VTA = 0 THEN 0 else 1 end as flg_mtoventa,
         resultadocda,
        case when decision in ('No cumple con filtro. Titular está en Archivo Negativo en estado activo reingreso o reiterativo con motivo grave.', 'No cumple con filtro. Titular está en Archivo Negativo en estado activo\\, reingreso o reiterativo con motivo grave.') then '03.Archivo Neg' 
        when decision='Perfil_SegmentoNoPermitido_Decline_Titular' then '08.Perfil Riesgos'
        when decision='ProdPasivos_BloqueoNoPermitido_Decline_Conyuge' then '05.Bloqueo BCP_Pas'
        else CLASIFICACION end CLASIFICACION,
        descampana,
        decision,
         count(1) as c,
         SUM(MTODESEMBOLSADOSOL_VTA) as MTODESEMBOLSO,
         SUM(mtosolicitado_SLF) as MTOSOLICITADO,
         SUM(num_solicitudes) as num_solicitudes
    FROM (SELECT *FROM catalog_lhcl_prod_bcp.bcp_edv_rbmper.T57182_BASE2 WHERE RK=1) a
    left join  tmp_result  b on a.decision=b.RESULTADO 
group by 
codmesevaluacion ,TIPO_LEAD,TIPO_EVALUACION,TIPO_CANAL,CANAL_F,resultado_final,resultado_final_unico,
case when mtosolicitado_SLF is null OR mtosolicitado_SLF=0 then 0 else 1 end,
      case WHEN MTODESEMBOLSADOSOL_VTA IS NULL OR MTODESEMBOLSADOSOL_VTA = 0 THEN 0 else 1 end,resultadocda,
case when decision in ('No cumple con filtro. Titular está en Archivo Negativo en estado activo reingreso o reiterativo con motivo grave.', 'No cumple con filtro. Titular está en Archivo Negativo en estado activo\\, reingreso o reiterativo con motivo grave.') then '03.Archivo Neg' 
when decision='Perfil_SegmentoNoPermitido_Decline_Titular' then '08.Perfil Riesgos'
when decision='ProdPasivos_BloqueoNoPermitido_Decline_Conyuge' then '05.Bloqueo BCP_Pas'
else CLASIFICACION end,descampana,decision,SEGMENTO
""") 

df=df.repartition(1)
df.write.format("csv").mode("overwrite").option("header", "true").save(f"{mi_ruta}/resumen") 








=================================================================================================================================================================


%sql
DROP VIEW IF EXISTS  catalog_lhcl_prod_bcp.bcp_edv_rbmper.T57182_SOLICITUDES




=================================================================================================================================================================


%sql
DROP TABLE IF EXISTS  catalog_lhcl_prod_bcp.bcp_edv_rbmper.T57182_SOLICITUDES




=================================================================================================================================================================




%sql
CREATE TABLE  catalog_lhcl_prod_bcp.bcp_edv_rbmper.T57182_SOLICITUDES
USING DELTA
LOCATION 'abfss://bcp-edv-rbmper@adlscu1lhclbackp05.dfs.core.windows.net/data/in/T57182/REVIEW29'

WITH 
tmp_result as
(
  SELECT RESULTADO, MAX_BY(CLASIFICACION, CLASIFICACION) AS CLASIFICACION
    FROM catalog_lhcl_prod_bcp.bcp_edv_rbmper.Reglas
    GROUP BY RESULTADO 
)
  select codmesevaluacion CODMES,A.codinternocomputacional,
         -- TIPO_LEAD,
         CASE WHEN TRIM(A.codinternocomputacional)=TRIM(C.codinternocomputacional) THEN 'CON_LEAD' ELSE 'SIN_LEAD' END AS TIPO_LEAD,
         TIPO_EVALUACION,
         TIPO_CANAL,
         CANAL_F,
         resultado_final,
         resultado_final_unico,
         A.SEGMENTO,
         case when mtosolicitado_SLF is null OR mtosolicitado_SLF=0 then 0 else 1 end as flg_mtosolicitud,
         case WHEN MTODESEMBOLSADOSOL_VTA IS NULL OR MTODESEMBOLSADOSOL_VTA = 0 THEN 0 else 1 end as flg_mtoventa,
         resultadocda,
        case when decision in ('No cumple con filtro. Titular está en Archivo Negativo en estado activo reingreso o reiterativo con motivo grave.', 'No cumple con filtro. Titular está en Archivo Negativo en estado activo\\, reingreso o reiterativo con motivo grave.') then '03.Archivo Neg' 
        when decision='Perfil_SegmentoNoPermitido_Decline_Titular' then '08.Perfil Riesgos'
        when decision='ProdPasivos_BloqueoNoPermitido_Decline_Conyuge' then '05.Bloqueo BCP_Pas'
        else CLASIFICACION end CLASIFICACION,
        descampana,
        decision,
         count(1) as c,
         SUM(MTODESEMBOLSADOSOL_VTA) as MTODESEMBOLSO,
         SUM(mtosolicitado_SLF) as MTOSOLICITADO,
         SUM(num_solicitudes) as num_solicitudes, 
         SUM(MTO_OFERTA) MTO_OFERTA
    FROM (SELECT *FROM catalog_lhcl_prod_bcp.bcp_edv_rbmper.T57182_BASE2 WHERE RK=1) a
    LEFT JOIN  tmp_result  b on a.decision=b.RESULTADO 
    LEFT JOIN catalog_lhcl_prod_bcp.bcp_edv_rbmper.T57182_LEADS C ON A.codmesevaluacion=C.CODMES 
              AND TRIM(A.codinternocomputacional)=TRIM(C.codinternocomputacional)
group by ALL



FIN SEGUNDO NOTEBOOK
=================================================================================================================================================================


MIRA ESTE ES UNOS NOTEBOOK QUE AL FINAL SE OBTIENE ESTA TABLA catalog_lhcl_prod_bcp.bcp_edv_rbmper.T57182_SOLICITUDES PERO SIENTO QUE HAY PASOS INNECESARIOS Y NO SE SI SE PUEDA HACER TODO CON CTEs en SQL nada mas o es necesario pyspark
TAMBIÉN QUISIERA QUE ME LISTES TODAS LAS FUENTES NECESARIOS PARA PODER LLEGAR A ESTA TABLA.




