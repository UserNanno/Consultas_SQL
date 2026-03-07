Quiero ejecutar esto desde databricks

evaluaciones = spark.sql("""
    SELECT
        A.numsolicitudevaluacion,
        A.codmesevaluacion,
        A.fecevaluacion,
        A.codclavepartycli as codclavepartycli_a,
        A.descanalventasolicitudrbm as canal_a,
        CASE WHEN A.destipevaluacionsolicitudcredito = 'TC NUEVA' THEN 'TC Nueva'
            WHEN A.destipevaluacionsolicitudcredito = 'EP' THEN 'PTC'
            WHEN A.destipevaluacionsolicitudcredito = 'BT' THEN 'BT'
            ELSE A.destipevaluacionsolicitudcredito
            END AS producto_a,
        A.mtoingresoevaluacion AS ingresoevaluacion,
        A.mtocemevaluacion AS cemevaluacion,
        
        --CUADRE DATA
        A.codevaluacionsolicitud,
        A.desevaluacion AS flujoevaluacion,
        A.tipdecisionresultadorbm AS tipdecision,
        A.destipdecisionresultadorbm AS destipdecision,
        A.desdecisionevaluacion AS detallemotivo,
        A.desreglapauta AS reglamotivo,
        A.codcanalventasolicitudrbm AS codcanalevaluacion,
        A.descampaniasolicitud AS tipcampania,
        CASE WHEN A.descampaniasolicitud IN ('100% Aprobado','Pre-Aprobado') THEN 1
            ELSE 0
            END AS flgcampania,

        /*------------------------------------------------------------SALESFORCE------------------------------------------------------------*/
        CASE WHEN B.codsolicitudappriesgo IS NOT NULL THEN 1
            ELSE 0
            END AS flgcrucesalesforce,
        B.codsolicitud AS codsolicitudsalesforce, --cuadre

        B.codclavepartycli as codclavepartycli_b,
        CASE WHEN B.codapp = 'SCRM' THEN 'SALEFORCE'
            WHEN B.codapp = 'XT21' THEN 'CARDS'
            END AS canal_b,
        CASE WHEN B.destipflujoventatarjetacredito IN ('TC', 'SEGUNDA TC') THEN 'TC Nueva'
            WHEN B.destipflujoventatarjetacredito IN ('AMPLIACION') THEN 'Ampliacion'
            WHEN B.destipflujoventatarjetacredito IN ('ADICIONAL') THEN 'Adicional'
            WHEN B.destipflujoventatarjetacredito IN ('UPGRADE','UPGRATE') THEN 'Upgrade'
            WHEN B.destipflujoventatarjetacredito IN ('EP') THEN 'PTC'
            ELSE B.destipflujoventatarjetacredito
            END AS producto_b,
        B.destipetapa,            --INICIADO/SOLICITUD/EVALUACION/OPERACIONES-VENTAS/APROBACION DE OPERACIONES/RECHAZADA/CERRADA/DESESTIMADA/null (4905)
        CASE WHEN B.destipetapa = 'CERRADA' THEN '1.CERRADA'
            WHEN B.destipetapa = 'DESESTIMADA' THEN '2.DESESTIMADA'
            ELSE '3.OTRO'
            END AS destipetapa_f,
        B.tipevaluacionsolicitud, --EVAU/EVCE/null (970222)
        B.flgestadocda,           --N/S/null (4789)
        B.mtoaprobado,

        /*------------------------------------------------------------CENTRALIZADO------------------------------------------------------------*/
        CASE WHEN C.nrosolicitud IS NOT NULL THEN 1
            ELSE 0
            END AS flgcentralizado
    FROM catalog_lhcl_prod_bcp.bcp_ddv_rbmrbmper_modelogestion_vu.md_evaluacionsolicitudcredito A
    LEFT JOIN catalog_lhcl_prod_bcp.bcp_udv_int_vu.m_solicitudtarjetacredito B                                  --salesforce
    ON A.codidevaluacion = SUBSTRING(B.codsolicitudappriesgo, 1, LENGTH(B.codsolicitudappriesgo) - 2)
    LEFT JOIN (SELECT DISTINCT nrosolicitud FROM catalog_lhcl_prod_bcp.bcp_edv_rbmper.t57182_centralizado) C    --centralizado
    ON B.codsolicitud = C.nrosolicitud
    WHERE A.flgregeliminado = 'N' AND A.tipproductosolicitudrbm = 'TC'
    """)

#evaluaciones_fuente = spark.sql("""select * from catalog_lhcl_prod_bcp.bcp_ddv_rbmrbmper_modelogestion_vu.md_evaluacionsolicitudcredito
#                                WHERE flgregeliminado = 'N' AND tipproductosolicitudrbm = 'TC'""")
#print(f"Registros fuente: {evaluaciones_fuente.count()}")

#print(f"Registros base: {evaluaciones.count()}")
#display(evaluaciones)
evaluaciones.createOrReplaceTempView("evaluaciones")

PERO ME SALE ERROR

AnalysisException: [INSUFFICIENT_PERMISSIONS] Insufficient privileges:
User does not have USE SCHEMA on Schema 'catalog_lhcl_prod_bcp.bcp_edv_rbmper'.



pero necesito permisos y para ello uso esto

%pip install --upgrade databricks-sql-connector
dbutils.library.restartPython()  # o reinicia el cluster/kernel manualmente


from databricks import sql
import os

connection = sql.connect(
                        server_hostname = "adb-6238163592670798.18.azuredatabricks.net",
                        http_path = "/sql/1.0/warehouses/510f1d9e2d1b2250",
                        access_token = "<access-token>")

cursor = connection.cursor()

cursor.execute("SELECT * from range(10)")
print(cursor.fetchall())

cursor.close()
connection.close()


Cuando ejecuto esto ya me funciona leer los catalogos
