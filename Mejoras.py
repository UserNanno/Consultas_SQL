Mira esto hicieron en pyspark

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





validacion = spark.sql(f""" 
            select  CANAL_MDPREST_VTA, count(*), count(distinct codclavepartycli_VTA), SUM(mtodesembolsado_VTA)
            from    df_ventas_consolidado
            group by CANAL_MDPREST_VTA 
""")

#display(validacion)








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







leads_unicos_41 = spark.sql(f""" select  *
                from    (
                        select  A.*,
                                ROW_NUMBER() OVER(PARTITION BY codinternocomputacional ORDER BY prioridad_lead) AS RN
                        from    df_leads_consolidado_41  A)
                where   RN = 1 """)

#display(leads_unicos_41.limit(2))
leads_unicos_41.createOrReplaceTempView("df_leads_unicos_41")








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





spark.sql(f"DROP TABLE IF EXISTS catalog_lhcl_prod_bcp.bcp_edv_rbmper.T57182_SP_Solicitudes_{codmes}")
dbutils.fs.rm(f"{mi_ruta}/SOLICITUDES_SP/SOLICITUDES/{codmes}",recurse=True) 



solicitudes_lead.write.format("delta").partitionBy("canalventarbm").save(f"{mi_ruta}/SOLICITUDES_SP/SOLICITUDES/{codmes}")

ruta = f"""CREATE TABLE catalog_lhcl_prod_bcp.bcp_edv_rbmper.T57182_SP_Solicitudes_{codmes} USING DELTA LOCATION '{mi_ruta}/SOLICITUDES_SP/SOLICITUDES/{codmes}'"""
spark.sql(ruta)
