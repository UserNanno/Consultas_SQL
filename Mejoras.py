Luego volvio a hacer esto:

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



Es lo mismo, solo que cambio el nombre del df no? 
