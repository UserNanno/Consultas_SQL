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
