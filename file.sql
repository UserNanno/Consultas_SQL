De mi base también tengo estos campos extras quizá podamos utilziarlos para generar cortes/filtros en power bi mejor

schema = StructType([
    StructField("OPORTUNIDAD", StringType(), True),
    StructField("CODINTERNOCOMPUTACIONAL", StringType(), True),
    StructField("TIPPRODUCTO", StringType(), True),
    StructField("TIPOPERACION", StringType(), True),
    StructField("PRODUCTO", StringType(), True),
    StructField("CAMPANIA", StringType(), True),
    StructField("TIPEVALUACION", StringType(), True),
    StructField("ESTADO", StringType(), True),
    StructField("MTOSOLICITADO", StringType(), True),
    StructField("MTOAPROBADO", IntegerType(), True),
    StructField("MTODESEMBOLSADO", IntegerType(), True),
    StructField("FECCREACION", StringType(), True),
    StructField("SEGMENTO", StringType(), True)
])
