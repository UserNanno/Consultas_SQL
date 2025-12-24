eN MI ANTIGUA LOGICA LO HACIA ASI

df_final_solicitud = (
    df_final_solicitud
      .join(
          df_powerapp.select(
              "CODSOLICITUD",
              F.col("MATANALISTA").alias("MATANALISTA_APPS"),
              F.col("RESULTADOANALISTA").alias("RESULTADOANALISTA_APPS"),
              F.col("PRODUCTO").alias("PRODUCTO_APPS"),
              "MOTIVORESULTADOANALISTA",
              "MOTIVOMALADERIVACION",
              "SUBMOTIVOMALADERIVACION"
          ).dropDuplicates(["CODSOLICITUD"]),
          on="CODSOLICITUD",
          how="left"
      )
      .withColumn("MAT_ANALISTA_FINAL", F.coalesce(F.col("MAT_ANALISTA_FINAL"), F.col("MATANALISTA_APPS")))
      .withColumn("ESTADOSOLICITUD_PASO", F.coalesce(F.col("ESTADOSOLICITUD_PASO"), F.col("RESULTADOANALISTA_APPS")))
      .withColumn("NBRPRODUCTO", F.coalesce(F.col("NBRPRODUCTO"), F.col("PRODUCTO_APPS")))
      .withColumn(
          "ORIGEN_MAT_ANALISTA",
          F.when(F.col("ORIGEN_MAT_ANALISTA").isNull() & F.col("MATANALISTA_APPS").isNotNull(), F.lit("APPS_MAT"))
           .otherwise(F.col("ORIGEN_MAT_ANALISTA"))
      )
      .drop("MATANALISTA_APPS", "RESULTADOANALISTA_APPS", "PRODUCTO_APPS")
)

PARA QUE EN CASO ESOS CAMPOS SEAN NULL RECIEN TOMAMOS EN CUENTA LO DEL POWER APP Y ADICIONAL A ESTO OBTENER LOS CAMPOS MOTIVOMALADERIVACION Y SUBMOTIVOMALADERIVACION

Podemos implementar este paso más a lo que ya he armado ahora? 

también me gustaría ordenar el pipeline, quizá está algo desordenado las ultimas partes
