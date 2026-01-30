df_estados.groupBy("CODMESEVALUACION").count().orderBy("CODMESEVALUACION").show()

df_last_estado.groupBy("CODMESEVALUACION").count().orderBy("CODMESEVALUACION").show()

df_final.groupBy("CODMESEVALUACION").count().orderBy("CODMESEVALUACION").show()

df_final.filter("CODMESEVALUACION is null").count()

df_final.filter("CODMESEVALUACION is null").select("CODSOLICITUD","TS_ULTIMO_EVENTO_ESTADOS").show(20,False)
