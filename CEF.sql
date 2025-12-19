# ¿Productos tiene más de 1 fila por solicitud?
df_salesforce_productos_validos.groupBy("CODSOLICITUD").count().filter("count > 1").count()

# ¿df_productos_base es 1 fila por solicitud?
df_productos_base.groupBy("CODSOLICITUD").count().filter("count > 1").count()
