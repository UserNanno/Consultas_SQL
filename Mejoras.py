Va a cambiar un poco las cosas, lo que pasa que esos informes que me compartian de salesforce ya no estan disponibles pero la misma información la tengo ahora en dos nuevos reportes con columnas iguales pero en otro orden

Por ejemplo 

Antes era asI:

df_tcstock = pd.read_csv("INPUT/REPORT_TC.csv", encoding="latin1")

df_tcstock = df_tcstock[["Nombre del registro", "Estado", "Fecha de inicio del paso"]].rename(columns={
    "Nombre del registro": "OPORTUNIDAD",
    "Estado": "ESTADO",
    "Fecha de inicio del paso": "FECINICIOPASO"
})


Ahora es asi:

df_tcstock = pd.read_csv("INPUT/INFORME_ESTADO.csv", encoding="latin1")

y tiene estas columnas
"Nombre del registro","Estado","Fecha de inicio del paso","Fecha de finalización del paso","Paso: Nombre","Último actor: Nombre completo","Proceso de aprobación: Nombre","Último actor del paso: Nombre completo","Tipo de objeto","Estado del paso"


Antes era:

df_cef_tc = pd.read_csv("INPUT/REPORT_CEF_TC.csv", encoding="latin1", low_memory=False)

df_cef_tc = df_cef_tc[[
    "Nombre de la oportunidad",
    "Nombre del Producto",
    "Tipo de Acción",
    "Analista de crédito"
]].rename(columns={
    "Nombre de la oportunidad": "OPORTUNIDAD",
    "Nombre del Producto": "DESPRODUCTO",
    "Tipo de Acción": "DESTIPACCION",
    "Analista de crédito": "ANALISTACREDITO"
})

Ahora será asi:

df_cef_tc = pd.read_csv("INPUT/INFORME_PRODUCTO.csv", encoding="latin1", low_memory=False)

y tiene stas columnas:

"Nombre de la oportunidad","Nombre del Producto","Etapa","Estado de aprobación","Analista de crédito","Analista","Tipo de Acción","Fecha de inicio de evaluación","Fecha de creación","Monto/Línea Solicitud","Monto/Línea aprobada","Monto Solicitado/Ofertado Divisa","Monto Solicitado/Ofertado","Monto desembolsado Divisa","Monto desembolsado","Fecha de desembolso","Centralizado/Punto de Contacto","Divisa de la oportunidad"


En INFORME_PRODUCTO tenemos ambos productos tanto CEF como TC donde estan las mismas glosas que antes tenias en solo CEF y las glosas de TC pero eso es lo que menos me interesa, quiero detectar adecuadamnete la oportunidad que esta en estado pendiente y a que analista se le asigna

Para el analista lo tomaremos del campo "Analista de crédito" de INFORME_PRODUCTO
