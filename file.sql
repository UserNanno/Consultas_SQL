# Seleccionás solo las columnas que te interesan
df_pendientes_tcstock_final = df_pendientes_tcstock_base[
    ["OPORTUNIDAD", "DESTIPACCION", "ESTADO",
     "ANALISTA_MATCH", "FECHA", "HORA", "EXPERTISE", "FECHAHORA", "EQUIPO"]
].copy()

# Luego renombrás las que cambian de nombre
df_pendientes_tcstock_final.rename(columns={
    "DESTIPACCION": "TIPOPRODUCTO",
    "ESTADO": "RESULTADOANALISTA",
    "ANALISTA_MATCH": "ANALISTA"
}, inplace=True)
