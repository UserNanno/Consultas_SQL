df_pendientes_tcstock_base = df_pendientes_tcstock.loc[
    df_pendientes_tcstock["ESTADO"] == "Pendiente",
    ["OPORTUNIDAD", "DESTIPACCION", "ESTADO", "ANALISTA_MATCH", "FECINICIOPASO"]
].copy()

df_pendientes_tcstock_base["FECHA"] = df_pendientes_tcstock_base["FECINICIOPASO"].astype(str).str[:10]
df_pendientes_tcstock_base["HORA"] = df_pendientes_tcstock_base["FECINICIOPASO"].astype(str).str[11:]
df_pendientes_tcstock_base["FECHAHORA"] = df_pendientes_tcstock_base["FECINICIOPASO"].astype(str).str[:16]

df_pendientes_tcstock_base = df_pendientes_tcstock_base.merge(
    df_equipos[["ANALISTA", "EQUIPO"]],
    left_on="ANALISTA_MATCH", right_on="ANALISTA", how="left"
).merge(
    df_clasificacion[["NOMBRE", "EXPERTISE"]],
    left_on="ANALISTA_MATCH", right_on="NOMBRE", how="left"
)

df_pendientes_tcstock_final = df_pendientes_tcstock_base.rename(columns={
    "DESTIPACCION": "TIPOPRODUCTO",
    "ESTADO": "RESULTADOANALISTA",
    "ANALISTA_MATCH": "ANALISTA"
})[[
    "OPORTUNIDAD", "TIPOPRODUCTO", "RESULTADOANALISTA",
    "ANALISTA", "FECHA", "HORA", "EXPERTISE", "FECHAHORA", "EQUIPO"
]].copy()

df_pendientes_tcstock_final = df_pendientes_tcstock_final[
    df_pendientes_tcstock_final["TIPOPRODUCTO"].isin(["TC", "UPGRADE", "AMPLIACION", "ADICIONAL", "BT", "VENTA COMBO TC"])
]
df_pendientes_tcstock_final["FLGPENDIENTE"] = 1

cols_analista = df_pendientes_tcstock_final.loc[:, 'ANALISTA']
iguales = cols_analista.iloc[:, 0].equals(cols_analista.iloc[:, 1])

if iguales:
    df_pendientes_tcstock_final = df_pendientes_tcstock_final.loc[:, ~df_pendientes_tcstock_final.columns.duplicated()]
df_pendientes_tcstock_final.head()

df_pendientes_tcstock_final = df_pendientes_tcstock_final.drop_duplicates()
