df_pendientes_cef_base = df_pendientes_cef.loc[
    df_pendientes_cef["ESTADO"] == "Pendiente",
    ["OPORTUNIDAD", "DESPRODUCTO", "ESTADOAPROBACION", "ANALISTA", "FECINICIOEVALUACION"]
].copy()

df_pendientes_cef_base["FECHA"] = df_pendientes_cef_base["FECINICIOEVALUACION"].astype(str).str[:10]
df_pendientes_cef_base["HORA"] = df_pendientes_cef_base["FECINICIOEVALUACION"].astype(str).str[11:]
df_pendientes_cef_base["FECHAHORA"] = df_pendientes_cef_base["FECINICIOEVALUACION"].astype(str).str[:16]
df_pendientes_cef_base["DESPRODUCTO"] = df_pendientes_cef_base["DESPRODUCTO"].astype(str).str.upper()

prod_cef = {
    "CRÉDITOS PERSONALES MICROCREDITOS",
    "CREDITOS PERSONALES MICROCREDITOS",
    "CRÉDITOS PERSONALES EFECTIVO MP",
    "CREDITOS PERSONALES EFECTIVO MP",
    "CONVENIO DESCUENTOS POR PLANILLA"
}
df_pendientes_cef_base = df_pendientes_cef_base[df_pendientes_cef_base["DESPRODUCTO"].isin(prod_cef)]

df_pendientes_cef_base = df_pendientes_cef_base.merge(
    df_equipos[["ANALISTA", "EQUIPO"]],
    on="ANALISTA", how="left"
).merge(
    df_clasificacion[["NOMBRE", "EXPERTISE"]],
    left_on="ANALISTA", right_on="NOMBRE", how="left"
)

df_pendientes_cef_final = df_pendientes_cef_base.rename(columns={
    "DESPRODUCTO": "TIPOPRODUCTO",
    "ESTADOAPROBACION": "RESULTADOANALISTA"
})[[
    "OPORTUNIDAD", "TIPOPRODUCTO", "RESULTADOANALISTA",
    "ANALISTA", "FECHA", "HORA", "EXPERTISE", "FECHAHORA", "EQUIPO"
]].copy()

df_pendientes_cef_final["FLGPENDIENTE"] = 1

df_pendientes_cef_final = df_pendientes_cef_final.drop_duplicates()
