df_pendientes_tcstock_final = df_pendientes_tcstock_final.rename(columns={
    "FECINICIOPASO": "FECHAHORA"
})

df_pendientes_cef_final = df_pendientes_cef_final.rename(columns={
    "FECINICIOEVALUACION": "FECHAHORA"
})
