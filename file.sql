Que significa esto:
TCStock coincidencias: 11 (35.5% del total de pendientes)
CEF coincidencias: 14 (40.0% del total de pendientes)



La sigueinte etapa es esta:
df_diario["FLGPENDIENTE"] = 0

df_final_validado = pd.concat(
    [df_diario, df_pendientes_tcstock_final, df_pendientes_cef_final],
    ignore_index=True
)

df_final_validado["FLGPENDIENTE"] = df_final_validado["FLGPENDIENTE"].map({0: "NO", 1: "SI"})
