print("TCStock coincidencias:", (pd.merge(
    df_pendientes_tcstock_final[["ANALISTA","FECHA"]].drop_duplicates(),
    trabajo_dias, on=["ANALISTA","FECHA"], how="inner"
).shape[0]))

print("CEF coincidencias:", (pd.merge(
    df_pendientes_cef_final[["ANALISTA","FECHA"]].drop_duplicates(),
    trabajo_dias, on=["ANALISTA","FECHA"], how="inner"
).shape[0]))
