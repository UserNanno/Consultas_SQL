df_diario["ANALISTA"] = df_diario["ANALISTA"].astype(str).str.upper().str.strip()

df_pendientes_tcstock_final["FECHA"] = pd.to_datetime(
    df_pendientes_tcstock_final["FECHA"], dayfirst=True, errors="coerce"
).dt.date

df_pendientes_cef_final["FECHA"] = pd.to_datetime(
    df_pendientes_cef_final["FECHA"], dayfirst=True, errors="coerce"
).dt.date

df_pendientes_tcstock_sin_validar = df_pendientes_tcstock_final.copy()
df_pendientes_cef_sin_validar = df_pendientes_cef_final.copy()

trabajo_dias = (
    df_diario[["ANALISTA", "FECHA"]]
    .drop_duplicates()
    .assign(TRABAJO=1)
)

df_pendientes_tcstock_final = df_pendientes_tcstock_final.merge(trabajo_dias, on=["ANALISTA", "FECHA"], how="left")
df_pendientes_tcstock_final["TRABAJO"] = df_pendientes_tcstock_final["TRABAJO"].fillna(0)
df_pendientes_tcstock_final["FLGPENDIENTE"] = np.where(df_pendientes_tcstock_final["TRABAJO"] == 1, 1, 0)
df_pendientes_tcstock_final.drop(columns=["TRABAJO"], inplace=True)

df_pendientes_cef_final = df_pendientes_cef_final.merge(trabajo_dias, on=["ANALISTA", "FECHA"], how="left")
df_pendientes_cef_final["TRABAJO"] = df_pendientes_cef_final["TRABAJO"].fillna(0)
df_pendientes_cef_final["FLGPENDIENTE"] = np.where(df_pendientes_cef_final["TRABAJO"] == 1, 1, 0)
df_pendientes_cef_final.drop(columns=["TRABAJO"], inplace=True)
