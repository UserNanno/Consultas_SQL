df_unido["FECHA_ASIGNACION"] = pd.to_datetime(df_unido["FECHA_ASIGNACION"], errors="coerce")
df_unido["codmes"] = df_unido["FECHA_ASIGNACION"].dt.strftime("%Y%m")
