df_derivadas.loc[:, "FechaAsignacion"] = pd.to_datetime(df_derivadas["FechaAsignacion"], errors="coerce")
