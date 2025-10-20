# Convertir FECHAHORA a tipo datetime si aún no lo está
df_diario["FECHAHORA"] = pd.to_datetime(df_diario["FECHAHORA"], errors="coerce")

# Ordenar por fecha y quedarse con el último registro por OPORTUNIDAD
df_diario = df_diario.sort_values("FECHAHORA").drop_duplicates(subset="OPORTUNIDAD", keep="last")
