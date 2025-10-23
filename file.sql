# PROPUESTO (datetime)
df_pendientes_tcstock_base["FECHA"] = df_pendientes_tcstock_base["FECINICIOPASO"].dt.date
df_pendientes_tcstock_base["HORA"] = df_pendientes_tcstock_base["FECINICIOPASO"].dt.time
df_pendientes_tcstock_base["FECHAHORA"] = df_pendientes_tcstock_base["FECINICIOPASO"].dt.floor("min")
