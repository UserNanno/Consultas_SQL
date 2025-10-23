df_pendientes_tcstock_base = (
    df_pendientes_tcstock_base
      .merge(df_equipos[["ANALISTA", "EQUIPO"]],
             left_on="ANALISTA_MATCH", right_on="ANALISTA", how="left")
      .drop(columns=["ANALISTA"])
      .merge(df_clasificacion[["NOMBRE", "EXPERTISE"]],
             left_on="ANALISTA_MATCH", right_on="NOMBRE", how="left")
      .drop(columns=["NOMBRE"])
)
