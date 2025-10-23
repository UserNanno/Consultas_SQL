for name in ["df_pendientes_tcstock_final", "df_pendientes_cef_final"]:
    df = locals()[name]
    df = (
        df.merge(trabajo_dias, on=["ANALISTA", "FECHA"], how="left")
          .assign(
              TRABAJO=lambda x: x["TRABAJO"].fillna(0),
              FLGPENDIENTE=lambda x: np.where(x["TRABAJO"] == 1, 1, 0)
          )
          .drop(columns=["TRABAJO"])
    )
    locals()[name] = df
