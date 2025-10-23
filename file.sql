for nombre, df_pend in [("TCStock", df_pendientes_tcstock_final), ("CEF", df_pendientes_cef_final)]:
    coincidencias = (
        pd.merge(df_pend[["ANALISTA","FECHA"]].drop_duplicates(), trabajo_dias,
                 on=["ANALISTA","FECHA"], how="inner")
    )
    total = coincidencias.shape[0]
    print(f"{nombre} coincidencias: {total} ({total / len(df_pend):.1%} del total de pendientes)")
