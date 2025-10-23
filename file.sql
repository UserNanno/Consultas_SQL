for nombre, df_pend in [("TCStock", df_pendientes_tcstock_final), ("CEF", df_pendientes_cef_final)]:
    coincidencias = (
        pd.merge(df_pend[["ANALISTA","FECHA"]].drop_duplicates(), trabajo_dias,
                 on=["ANALISTA","FECHA"], how="inner")
    )
    total = coincidencias.shape[0]
    print(f"{nombre} coincidencias: {total} ({total / len(df_pend):.1%} del total de pendientes)")

TCStock coincidencias: 11 (35.5% del total de pendientes)
CEF coincidencias: 14 (40.0% del total de pendientes)


def resumen_pendientes_sin_trabajo(df_pend, trabajo_dias, nombre_fuente):
    # Pendientes sin match con df_diario
    sin_match = pd.merge(
        df_pend[["ANALISTA", "FECHA"]].drop_duplicates(),
        trabajo_dias,
        on=["ANALISTA", "FECHA"],
        how="left",
        indicator=True
    ).query('_merge == "left_only"')
    
    resumen = sin_match["ANALISTA"].value_counts()
    
    total_pend = sin_match.shape[0]
    total_analistas = resumen.shape[0]
    
    print(f"\n{nombre_fuente}: {total_pend} pendientes sin registro en df_diario ({total_analistas} analistas)\n")
    
    if total_pend == 0:
        print("Todos los pendientes tienen días con registro de trabajo.\n")
    else:
        print("Analistas con pendientes sin registro:")
        for analista, cantidad in resumen.items():
            print(f"  - {analista}: {cantidad} pendiente(s)")
        print()

resumen_pendientes_sin_trabajo(df_pendientes_tcstock_final, trabajo_dias, "TCStock")
resumen_pendientes_sin_trabajo(df_pendientes_cef_final, trabajo_dias, "CEF")


TCStock: 9 pendientes sin registro en df_diario (9 analistas)

Analistas con pendientes sin registro:
  - JESSICA FARRO CRUZ: 1 pendiente(s)
  - FRANKEL MAYERS CHUQUICHAICO RECUAY: 1 pendiente(s)
  - LOURDES AGUILAR TORRE: 1 pendiente(s)
  - JACQUELINE IZAGUIRRE PUMACAYO: 1 pendiente(s)
  - NARDA TRIGUEROS CRUZ: 1 pendiente(s)
  - MARVIN VIRHUEZ BLANCAS: 1 pendiente(s)
  - GABRIELA VILCHEZ RAMOS: 1 pendiente(s)
  - : 1 pendiente(s)
  - CONNIE YUPANQUI R.: 1 pendiente(s)


CEF: 2 pendientes sin registro en df_diario (2 analistas)

Analistas con pendientes sin registro:
  - JOSE PACHECO V.: 1 pendiente(s)
  - BARBARA PRADO MAZA: 1 pendiente(s)
