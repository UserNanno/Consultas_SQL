for nombre, df_pend in [("TCStock", df_pendientes_tcstock_final), ("CEF", df_pendientes_cef_final)]:
    base_unica = df_pend[["ANALISTA","FECHA"]].drop_duplicates()
    coincidencias = base_unica.merge(trabajo_dias, on=["ANALISTA","FECHA"], how="inner")
    total_match = len(coincidencias)
    total_base  = len(base_unica)
    pct = (total_match / total_base) if total_base else 0.0
    print(f"{nombre} coincidencias: {total_match} ({pct:.1%} del total de pendientes únicos)")


def resumen_pendientes_sin_trabajo(df_pend, trabajo_dias, nombre_fuente):
    base_unica = df_pend[["ANALISTA", "FECHA"]].drop_duplicates()

    sin_match = base_unica.merge(
        trabajo_dias,
        on=["ANALISTA", "FECHA"],
        how="left",
        indicator=True
    ).query('_merge == "left_only"').drop(columns=['_merge'])

    # Etiqueta analista vacío para que se vea claro
    sin_match["ANALISTA"] = sin_match["ANALISTA"].replace({"": "(ANALISTA VACÍO)"})

    resumen = sin_match["ANALISTA"].value_counts()
    total_pend = len(sin_match)
    total_analistas = len(resumen)

    print(f"\n{nombre_fuente}: {total_pend} pendientes sin registro en df_diario ({total_analistas} analistas)\n")
    if total_pend == 0:
        print("Todos los pendientes tienen días con registro de trabajo.\n")
    else:
        print("Analistas con pendientes sin registro:")
        for analista, cantidad in resumen.items():
            print(f"  - {analista}: {cantidad} pendiente(s)")
        print()

# Ejecutar
resumen_pendientes_sin_trabajo(df_pendientes_tcstock_final, trabajo_dias, "TCStock")
resumen_pendientes_sin_trabajo(df_pendientes_cef_final, trabajo_dias, "CEF")
