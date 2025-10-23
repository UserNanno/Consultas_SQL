def resumen_pendientes_sin_trabajo(df_pend, trabajo_dias, nombre_fuente):
    # Pendientes que no tienen match con df_diario
    sin_match = pd.merge(
        df_pend[["ANALISTA", "FECHA"]].drop_duplicates(),
        trabajo_dias,
        on=["ANALISTA", "FECHA"],
        how="left",
        indicator=True
    ).query('_merge == "left_only"')
    
    # Resumen por analista
    resumen = sin_match["ANALISTA"].value_counts()
    
    total_pend = sin_match.shape[0]
    total_analistas = resumen.shape[0]
    
    print(f"\n🔹 {nombre_fuente}: {total_pend} pendientes sin registro en df_diario ({total_analistas} analistas)\n")
    
    if total_pend == 0:
        print("✅ Todos los pendientes tienen días con registro de trabajo.\n")
    else:
        print("Analistas con pendientes sin registro:")
        for analista, cantidad in resumen.items():
            print(f"  - {analista}: {cantidad} pendiente(s)")
        print()

# Ejecutar para ambas fuentes
resumen_pendientes_sin_trabajo(df_pendientes_tcstock_final, trabajo_dias, "TCStock")
resumen_pendientes_sin_trabajo(df_pendientes_cef_final, trabajo_dias, "CEF")




La sigueinte etapa es esta:
df_diario["FLGPENDIENTE"] = 0

df_final_validado = pd.concat(
    [df_diario, df_pendientes_tcstock_final, df_pendientes_cef_final],
    ignore_index=True
)

df_final_validado["FLGPENDIENTE"] = df_final_validado["FLGPENDIENTE"].map({0: "NO", 1: "SI"})


