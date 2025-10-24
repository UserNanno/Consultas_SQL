# Suponiendo que:
# tp_powerapp["ANALISTA"]  → nombres de analistas en el primer DF
# df_analistas["NOMBREPOWERAPP"] → nombres válidos en el segundo DF

faltantes = tp_powerapp.loc[
    ~tp_powerapp["ANALISTA"].isin(df_analistas["NOMBREPOWERAPP"])
]

faltantes_analistas = faltantes["ANALISTA"].unique()

print(f"Analistas no encontrados ({len(faltantes_analistas)}):")
print(faltantes_analistas)
