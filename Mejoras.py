df_unido["Columna"].isna().sum()


df_unido[df_unido["Columna"].isna()]


df_unido[df_unido["Columna"].isna() | (df_unido["Columna"] == "")]


df_unido.isna().sum()
