df_powerapp['CODMES'].value_counts().sort_index()


df_powerapp.loc[df_powerapp['CREATED'].str.contains('2025-06', na=False)]



tp_bad_derivadas = df_powerapp[
    (~df_powerapp['TIPOPRODUCTO'].isin(EXCLUIR_TIPO_PRODUCTO)) &
    (df_powerapp['ResultadoAnalista'] == 'Denegado por Analista de credito') &
    (df_powerapp['MOTIVO'] != 'NAN')
]




df_junio = df_powerapp[df_powerapp['CODMES'] == '202506']

print(len(df_junio), "registros totales junio")
print(sum(df_junio['ResultadoAnalista'] == 'Denegado por Analista de credito'), "denegados analista")
print(sum(~df_junio['TIPOPRODUCTO'].isin(EXCLUIR_TIPO_PRODUCTO)), "no excluidos")
print(sum(df_junio['MOTIVO'] != 'NAN'), "motivo no NaN")



df_powerapp['ResultadoAnalista'].unique()


df_powerapp.loc[df_powerapp['CODMES'] == '202506', ['TIPOPRODUCTO','ResultadoAnalista','MOTIVO','SUBMOTIVO']].head(10)



df_powerapp.query("CODMES == '202506'").shape
tp_bad_derivadas.query("CODMES == '202506'").shape
