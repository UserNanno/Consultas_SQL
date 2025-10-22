df_powerapp['CODMES'].value_counts().sort_index()

CODMES
202501    22972
202502    21448
202503    20574
202504    19678
202505    19579
202506    19193
202507    17866
202508    17808
202509    20931
202510    10544
Name: count, dtype: int64



df_powerapp.loc[df_powerapp['CREATED'].str.contains('2025-06', na=False)]

Si me muestra valores


df_junio = df_powerapp[df_powerapp['CODMES'] == '202506']

print(len(df_junio), "registros totales junio")
print(sum(df_junio['ResultadoAnalista'] == 'Denegado por Analista de credito'), "denegados analista")
print(sum(~df_junio['TIPOPRODUCTO'].isin(EXCLUIR_TIPO_PRODUCTO)), "no excluidos")
print(sum(df_junio['MOTIVO'] != 'NAN'), "motivo no NaN")

19193 registros totales junio
8145 denegados analista
14899 no excluidos
2471 motivo no NaN

df_powerapp['ResultadoAnalista'].unique()

array(['Denegado por Analista de credito',
       'Aprobado por Analista de credito', 'Devolver al gestor', 'FACA'],
      dtype=object)


df_powerapp.loc[df_powerapp['CODMES'] == '202506', ['TIPOPRODUCTO','ResultadoAnalista','MOTIVO','SUBMOTIVO']].head(10)

Si me muestra registros



df_powerapp.query("CODMES == '202506'").shape
(19193, 16)
tp_bad_derivadas.query("CODMES == '202506'").shape
(0, 16)

