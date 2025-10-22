df_powerapp.loc[
    (~df_powerapp['TIPOPRODUCTO'].isin(EXCLUIR_TIPO_PRODUCTO)) &
    (df_powerapp['ResultadoAnalista'] == 'Denegado por Analista de credito') &
    (df_powerapp['MOTIVO'] != 'NAN') &
    (df_powerapp['CODMES'] == '202506')
].shape[0]



df_powerapp.loc[
    (~df_powerapp['TIPOPRODUCTO'].isin(EXCLUIR_TIPO_PRODUCTO)) &
    (df_powerapp['ResultadoAnalista'] == 'Denegado por Analista de credito') &
    (df_powerapp['MOTIVO'] != 'NAN') &
    (df_powerapp['CODMES'] == '202506')
, ['OPORTUNIDAD']].merge(
    df_powerapp.loc[
        (~df_powerapp['TIPOPRODUCTO'].isin(EXCLUIR_TIPO_PRODUCTO)) &
        (df_powerapp['ResultadoAnalista'] == 'Denegado por Analista de credito') &
        (df_powerapp['MOTIVO'] != 'NAN')
    , ['OPORTUNIDAD','CODMES']], on='OPORTUNIDAD'
).assign(es_jun=lambda d: d['CODMES_x']=='202506')\
.drop_duplicates('OPORTUNIDAD')\
.query("CODMES_y!='202506'").shape[0]
