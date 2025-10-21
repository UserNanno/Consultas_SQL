# ============================================
# 4.1) NUEVAS COLUMNAS: FLGQUINCENA, EQUIPOBEX, EQUIPOENALTA
#     (usar UnidadOrganizativa del CENTRALIZADO como llave)
# ============================================

# 1) FLGQUINCENA: Dia <= 15 => 1, si no 0
tp_centralizado['FLGQUINCENA'] = (
    pd.to_numeric(tp_centralizado['Dia'], errors='coerce').le(15)
).astype(int)

# 2) EQUIPOBEX (join por Área)
df_bex = pd.read_csv('INPUT/BEX.csv', encoding='utf-8-sig', usecols=['AREA', 'EQUIPO'])
bex_map = df_bex.set_index('AREA')['EQUIPO']
tp_centralizado['EQUIPOBEX'] = tp_centralizado['UnidadOrganizativa_CENTRALIZADO'].map(bex_map)

# 3) EQUIPOENALTA (join por Área)
df_enalta = pd.read_csv('INPUT/ENALTA.csv', encoding='utf-8-sig', usecols=['AREA', 'EQUIPO'])
enalta_map = df_enalta.set_index('AREA')['EQUIPO']
tp_centralizado['EQUIPOENALTA'] = tp_centralizado['UnidadOrganizativa_CENTRALIZADO'].map(enalta_map)

# Nota: si en lugar de la del centralizado quieres usar la UO de orgánico, 
# cambia 'UnidadOrganizativa_CENTRALIZADO' por 'UnidadOrganizativa_ORGANICO' en los .map() de arriba.







cols_vista = [
    'FLGMALDERIVADO','MOTIVO_MALDERIVADO',
    'CODMES','LLAVEMATRICULA',
    'AGENCIA','GERENTE_AGENCIA',
    'UnidadOrganizativa_CENTRALIZADO','UnidadOrganizativa_ORGANICO','SERVICIO_TRIBU_COE',
    'FLGQUINCENA','EQUIPOBEX','EQUIPOENALTA',
    'TIEMPO_ASESOR','FLG_TIEMPO'
]
print(tp_centralizado[[c for c in cols_vista if c in tp_centralizado.columns]].head(10))
