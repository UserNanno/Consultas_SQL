# ============================================
# 4.1) NUEVAS COLUMNAS: FLGQUINCENA, AREABEX, AREAENALTA
#     (usando la UO del CENTRALIZADO como EQUIPO para buscar su AREA)
# ============================================

# 1) FLGQUINCENA: Dia <= 15 => 1, si no 0
tp_centralizado['FLGQUINCENA'] = (
    pd.to_numeric(tp_centralizado['Dia'], errors='coerce').le(15)
).astype(int)

# 2) AREABEX: map por EQUIPO desde BEX.csv (traer AREA)
df_bex = pd.read_csv('INPUT/BEX.csv', encoding='utf-8-sig', usecols=['AREA', 'EQUIPO'])
bex_map = df_bex.set_index('EQUIPO')['AREA']
tp_centralizado['AREABEX'] = tp_centralizado['UnidadOrganizativa_CENTRALIZADO'].map(bex_map)

# 3) AREAENALTA: map por EQUIPO desde ENALTA.csv (traer AREA)
df_enalta = pd.read_csv('INPUT/ENALTA.csv', encoding='utf-8-sig', usecols=['AREA', 'EQUIPO'])
enalta_map = df_enalta.set_index('EQUIPO')['AREA']
tp_centralizado['AREAENALTA'] = tp_centralizado['UnidadOrganizativa_CENTRALIZADO'].map(enalta_map)

# Si prefieres usar la UO del orgánico para estos cruces, cambia
# 'UnidadOrganizativa_CENTRALIZADO' -> 'UnidadOrganizativa_ORGANICO' en los .map() de arriba.
