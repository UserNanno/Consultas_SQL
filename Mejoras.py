# === 0) RESUMEN BASE: total por mes antes de filtrar ===
print("Totales por CODMES (antes de filtrar):")
print(df_powerapp['CODMES'].value_counts().sort_index())
print()

# === 1) DEFINIR MÁSCARAS SEGÚN TU CÓDIGO ACTUAL ===
m_tipo   = ~df_powerapp['TIPOPRODUCTO'].isin(EXCLUIR_TIPO_PRODUCTO)
m_ra     = df_powerapp['ResultadoAnalista'] == 'Denegado por Analista de credito'
m_motivo = df_powerapp['MOTIVO'] != 'NAN'  # tu criterio actual

# Intersección EXACTA de tu filtro
m_all = m_tipo & m_ra & m_motivo

# === 2) CONTAR EFECTO DEL FILTRO POR MES (ANTES DE DEDUPLICAR) ===
print("Filas que pasan el filtro (SIN dedupe) por CODMES:")
tp_pre = df_powerapp.loc[m_all, ['OPORTUNIDAD','CODMES','FECHA','MOTIVO','SUBMOTIVO']].copy()
print(tp_pre['CODMES'].value_counts().sort_index())
print()

# === 3) DEBUG PARA JUNIO (202506): ver cada condición e intersección ===
CODMES_OBJ = '202506'
df_junio = df_powerapp[df_powerapp['CODMES'] == CODMES_OBJ].copy()

m_tipo_j   = ~df_junio['TIPOPRODUCTO'].isin(EXCLUIR_TIPO_PRODUCTO)
m_ra_j     = df_junio['ResultadoAnalista'] == 'Denegado por Analista de credito'
m_motivo_j = df_junio['MOTIVO'] != 'NAN'
m_all_j    = m_tipo_j & m_ra_j & m_motivo_j

print(f"--- Debug junio {CODMES_OBJ} ---")
print("Total junio:", len(df_junio))
print("No excluidos (TIPOPRODUCTO):", m_tipo_j.sum())
print("Denegado analista (match exacto):", m_ra_j.sum())
print("MOTIVO != 'NAN':", m_motivo_j.sum())
print("Intersección (tres condiciones):", m_all_j.sum())
print()

# Tabla de combinaciones para ver qué condición está descartando:
comb_junio = (
    df_junio.assign(m_tipo=m_tipo_j, m_ra=m_ra_j, m_motivo=m_motivo_j)
            .groupby(['m_tipo','m_ra','m_motivo'])
            .size()
            .rename('conteo')
            .reset_index()
            .sort_values('conteo', ascending=False)
)
print("Combinaciones de condiciones en junio:")
print(comb_junio.to_string(index=False))
print()

# === 4) IMPACTO DEL DROP_DUPLICATES POR OPORTUNIDAD ===
# (Simula exactamente tu pipeline: después del filtro, deduplicas por OPORTUNIDAD manteniendo la PRIMERA aparición)
tp_pre2 = df_powerapp.loc[m_all, ['OPORTUNIDAD','CODMES','FECHA','MOTIVO','SUBMOTIVO']].copy()
tp_pre2['FLGMALDERIVADO'] = 1

# Conteo por mes ANTES de dedupe
cnt_pre = tp_pre2['CODMES'].value_counts().sort_index()

# Dedupe manteniendo la primera aparición (equivalente a tu código)
tp_post = tp_pre2.drop_duplicates(subset=['OPORTUNIDAD'], keep='first')
cnt_post = tp_post['CODMES'].value_counts().sort_index()

print("Conteo por mes SIN dedupe:")
print(cnt_pre)
print()
print("Conteo por mes CON dedupe (keep='first'):")
print(cnt_post)
print()

# En particular para junio:
pre_jun  = cnt_pre.get(CODMES_OBJ, 0)
post_jun = cnt_post.get(CODMES_OBJ, 0)
print(f"Junio {CODMES_OBJ} - antes dedupe: {pre_jun} | después dedupe: {post_jun}")
print()

# === 5) ¿Se están perdiendo oportunidades de junio por duplicarse en otros meses? ===
ops_jun  = set(tp_pre2.loc[tp_pre2['CODMES'] == CODMES_OBJ, 'OPORTUNIDAD'])
ops_otros = set(tp_pre2.loc[tp_pre2['CODMES'] != CODMES_OBJ, 'OPORTUNIDAD'])
repetidas_jun_vs_otros = ops_jun & ops_otros

print(f"Oportunidades de junio que también aparecen en otros meses (antes del dedupe): {len(repetidas_jun_vs_otros)}")

# (Opcional) muestra algunas para inspección:
if len(repetidas_jun_vs_otros) > 0:
    muestra = list(repetidas_jun_vs_otros)[:10]
    print("Ejemplos (hasta 10):", muestra)

# === 6) Extra útil: ¿qué mes 'gana' al deduplicar por primera aparición? ===
# Miremos, para cada OPORTUNIDAD, el primer índice (posición) y su CODMES correspondiente
tp_pre2 = tp_pre2.reset_index(drop=True)
first_idx = tp_pre2.groupby('OPORTUNIDAD', as_index=False)['CODMES'].agg(lambda s: s.index[0])
mes_ganador = tp_pre2.loc[first_idx['CODMES'], 'CODMES']  # usamos el índice almacenado arriba
# Nota: si la línea anterior te da confusión por nombre, hazlo explícito:
first_pos = tp_pre2.groupby('OPORTUNIDAD')['CODMES'].apply(lambda s: s.index[0])
mes_ganador = tp_pre2.loc[first_pos, 'CODMES'].value_counts().sort_index()

print("\nMes 'ganador' por OPORTUNIDAD (el que queda tras keep='first'):")
print(mes_ganador)
print()

# === 7) (Opcional) Comparar con un dedupe 'más reciente' (keep='last' según orden actual) ===
tp_post_last = tp_pre2.drop_duplicates(subset=['OPORTUNIDAD'], keep='last')
cnt_post_last = tp_post_last['CODMES'].value_counts().sort_index()
print("Conteo por mes CON dedupe (keep='last'):")
print(cnt_post_last)
print()

# Si aquí aparece 202506 pero no aparecía con keep='first', entonces el problema es el orden de concatenación
# (junio está después de otra ocurrencia de la misma OPORTUNIDAD y pierde con keep='first').
