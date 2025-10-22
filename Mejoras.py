df_junio = df_powerapp[df_powerapp['CODMES'] == '202506']

m_tipo   = ~df_junio['TIPOPRODUCTO'].isin(EXCLUIR_TIPO_PRODUCTO)
m_ra     = df_junio['ResultadoAnalista'] == 'Denegado por Analista de credito'
m_motivo = df_junio['MOTIVO'] != 'NAN'  # tu criterio actual

print("Intersección junio:", (m_tipo & m_ra & m_motivo).sum())
print("Por condición (junio):",
      m_tipo.sum(), m_ra.sum(), m_motivo.sum())

# Para ver por qué se cae, muestra conteos por cada combinación
tmp = df_junio.assign(m_tipo=m_tipo, m_ra=m_ra, m_motivo=m_motivo)
print(tmp.groupby(['m_tipo','m_ra','m_motivo']).size())
