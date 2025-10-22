# 1) ¿Qué valores exactos trae ResultadoAnalista en junio?
print("Top ResultadoAnalista en junio:")
print(
    df_powerapp.loc[df_powerapp['CODMES']=='202506','ResultadoAnalista']
    .value_counts(dropna=False)
    .head(30)
)

# 2) ¿Cuántos pasan cada condición por separado en junio?
df_jun = df_powerapp[df_powerapp['CODMES']=='202506'].copy()
c1 = (~df_jun['TIPOPRODUCTO'].isin(EXCLUIR_TIPO_PRODUCTO)).sum()
c2 = (df_jun['ResultadoAnalista'] == 'Denegado por Analista de credito').sum()
c3 = (df_jun['MOTIVO'] != 'NAN').sum()
print(f"Junio: total={len(df_jun)} | no_excluidos={c1} | RA_exact={c2} | motivo_!=NAN={c3}")

# 3) ¿Y si ignoramos tildes/mayúsculas/espacios y exigimos MOTIVO no vacío real?
import unicodedata
def norm(s):
    s = str(s or '').strip()
    s = unicodedata.normalize('NFKD', s)
    s = ''.join(ch for ch in s if not unicodedata.combining(ch))
    return s.lower()

df_jun['RA_norm'] = df_jun['ResultadoAnalista'].map(norm)
mask_tipo   = ~df_jun['TIPOPRODUCTO'].isin(EXCLUIR_TIPO_PRODUCTO)
mask_ra     = df_jun['RA_norm'].str.contains('denegado por analista de credito', regex=False)
mask_motivo = df_jun['MOTIVO'].notna() & (df_jun['MOTIVO'].str.strip()!='')

print("Junio (norm): no_excluidos=", mask_tipo.sum(),
      "| RA_norm=", mask_ra.sum(),
      "| motivo_ok=", mask_motivo.sum(),
      "| intersección=", (mask_tipo & mask_ra & mask_motivo).sum())
