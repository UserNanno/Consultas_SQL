# Bloque O1: imports, paths y lectura
import pandas as pd
import glob, os

PATH_ORGANICO = "INPUT/ORGANICO/"
FILES = glob.glob(os.path.join(PATH_ORGANICO, "1n_Activos_2025*.xlsx"))

df_organico = pd.concat(
    (pd.read_excel(f, sheet_name=0) for f in FILES),
    ignore_index=True
)


# Bloque O2: selección + renombrado
cols_selected = [
    'CODMES', 'Matrícula', 'Código Area/Tribu/COE', 'Area/Tribu/COE',
    'Código Servicio/Tribu/COE', 'Servicio/Tribu/COE',
    'Código Unidad Organizativa', 'Unidad Organizativa',
    'Código Agencia', 'Agencia',
    'Código Función', 'Función',
    'Matrícula Superior'
]

df_organico = df_organico[cols_selected].rename(columns={
    'CODMES': 'CODMES',
    'Matrícula': 'MATRICULA',
    'Código Area/Tribu/COE': 'COD_AREA_TRIBU_COE',
    'Area/Tribu/COE': 'AREA_TRIBU_COE',
    'Código Servicio/Tribu/COE': 'COD_SERVICIO_TRIBU_COE',
    'Servicio/Tribu/COE': 'SERVICIO_TRIBU_COE',
    'Código Unidad Organizativa': 'COD_UNIDAD_ORG',
    'Unidad Organizativa': 'UNIDAD_ORG',
    'Código Agencia': 'COD_AGENCIA',
    'Agencia': 'AGENCIA',
    'Código Función': 'COD_FUNCION',
    'Función': 'FUNCION',
    'Matrícula Superior': 'MATRICULA_SUPERIOR'
})



# Bloque O3: normalización de texto (tildes -> strip -> upper) con .loc
cols_norm = [
    'MATRICULA', 'AREA_TRIBU_COE', 'SERVICIO_TRIBU_COE',
    'UNIDAD_ORG', 'AGENCIA', 'FUNCION', 'MATRICULA_SUPERIOR'
]

for col in cols_norm:
    df_organico.loc[:, col] = df_organico[col].apply(norm_txt)



# Bloque O4: quitar 0 inicial si sigue el patrón LETRA + 5 dígitos (p.ej. "0A12345" -> "A12345")
df_organico.loc[:, "MATRICULA_SUPERIOR"] = (
    df_organico["MATRICULA_SUPERIOR"]
      .astype(str)
      .str.replace(r'^0(?=[A-Z]\d{5})', '', regex=True)
)


# Bloque O5: orden de columnas (opcional)
orden_columnas = [
    'CODMES',
    'MATRICULA', 'MATRICULA_SUPERIOR',
    'COD_AREA_TRIBU_COE', 'AREA_TRIBU_COE',
    'COD_SERVICIO_TRIBU_COE', 'SERVICIO_TRIBU_COE',
    'COD_UNIDAD_ORG', 'UNIDAD_ORG',
    'COD_AGENCIA', 'AGENCIA',
    'COD_FUNCION', 'FUNCION'
]
orden_final = [c for c in orden_columnas if c in df_organico.columns]
df_organico = df_organico.loc[:, orden_final]
