import re
PATH_ORGANICO = "INPUT/ORGANICO/"
FILES = glob.glob(os.path.join(PATH_ORGANICO, "1n_Activos_2025*.xlsx"))

df_organico = pd.concat(
    (pd.read_excel(f, sheet_name=0) for f in FILES),
    ignore_index=True
)

cols_selected = ['CODMES', 'Matrícula', 'Código Area/Tribu/COE', 'Area/Tribu/COE', 'Código Servicio/Tribu/COE', 'Servicio/Tribu/COE', 'Código Unidad Organizativa', 'Unidad Organizativa', 'Código Agencia', 'Agencia', 'Código Función', 'Función', 'Matrícula Superior']  # cambia por tus columnas reales

df_organico = df_organico[cols_selected]



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

df_organico["MATRICULA_SUPERIOR"] = (
    df_organico["MATRICULA_SUPERIOR"]
    .astype(str)
    .str.strip()
    .str.upper()
    .str.replace(r'^0(?=[A-Z]\d{5})', '', regex=True)
)

cols_text = ['MATRICULA', 'AREA_TRIBU_COE', 'SERVICIO_TRIBU_COE', 'UNIDAD_ORG', 'AGENCIA', 'FUNCION', 'MATRICULA_SUPERIOR']
for col in cols_text:
    df_organico[col] = df_organico[col].apply(quitar_tildes)

for col in ['MATRICULA', 'AREA_TRIBU_COE', 'SERVICIO_TRIBU_COE', 'UNIDAD_ORG', 'AGENCIA', 'FUNCION', 'MATRICULA_SUPERIOR']:
    df_organico[col] = df_organico[col].astype(str).str.strip().str.upper()
