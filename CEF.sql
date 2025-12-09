import re

# Limpieza de nombres
df_salesforce['NBRANALISTA_CLEAN'] = df_salesforce['NBRANALISTA'].str.upper().apply(
    lambda x: re.sub(r'\(CESADO\)', '', str(x)).strip()
)
df_organico['NOMBRECOMPLETO_CLEAN'] = df_organico['NOMBRECOMPLETO'].str.upper().apply(
    lambda x: re.sub(r'\(CESADO\)', '', str(x)).strip()
)


# Crear llave solo con el nombre ordenado (sin mes)
df_salesforce['KEY_NAME'] = df_salesforce['NBRANALISTA_CLEAN'].apply(
    lambda x: tuple(sorted(x.split()))
)
df_organico['KEY_NAME'] = df_organico['NOMBRECOMPLETO_CLEAN'].apply(
    lambda x: tuple(sorted(x.split()))
)







PATH_ORGANICO = "INPUT/ORGANICO/"
FILES = glob.glob(os.path.join(PATH_ORGANICO, "1n_Activos_2025*.xlsx"))

df_organico = pd.concat(
    (pd.read_excel(f, sheet_name=0) for f in FILES),
    ignore_index=True
)


cols_selected = [
    'CODMES', 
    'Matrícula',
    'Nombre Completo',
    'Correo electronico',
    'Fecha Ingreso',
    'Matrícula Superior'
]

df_organico = df_organico[cols_selected].rename(columns={
    'CODMES': 'CODMES',
    'Matrícula': 'MATORGANICO',
    'Nombre Completo': 'NOMBRECOMPLETO',
    'Correo electronico': 'CORREO',
    'Fecha Ingreso': 'FECINGRESO',
    'Matrícula Superior': 'MATSUPERIOR'
})


# normaliza - tildes -> strip -> upper
cols_norm = [
    'MATORGANICO', 'NOMBRECOMPLETO', 'MATSUPERIOR'
]

for col in cols_norm:
    df_organico.loc[:, col] = df_organico[col].apply(norm_txt)


# Quitar 0 inicial
df_organico.loc[:, "MATSUPERIOR"] = (
    df_organico["MATSUPERIOR"]
      .astype(str)
      .str.replace(r'^0(?=[A-Z]\d{5})', '', regex=True)
)


df_organico['FECINGRESO'] = pd.to_datetime(df_organico['FECINGRESO'], errors='coerce')


