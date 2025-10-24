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
