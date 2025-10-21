# 4) ORGÁNICO: LECTURA MULTIMES Y MERGE POR LLAVE
#    (traer: Agencia → AGENCIA, Nombre Corto Superior → GERENTE_AGENCIA,
#            Unidad Organizativa → UNIDAD_ORGANICA, Servicio/Tribu/COE → SERVICIO_TRIBU_COE) → (conservar ambas UO)
lista_org = []
for mes in RANGO_MESES_ORGANICO:
    ruta = os.path.join(RUTA_ORGANICO_DIR, f"1n_Activos_{mes}.xlsx")
    try:
        lista_org.append(pd.read_excel(ruta))
    except Exception as e:
        print(f"[AVISO] No se pudo leer {ruta}: {e}")

if len(lista_org) == 0:
    df_organico = pd.DataFrame(columns=[
        'LlaveCodMat','Agencia','Nombre Corto Superior','Unidad Organizativa','Servicio/Tribu/COE','CODMES'
    ])
else:
    df_organico = pd.concat(lista_org, ignore_index=True)

# Asegurar existencia de columna llave
if 'LlaveCodMat' not in df_organico.columns:
    df_organico['LlaveCodMat'] = ''

cols_org = ['LlaveCodMat', 'Agencia', 'Nombre Corto Superior', 'Unidad Organizativa', 'Servicio/Tribu/COE']
faltan = [c for c in cols_org if c not in df_organico.columns]
if faltan:
    print("[AVISO] Faltan columnas en orgánico:", faltan)

# Suffixes para distinguir columnas con el mismo nombre
tp_centralizado = tp_centralizado.merge(
    df_organico[[c for c in cols_org if c in df_organico.columns]],
    left_on='LLAVEMATRICULA',
    right_on='LlaveCodMat',
    how='left',
    suffixes=('', '_ORG')  # lo de la derecha que choque tendrá sufijo _ORG
)

# Renombrar campos finales
tp_centralizado.rename(columns={
    'Agencia': 'AGENCIA',
    'Nombre Corto Superior': 'GERENTE_AGENCIA',
    'Servicio/Tribu/COE': 'SERVICIO_TRIBU_COE',
    'Unidad Organizativa': 'UnidadOrganizativa_CENTRALIZADO',  # viene del centralizado (lado izquierdo)
    'Unidad Organizativa_ORG': 'UnidadOrganizativa_ORGANICO'   # viene del orgánico (lado derecho)
}, inplace=True)

# Limpieza de llave del orgánico (si no la usas luego)
tp_centralizado.drop(columns=['LlaveCodMat'], inplace=True, errors='ignore')
