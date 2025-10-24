# Bloque 1: imports, opciones y funciones auxiliares
import pandas as pd
import glob, os, unicodedata

pd.set_option("display.max_columns", None)
pd.set_option("display.max_rows", None)
pd.set_option("display.width", None)

def quitar_tildes(s):
    if isinstance(s, str):
        return ''.join(c for c in unicodedata.normalize('NFD', s) if unicodedata.category(c) != 'Mn')
    return s

def norm_txt(s):
    # normaliza en un solo paso: tildes -> strip -> upper
    if not isinstance(s, str):
        return s if pd.notna(s) else s
    return quitar_tildes(s).strip().upper()




# Bloque 2: lectura, selección y renombrado
PATH_POWERAPP = "INPUT/POWERAPP/"
FILES = glob.glob(os.path.join(PATH_POWERAPP, "1n_Apps_2025*.csv"))

df = pd.concat((pd.read_csv(f) for f in FILES), ignore_index=True)

cols_selected = [
    'Title', 'FechaAsignacion', 'Tipo de Producto', 'ResultadoAnalista',
    'Analista', 'Motivo Resultado Analista', 'AñoMes', 'Created',
    'Motivo_MD', 'Submotivo_MD'
]
tp_powerapp = df[cols_selected].rename(columns={
    'Title': 'OPERACION',
    'FechaAsignacion': 'FECASIGNACION',
    'Tipo de Producto': 'PRODUCTO',
    'ResultadoAnalista': 'RESULTADOANALISTA',
    'Analista': 'ANALISTA',
    'Motivo Resultado Analista': 'MOTIVORESULTADOANALISTA',
    'AñoMes': 'CODMES',
    'Created': 'CREATED',
    'Motivo_MD': 'MOTIVOMALADERIVACION',
    'Submotivo_MD': 'SUBMOTIVOMALADERIVACION'
})






# Bloque 3: normalización de texto con .loc
cols_norm = [
    'ANALISTA', 'PRODUCTO', 'RESULTADOANALISTA',
    'MOTIVORESULTADOANALISTA', 'MOTIVOMALADERIVACION', 'SUBMOTIVOMALADERIVACION'
]
for col in cols_norm:
    tp_powerapp.loc[:, col] = tp_powerapp[col].apply(norm_txt)

# Reglas de estandarización sobre texto ya normalizado
tp_powerapp.loc[:, "RESULTADOANALISTA"] = tp_powerapp["RESULTADOANALISTA"].replace({
    "DENEGADO POR ANALISTA DE CREDITO": "DENEGADO",
    "APROBADO POR ANALISTA DE CREDITO": "APROBADO",
    "DEVOLVER AL GESTOR": "DEVUELTO AL GESTOR"
})




# Bloque 4: fechas/horas (UTC -> America/Lima)
created_lima = (
    pd.to_datetime(tp_powerapp["CREATED"], utc=True, errors="coerce")
      .dt.tz_convert("America/Lima")
      .dt.floor("min")
)

tp_powerapp.loc[:, "FECHA"] = created_lima.dt.date
tp_powerapp.loc[:, "HORA"]  = created_lima.dt.time
tp_powerapp.loc[:, "FECHAHORA"] = created_lima.dt.tz_localize(None)  # naive en hora Lima







# Bloque 5: lectura y normalización de df_analistas
df_analistas = pd.read_csv('INPUT/POWERAPP/ANALISTAS.csv', sep=';', encoding='latin-1')

for col in ['NOMBREPOWERAPP', 'MATRICULA', 'NOMBRECOMPLETO']:
    if col in df_analistas.columns:
        df_analistas.loc[:, col] = df_analistas[col].apply(norm_txt)





# Bloque 6: merge ANALISTA (tp_powerapp) vs NOMBREPOWERAPP (df_analistas)
tp_powerapp = tp_powerapp.merge(
    df_analistas[['NOMBREPOWERAPP', 'MATRICULA']],
    left_on='ANALISTA', right_on='NOMBREPOWERAPP', how='left'
)

# opcional: remover la llave derecha si no la necesitas
tp_powerapp.drop(columns=['NOMBREPOWERAPP'], inplace=True)

# opcional: reordenar columnas para ver ANALISTA y MATRICULA juntos
cols = list(tp_powerapp.columns)
if 'ANALISTA' in cols and 'MATRICULA' in cols:
    cols.remove('MATRICULA')
    idx = cols.index('ANALISTA') + 1
    cols.insert(idx, 'MATRICULA')
    tp_powerapp = tp_powerapp.loc[:, cols]






# Bloque 7: chequeos rápidos
print("Registros totales:", f"{len(tp_powerapp):,}")
print("Analistas sin matrícula (post-merge):", tp_powerapp['MATRICULA'].isna().sum())
tp_powerapp[['ANALISTA','MATRICULA']].head(10)
