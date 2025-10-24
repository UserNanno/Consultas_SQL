import pandas as pd
import glob
import os

pd.set_option("display.max_columns", None)
pd.set_option("display.max_rows", None)
pd.set_option("display.width", None)

PATH_POWERAPP = "INPUT/POWERAPP/"
FILES = glob.glob(os.path.join(PATH_POWERAPP, "1n_Apps_2025*.csv"))

df = pd.concat((pd.read_csv(f) for f in FILES), ignore_index=True)

cols_selected = ['Title', 'FechaAsignacion', 'Tipo de Producto', 'ResultadoAnalista', 'Analista', 'Motivo Resultado Analista', 'AñoMes', 'Created', 'Motivo_MD', 'Submotivo_MD']  # cambia por tus columnas reales
tp_powerapp = df[cols_selected]


tp_powerapp = tp_powerapp.rename(columns={
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

for col in ['ANALISTA', 'PRODUCTO', 'RESULTADOANALISTA', 'SUBMOTIVOMALADERIVACION']:
    tp_powerapp[col] = tp_powerapp[col].astype(str).str.strip().str.upper()

created_lima = (
    pd.to_datetime(tp_powerapp["CREATED"], utc=True, errors="coerce")
      .dt.tz_convert("America/Lima")
      .dt.floor("min")
)

tp_powerapp["FECHA"] = created_lima.dt.date
tp_powerapp["HORA"]  = created_lima.dt.time
tp_powerapp["FECHAHORA"] = created_lima.dt.tz_localize(None)



import unicodedata

def quitar_tildes(s):
    if isinstance(s, str):
        return ''.join(
            c for c in unicodedata.normalize('NFD', s)
            if unicodedata.category(c) != 'Mn'
        )
    return s

cols_text = ['PRODUCTO', 'RESULTADOANALISTA', 'MOTIVORESULTADOANALISTA', 'MOTIVOMALADERIVACION', 'SUBMOTIVOMALADERIVACION']
for col in cols_text:
    tp_powerapp[col] = tp_powerapp[col].apply(quitar_tildes)


tp_powerapp["RESULTADOANALISTA"] = tp_powerapp["RESULTADOANALISTA"].replace({
    "DENEGADO POR ANALISTA DE CREDITO": "DENEGADO",
    "APROBADO POR ANALISTA DE CREDITO": "APROBADO",
    "DEVOLVER AL GESTOR": "DEVUELTO AL GESTOR"
})


df_analistas = pd.read_csv('INPUT/POWERAPP/ANALISTAS.csv', sep=';', encoding='latin-1')

for col in ['NOMBREPOWERAPP', 'MATRICULA', 'NOMBRECOMPLETO']:
    df_analistas[col] = df_analistas[col].astype(str).str.strip().str.upper()
