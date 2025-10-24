import pandas as pd
import glob
import os

pd.set_option("display.max_columns", None)
pd.set_option("display.max_rows", None)
pd.set_option("display.width", None)

PATH_POWERAPP = "INPUT/POWERAPP/"
FILES = glob.glob(os.path.join(PATH_POWERAPP, "1n_Apps_2025*.csv"))

df = pd.concat((pd.read_csv(f) for f in archivos), ignore_index=True)

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

tp_powerapp["ANALISTA"] = tp_powerapp["ANALISTA"].astype(str).str.strip().str.upper()
tp_powerapp["PRODUCTO"] = tp_powerapp["PRODUCTO"].astype(str).str.strip().str.upper()
tp_powerapp["RESULTADOANALISTA"] = tp_powerapp["RESULTADOANALISTA"].astype(str).str.strip().str.upper()
tp_powerapp["SUBMOTIVOMALADERIVACION"] = tp_powerapp["SUBMOTIVOMALADERIVACION"].astype(str).str.strip().str.upper()

created_lima = (
    pd.to_datetime(tp_powerapp["CREATED"], utc=True, errors="coerce")
      .dt.tz_convert("America/Lima")
      .dt.floor("min")
)

tp_powerapp["FECHA"] = created_lima.dt.date
tp_powerapp["HORA"]  = created_lima.dt.time
tp_powerapp["FECHAHORA"] = created_lima.dt.tz_localize(None)
