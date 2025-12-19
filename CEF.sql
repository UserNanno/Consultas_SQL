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
    # normaliza - tildes -> strip -> upper
    if not isinstance(s, str):
        return s if pd.notna(s) else s
    return quitar_tildes(s).strip().upper()


PATH_POWERAPP = "INPUT/POWERAPP/"
FILES = glob.glob(os.path.join(PATH_POWERAPP, "1n_Apps_2025*.csv"))

df = pd.concat((pd.read_csv(f) for f in FILES), ignore_index=True)

cols_selected = [
    'Title', 'FechaAsignacion', 'Tipo de Producto', 'ResultadoAnalista',
    'Mail', 'Motivo Resultado Analista', 'AñoMes', 'Created',
    'Motivo_MD', 'Submotivo_MD'
]
tp_powerapp = df[cols_selected].rename(columns={
    'Title': 'CODSOLICITUD',
    'FechaAsignacion': 'FECASIGNACION',
    'Tipo de Producto': 'PRODUCTO',
    'ResultadoAnalista': 'RESULTADOANALISTA',
    'Mail': 'CORREO',
    'Motivo Resultado Analista': 'MOTIVORESULTADOANALISTA',
    'AñoMes': 'CODMES',
    'Created': 'CREATED',
    'Motivo_MD': 'MOTIVOMALADERIVACION',
    'Submotivo_MD': 'SUBMOTIVOMALADERIVACION'
})


tp_powerapp["FECASIGNACION"] = pd.to_datetime(tp_powerapp["FECASIGNACION"], errors="coerce", dayfirst=True)


tp_powerapp = tp_powerapp.sort_values(by='FECASIGNACION', ascending=False)


tp_powerapp_clean = tp_powerapp.drop_duplicates(subset=['CODSOLICITUD'], keep='first')


# Normalizacion de texto
cols_norm = [
    'PRODUCTO', 'RESULTADOANALISTA',
    'MOTIVORESULTADOANALISTA', 'MOTIVOMALADERIVACION', 'SUBMOTIVOMALADERIVACION'
]
for col in cols_norm:
    tp_powerapp_clean.loc[:, col] = tp_powerapp_clean[col].apply(norm_txt)

tp_powerapp_clean.loc[:, "RESULTADOANALISTA"] = tp_powerapp_clean["RESULTADOANALISTA"].replace({
    "DENEGADO POR ANALISTA DE CREDITO": "DENEGADO",
    "APROBADO POR ANALISTA DE CREDITO": "APROBADO",
    "DEVOLVER AL GESTOR": "DEVUELTO AL GESTOR"
})


tp_powerapp_clean = tp_powerapp_clean.copy()


# Fechas/horas (UTC -> America/Lima)
created_lima = (
    pd.to_datetime(tp_powerapp_clean["CREATED"], utc=True, errors="coerce")
      .dt.tz_convert("America/Lima")
      .dt.floor("min")
)

tp_powerapp_clean.loc[:, "FECCREACION"] = created_lima.dt.date
tp_powerapp_clean.loc[:, "HORACREACION"]  = created_lima.dt.time
tp_powerapp_clean.loc[:, "FECHORACREACION"] = created_lima.dt.tz_localize(None)


tp_powerapp_clean["FECASIGNACION"] = pd.to_datetime(
    tp_powerapp_clean["FECASIGNACION"].astype(str),
    dayfirst=True,
    errors="coerce"
)


df_organico = df_organico[df_organico['CORREO'] != '-']


correos_mes = tp_powerapp_clean[['CORREO', 'CODMES']]

df_merge = pd.merge(
    tp_powerapp_clean[['CODMES','CORREO']],
    df_organico[['CODMES','CORREO','MATORGANICO','FECINGRESO']],
    on=['CODMES','CORREO'],
    how='inner'
)

df_merge = df_merge.sort_values('FECINGRESO', ascending=False)
df_unique = df_merge.drop_duplicates(subset=['CODMES','CORREO'])


df_final = df_unique[['CODMES','MATORGANICO','CORREO']]


tp_powerapp_clean = pd.merge(
    tp_powerapp_clean,
    df_final.rename(columns={'MATORGANICO':'MATANALISTA'}),
    on=['CODMES','CORREO'],
    how='left'
)


columnas = ["CODMES", "CODSOLICITUD", "FECHORACREACION", "FECCREACION", "HORACREACION", "MATANALISTA", 
            "FECASIGNACION", "PRODUCTO", "RESULTADOANALISTA", "MOTIVORESULTADOANALISTA", "MOTIVOMALADERIVACION", "SUBMOTIVOMALADERIVACION"]

tp_powerapp = tp_powerapp_clean[columnas]


df_powerapp = tp_powerapp.drop_duplicates()

df_powerapp.to_csv(
    "OUTPUT/POWERAPP_EDV.csv", index=False, encoding="utf-8-sig", sep=';'
)






En mi proceso que lo tenía en local de jupyter notebook lo preprocesaba de esta forma
El organico es lo que ya tenemos, incluso el mismo campo correo

El ruta donde estarn todos los archivos PATH_PA_SOLICITUDES = "abfss://bcp-edv-rbmbdn@adlscu1lhclbackp05.dfs.core.windows.net/T72496/CARGA/POWERAPPS/BASESOLICITUDES/1n_Apps_*.csv"

Entonces en el mismo pipeline creemos una sección para procesar esto y luego hacerle el join 
