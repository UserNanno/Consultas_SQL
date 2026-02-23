Mira esto tenia en mi jupyter notebok que trabajaba de forma local
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
FILES = glob.glob(os.path.join(PATH_POWERAPP, "1n_Apps_202*.csv"))

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

# Fechas/horas (UTC -> America/Lima)
created_lima = (
    pd.to_datetime(tp_powerapp["CREATED"], utc=True, errors="coerce")
      .dt.tz_convert("America/Lima")
      .dt.floor("min")
)

tp_powerapp.loc[:, "FECCREACION"] = created_lima.dt.date
tp_powerapp.loc[:, "HORACREACION"]  = created_lima.dt.time
tp_powerapp.loc[:, "FECHORACREACION"] = created_lima.dt.tz_localize(None)



tp_powerapp = tp_powerapp.sort_values(
    by=["FECHORACREACION", "FECASIGNACION"],
    ascending=[False, False],
    na_position="last"  # NaT al final
)

tp_powerapp_clean = tp_powerapp.drop_duplicates(subset=["CODSOLICITUD"], keep="first").copy()


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


	--- Estoy ytratando de replicarlo en databricks con spark

	
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import DecimalType

PATH_PA_SOLICITUDES = "abfss://bcp-edv-rbmbdn@adlscu1lhclbackp05.dfs.core.windows.net/T72496/CARGA/POWERAPPS/1n_Apps_*.csv"

def parse_fecha_hora_esp(col):
    s = col.cast("string")
    s = F.regexp_replace(s, u"\u00A0", " ")
    s = F.regexp_replace(s, u"\u202F", " ")
    s = F.lower(F.trim(s))
    s = F.regexp_replace(s, r"\s+", " ")
    s = F.regexp_replace(s, r"(?i)a\W*m\W*", "AM")
    s = F.regexp_replace(s, r"(?i)p\W*m\W*", "PM")
    return F.to_timestamp(s, "dd/MM/yyyy hh:mm a")



def quitar_tildes(col):
    c = F.regexp_replace(col, "[ÁÀÂÄáàâä]", "A")
    c = F.regexp_replace(c, "[ÉÈÊËéèêë]", "E")
    c = F.regexp_replace(c, "[ÍÌÎÏíìîï]", "I")
    c = F.regexp_replace(c, "[ÓÒÔÖóòôö]", "O")
    c = F.regexp_replace(c, "[ÚÙÛÜúùûü]", "U")
    return c

def norm_txt(col):
    """
    Normalización 'robusta':
    - upper
    - quitar tildes
    - normalizar espacios
    - dejar letras/números/espacios (reduce ruido de caracteres especiales)
    """
    c = col.cast("string")
    c = F.upper(c)
    c = quitar_tildes(c)
    c = F.regexp_replace(c, u"\u00A0", " ")
    c = F.regexp_replace(c, u"\u202F", " ")
    c = F.regexp_replace(c, r"\s+", " ")
    c = F.trim(c)
    c = F.regexp_replace(c, r"[^A-Z0-9 ]", " ")
    c = F.regexp_replace(c, r"\s+", " ")
    c = F.trim(c)
    return c

def norm_col(df, cols):
    for c in cols:
        df = df.withColumn(c, norm_txt(F.col(c)))
    return df


def load_powerapps(spark, path_apps):
    df = (
        spark.read.format("csv")
        .option("header", "true")
        .option("sep", ";")
        .option("encoding", "utf-8")
        .option("ignoreLeadingWhiteSpace", "true")
        .option("ignoreTrailingWhiteSpace", "true")
        .load(path_apps)
        .select(
            F.col("Title").alias("CODSOLICITUD"),
            F.col("FechaAsignacion").alias("FECASIGNACION"),
            F.col("Tipo de Producto").alias("PRODUCTO"),
            F.col("ResultadoAnalista").alias("RESULTADOANALISTA"),
            F.col("Mail").alias("CORREO"),
            F.col("Motivo Resultado Analista").alias("MOTIVORESULTADOANALISTA"),
            F.col("AñoMes").alias("CODMES"),
            F.col("Created").alias("CREATED"),
            F.col("Motivo_MD").alias("MOTIVOMALADERIVACION"),
            F.col("Submotivo_MD").alias("SUBMOTIVOMALADERIVACION"),
        )
    )

    df = df.withColumn("CODSOLICITUD", F.trim(F.col("CODSOLICITUD").cast("string")))
    df = norm_col(df, [
        "PRODUCTO", "RESULTADOANALISTA", "MOTIVORESULTADOANALISTA",
        "MOTIVOMALADERIVACION", "SUBMOTIVOMALADERIVACION"
    ])

    return df



df_apps = load_powerapps(spark, PATH_PA_SOLICITUDES)


Voy por buen camino? ayudame a completarlo y terminarlo
