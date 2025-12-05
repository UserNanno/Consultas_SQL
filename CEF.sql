def parse_fecha_hora_esp(series):
    """
    Convierte strings como '01/10/2025 08:42 a. m.' o '01/10/2025 3:25 p. m.'
    en datetime de 24 horas, respetando el día/mes correcto (formato español).
    """
    def clean_and_parse(value):
        if pd.isna(value):
            return pd.NaT
        s = str(value).strip().lower()
        s = re.sub(r'\s+', ' ', s)
        # Limpieza
        s = (s.replace('a. m.', 'AM')
               .replace('p. m.', 'PM')
               .replace('a. m', 'AM')
               .replace('p. m', 'PM')
               .replace('a m', 'AM')
               .replace('p m', 'PM')
               .strip())
        try:
            # formato explícito día/mes/año con 12h
            return pd.to_datetime(s, format="%d/%m/%Y %I:%M %p", errors="coerce", dayfirst=True)
        except Exception:
            return pd.to_datetime(s, errors="coerce", dayfirst=True)
    return series.apply(clean_and_parse)


import pandas as pd
from datetime import datetime
from unidecode import unidecode
import re
import glob

files = glob.glob("INPUT/SALESFORCE/INFORME_ESTADO_*.csv")

df_salesforce = pd.concat([
    pd.read_csv(
        f,
        encoding="latin1",
        usecols=[
            "Nombre del registro", "Estado", "Fecha de inicio del paso",
            "Fecha de finalización del paso", "Paso: Nombre",
            "Último actor: Nombre completo", "Proceso de aprobación: Nombre"
        ]
    ) for f in files
], ignore_index=True)

df_salesforce = df_salesforce.rename(columns={
    "Nombre del registro": "CODSOLICITUD",
    "Estado": "ESTADOSOLICITUD",
    "Fecha de inicio del paso": "FECINICIOEVALUACION",
    "Fecha de finalización del paso": "FECFINEVALUACION",
    "Paso: Nombre": "NBRPASO",
    "Último actor: Nombre completo": "NBRANALISTA",
    "Proceso de aprobación: Nombre": "PROCESO"
})




df_salesforce.loc[:, "ESTADOSOLICITUD"] = df_salesforce["ESTADOSOLICITUD"].str.upper()
df_salesforce.loc[:, "NBRPASO"] = df_salesforce["NBRPASO"].str.upper()
df_salesforce.loc[:, "NBRANALISTA"] = df_salesforce["NBRANALISTA"].str.upper()
df_salesforce.loc[:, "PROCESO"] = df_salesforce["PROCESO"].str.upper()



df_salesforce["NBRPASO"] = df_salesforce["NBRPASO"].apply(lambda x: unidecode(str(x)))
df_salesforce["NBRANALISTA"] = df_salesforce["NBRANALISTA"].apply(lambda x: unidecode(str(x)))
df_salesforce["PROCESO"] = df_salesforce["PROCESO"].apply(lambda x: unidecode(str(x)))



df_salesforce["FECHORINICIOEVALUACION"] = parse_fecha_hora_esp(df_salesforce["FECINICIOEVALUACION"])
df_salesforce["FECHORFINEVALUACION"] = parse_fecha_hora_esp(df_salesforce["FECFINEVALUACION"])

df_salesforce["FECINICIOEVALUACION"] = df_salesforce["FECHORINICIOEVALUACION"].dt.date
df_salesforce["FECFINEVALUACION"] = df_salesforce["FECHORFINEVALUACION"].dt.date

df_salesforce["HORINICIOEVALUACION"] = df_salesforce["FECHORINICIOEVALUACION"].dt.time
df_salesforce["HORFINEVALUACION"] = df_salesforce["FECHORFINEVALUACION"].dt.time


df_salesforce["CODMESEVALUACION"] = df_salesforce["FECINICIOEVALUACION"].apply(
    lambda x: f"{x.year}{x.month:02}" if pd.notna(x) else None
)


df_salesforce = df_salesforce.sort_values(
    by=["CODSOLICITUD", "FECHORINICIOEVALUACION"],
    ascending=[True, True]  # orden ascendente para que el último sea el más reciente
)


df_salesforce = df_salesforce.drop_duplicates(
    subset=["CODSOLICITUD"], 
    keep="last"
)


df_salesforce['CODMESEVALUACION'] = df_salesforce['CODMESEVALUACION'].astype(int)
df_organico['CODMES'] = df_organico['CODMES'].astype(int)


# Diccionario con los cambios deseados
analistas_map = {
    'LESLY ASPIROS MEDINA': 'ASPIROS MEDINA LESLY FIORELA',
    'GUIDO FERNANDEZ PORROA': 'FERNANDEZ PORROA GUIDO DAVID',
    'PIERO VELARDE HUBY': 'VELARDE HUBY PIERO ANGELO',
    'MARVIN VIRHUEZ BLANCAS': 'VIRHUEZ BLANCAS MARVIN ANTONIO',
    'KARINA MONTALVA DE FALLA': 'MONTALVA DE FALLA KARINA PAOLA',
    'RICARDO RIVERA DELGADO': 'RIVERA DELGADO RICARDO ANTONIO',
    'PILAR DEL AGUILA VALDEZ': 'DEL AGUILA VALDEZ PILAR MARLENE',
    'SOLANGE LOPEZ ALIAGA': 'LOPEZ ALIAGA SOLANGE XIMENA'
}

# Aplicar el reemplazo en la columna
df_salesforce['NBRANALISTA'] = df_salesforce['NBRANALISTA'].replace(analistas_map)



import re

# Limpieza de nombres
df_salesforce['NBRANALISTA_CLEAN'] = df_salesforce['NBRANALISTA'].str.upper().apply(
    lambda x: re.sub(r'\(CESADO\)', '', str(x)).strip()
)
df_organico['NOMBRECOMPLETO_CLEAN'] = df_organico['NOMBRECOMPLETO'].str.upper().apply(
    lambda x: re.sub(r'\(CESADO\)', '', str(x)).strip()
)
