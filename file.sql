df_diario = df_tp[[
    "OPORTUNIDAD", "TIPOPRODUCTO", "RESULTADOANALISTA",
    "ANALISTA", "FECHA", "HORA", "EXPERTISE", "EQUIPO", "FECHAHORA"
]]

df_diario = df_diario.drop_duplicates()






import pandas as pd
import re

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
        # Limpieza de puntos y espacios en a. m. / p. m.
        s = (s.replace('a. m.', 'AM')
               .replace('p. m.', 'PM')
               .replace('a. m', 'AM')
               .replace('p. m', 'PM')
               .replace('a m', 'AM')
               .replace('p m', 'PM')
               .strip())
        try:
            # 🔹 formato explícito día/mes/año con 12h
            return pd.to_datetime(s, format="%d/%m/%Y %I:%M %p", errors="coerce", dayfirst=True)
        except Exception:
            return pd.to_datetime(s, errors="coerce", dayfirst=True)
    return series.apply(clean_and_parse)



import pandas as pd
import re
from unidecode import unidecode

df_tcstock = pd.read_csv("INPUT/REPORT_TC.csv", encoding="latin1")
df_cef_tc = pd.read_csv("INPUT/REPORT_CEF_TC.csv", encoding="latin1")

df_tcstock = df_tcstock[["Nombre del registro", "Estado", "Fecha de inicio del paso"]].rename(columns={
    "Nombre del registro": "OPORTUNIDAD",
    "Estado": "ESTADO",
    "Fecha de inicio del paso": "FECINICIOPASO"
})

df_cef_tc = df_cef_tc[[
    "Nombre de la oportunidad",
    "Nombre del Producto",
    "Tipo de Acción",
    "Analista de crédito",
    "Estado de aprobación"
]].rename(columns={
    "Nombre de la oportunidad": "OPORTUNIDAD",
    "Nombre del Producto": "DESPRODUCTO",
    "Tipo de Acción": "DESTIPACCION",
    "Analista de crédito": "ANALISTACREDITO",
    "Estado de aprobación": "ESTADOAPROBACION"
})

df_pendientes_tcstock = df_tcstock.merge(df_cef_tc, on="OPORTUNIDAD", how="left")[
    ["OPORTUNIDAD", "ESTADO", "DESPRODUCTO", "DESTIPACCION", "ANALISTACREDITO", "FECINICIOPASO"]
]

def normalize_for_matching(s):
    if pd.isna(s):
        return ""
    s = unidecode(str(s)).upper()
    s = s.replace('\r', ';').replace('\n', ';').replace(',', ';')
    s = re.sub(r'\s+', ' ', s).strip()
    s = re.sub(r'[;]+', ';', s)
    s = re.sub(r'\s*;\s*', ';', s).strip(';')
    return s

name_to_analyst = {}
for _, row in df_equipos.iterrows():
    raw = row.get("NOMBRECOMPLETO", "")
    analyst = row.get("ANALISTA", "")
    norm = normalize_for_matching(raw)
    if not norm:
        continue
    for part in [p.strip() for p in norm.split(";") if p.strip()]:
        name_to_analyst[part] = analyst

sorted_name_keys = sorted(name_to_analyst.keys(), key=len, reverse=True)

def find_analysts_in_cell(text):
    txt = normalize_for_matching(text)
    if not txt:
        return ""
    txt_sep = ";" + txt + ";"
    found = [name_to_analyst[k] for k in sorted_name_keys if (';' + k + ';') in txt_sep or k in txt]
    unique_found = []
    for v in found:
        if v not in unique_found:
            unique_found.append(v)
    return "; ".join(unique_found) if unique_found else ""

df_pendientes_tcstock["ANALISTA_MATCH"] = df_pendientes_tcstock["ANALISTACREDITO"].apply(find_analysts_in_cell)
df_pendientes_tcstock["FECINICIOPASO"] = parse_fecha_hora_esp(df_pendientes_tcstock["FECINICIOPASO"])

df_pendientes_tcstock = df_pendientes_tcstock.drop_duplicates()
