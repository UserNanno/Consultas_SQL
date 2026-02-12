import pandas as pd

pd.set_option("display.max_columns", None)
pd.set_option("display.max_rows", None)
pd.set_option("display.width", None)

df = pd.read_csv(
    "INPUT/POWERAPP.csv",
    encoding="utf-8-sig",
    usecols=["Title", "Tipo de Producto", "ResultadoAnalista", "Analista", "Created"]
).rename(columns={
    "Title": "OPORTUNIDAD",
    "Tipo de Producto": "TIPOPRODUCTO",
    "ResultadoAnalista": "RESULTADOANALISTA",
    "Analista": "ANALISTA",
    "Created": "CREATED",
})
df["ANALISTA"] = df["ANALISTA"].astype(str).str.strip().str.upper()
df["TIPOPRODUCTO"] = df["TIPOPRODUCTO"].astype(str).str.strip().str.upper()
df["RESULTADOANALISTA"] = df["RESULTADOANALISTA"].astype(str).str.strip().str.upper()

df_clasificacion = pd.read_csv(
    "INPUT/CLASIFICACION_ANALISTAS.csv",
    encoding="utf-8-sig",
    usecols=["NOMBRE", "EXPERTISE"]
)
df_clasificacion = df_clasificacion.drop_duplicates(subset=["NOMBRE"])

df_equipos = pd.read_csv(
    "INPUT/EQUIPOS.csv",
    delimiter=";",
    encoding="utf-8-sig",
    usecols=["Analista nombre completo", "ANALISTA", "equipo"]
).rename(columns={"Analista nombre completo": "NOMBRECOMPLETO", "equipo": "EQUIPO"})
df_equipos["ANALISTA"] = df_equipos["ANALISTA"].astype(str).str.strip().str.upper()
df_equipos["EQUIPO"] = df_equipos["EQUIPO"].astype(str).str.strip().str.upper()
df_equipos = df_equipos.drop_duplicates(subset=["ANALISTA"])

df_tp = (
    df.merge(df_clasificacion, left_on="ANALISTA", right_on="NOMBRE", how="left")
      .merge(df_equipos, on="ANALISTA", how="left")
)

created_lima = (
    pd.to_datetime(df_tp["CREATED"], utc=True, errors="coerce")
      .dt.tz_convert("America/Lima")
      .dt.floor("min")
)
df_tp["FECHA"] = created_lima.dt.date
df_tp["HORA"]  = created_lima.dt.time
df_tp["FECHAHORA"] = created_lima.dt.tz_localize(None)





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
import re
from unidecode import unidecode

df_tcstock = pd.read_csv("INPUT/REPORT_TC.csv", encoding="latin1")
df_cef_tc = pd.read_csv("INPUT/REPORT_CEF_TC.csv", encoding="latin1", low_memory=False)

df_tcstock = df_tcstock[["Nombre del registro", "Estado", "Fecha de inicio del paso"]].rename(columns={
    "Nombre del registro": "OPORTUNIDAD",
    "Estado": "ESTADO",
    "Fecha de inicio del paso": "FECINICIOPASO"
})

df_cef_tc = df_cef_tc[[
    "Nombre de la oportunidad",
    "Nombre del Producto",
    "Tipo de Acción",
    "Analista de crédito"
]].rename(columns={
    "Nombre de la oportunidad": "OPORTUNIDAD",
    "Nombre del Producto": "DESPRODUCTO",
    "Tipo de Acción": "DESTIPACCION",
    "Analista de crédito": "ANALISTACREDITO"
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





import pandas as pd
import numpy as np

df_cef = pd.read_csv(
    "INPUT/REPORT_CEF.csv",
    encoding="latin1",
    usecols=[
        "Nombre de la oportunidad",
        "Nombre del Producto: Nombre del producto",
        "Etapa",
        "Fecha de inicio de evaluación",
        "Analista: Nombre completo",
        "Analista de crédito: Nombre completo",
    ],
)

override = df_cef["Analista de crédito: Nombre completo"].isin([
    "JOHN MARTIN RAMIREZ GALINDO",
    "KIARA ALESSANDRA GARIBAY QUISPE",
])
df_cef["ANALISTA_FINAL"] = np.where(
    override,
    df_cef["Analista: Nombre completo"],
    df_cef["Analista de crédito: Nombre completo"],
)

df_cef = df_cef.rename(columns={
    "Nombre de la oportunidad": "OPORTUNIDAD",
    "Nombre del Producto: Nombre del producto": "DESPRODUCTO",
    "Etapa": "ETAPA",
    "Fecha de inicio de evaluación": "FECINICIOEVALUACION",
})[["OPORTUNIDAD", "DESPRODUCTO", "ETAPA", "FECINICIOEVALUACION", "ANALISTA_FINAL"]]

df_cef = df_cef.merge(
    df_tcstock[["OPORTUNIDAD", "ESTADO"]],
    on="OPORTUNIDAD",
    how="left",
)

df_cef["ANALISTA"] = df_cef["ANALISTA_FINAL"].apply(find_analysts_in_cell)
df_cef["FECINICIOEVALUACION"] = parse_fecha_hora_esp(df_cef["FECINICIOEVALUACION"])

df_pendientes_cef = df_cef[[
    "OPORTUNIDAD", "DESPRODUCTO",
    "ETAPA", "ANALISTA_FINAL", "ANALISTA",
    "FECINICIOEVALUACION", "ESTADO"
]]

df_pendientes_cef = df_pendientes_cef.drop_duplicates()









df_pendientes_tcstock_base = df_pendientes_tcstock.loc[
    df_pendientes_tcstock["ESTADO"] == "Pendiente",
    ["OPORTUNIDAD", "DESTIPACCION", "ESTADO", "ANALISTA_MATCH", "FECINICIOPASO"]
].copy()

df_pendientes_tcstock_base["FECHA"] = df_pendientes_tcstock_base["FECINICIOPASO"].dt.date
df_pendientes_tcstock_base["HORA"] = df_pendientes_tcstock_base["FECINICIOPASO"].dt.time

df_pendientes_tcstock_base = (
    df_pendientes_tcstock_base
      .merge(df_equipos[["ANALISTA", "EQUIPO"]],
             left_on="ANALISTA_MATCH", right_on="ANALISTA", how="left")
      .drop(columns=["ANALISTA"])
      .merge(df_clasificacion[["NOMBRE", "EXPERTISE"]],
             left_on="ANALISTA_MATCH", right_on="NOMBRE", how="left")
      .drop(columns=["NOMBRE"])
)

df_pendientes_tcstock_final = df_pendientes_tcstock_base[
    ["OPORTUNIDAD", "DESTIPACCION", "ESTADO", "FECINICIOPASO", "FECHA", "HORA", "ANALISTA_MATCH", "EXPERTISE", "EQUIPO"]
].copy()

df_pendientes_tcstock_final.rename(columns={
    "DESTIPACCION": "TIPOPRODUCTO",
    "ESTADO": "RESULTADOANALISTA",
    "ANALISTA_MATCH": "ANALISTA",
    "FECINICIOPASO": "FECHAHORA"
}, inplace=True)

df_pendientes_tcstock_final = df_pendientes_tcstock_final[
    df_pendientes_tcstock_final["TIPOPRODUCTO"].isin(
        ["TC", "UPGRADE", "AMPLIACION", "ADICIONAL", "BT", "VENTA COMBO TC"]
    )
].copy()

df_pendientes_tcstock_final["FLGPENDIENTE"] = 1

df_pendientes_tcstock_final = df_pendientes_tcstock_final.drop_duplicates()










df_pendientes_cef_base = df_pendientes_cef.loc[
    df_pendientes_cef["ESTADO"] == "Pendiente",
    ["OPORTUNIDAD", "DESPRODUCTO", "ESTADO", "ANALISTA", "FECINICIOEVALUACION"]
].copy()

df_pendientes_cef_base["FECHA"] = df_pendientes_cef_base["FECINICIOEVALUACION"].dt.date
df_pendientes_cef_base["HORA"] = df_pendientes_cef_base["FECINICIOEVALUACION"].dt.time
df_pendientes_cef_base["DESPRODUCTO"] = df_pendientes_cef_base["DESPRODUCTO"].astype(str).str.upper()

prod_cef = {
    "CRÉDITOS PERSONALES MICROCREDITOS",
    "CREDITOS PERSONALES MICROCREDITOS",
    "CRÉDITOS PERSONALES EFECTIVO MP",
    "CREDITOS PERSONALES EFECTIVO MP",
    "CONVENIO DESCUENTOS POR PLANILLA"
}
df_pendientes_cef_base = df_pendientes_cef_base[df_pendientes_cef_base["DESPRODUCTO"].isin(prod_cef)]

df_pendientes_cef_base = df_pendientes_cef_base.merge(
    df_equipos[["ANALISTA", "EQUIPO"]],
    on="ANALISTA", how="left"
).merge(
    df_clasificacion[["NOMBRE", "EXPERTISE"]],
    left_on="ANALISTA", right_on="NOMBRE", how="left"
)

df_pendientes_cef_final = df_pendientes_cef_base.rename(columns={
    "DESPRODUCTO": "TIPOPRODUCTO",
    "ESTADO": "RESULTADOANALISTA",
    "FECINICIOEVALUACION": "FECHAHORA"
})[[
    "OPORTUNIDAD", "TIPOPRODUCTO", "RESULTADOANALISTA", "FECHAHORA", "FECHA", "HORA", "ANALISTA", "EXPERTISE", "EQUIPO"
]].copy()

df_pendientes_cef_final["FLGPENDIENTE"] = 1

df_pendientes_cef_final = df_pendientes_cef_final.drop_duplicates()









trabajo_dias = (
    df_diario[["ANALISTA", "FECHA"]]
    .drop_duplicates()
    .assign(TRABAJO=1)
)
df_pendientes_tcstock_final = (
    df_pendientes_tcstock_final
      .merge(trabajo_dias, on=["ANALISTA", "FECHA"], how="left")
      .assign(
          TRABAJO=lambda x: x["TRABAJO"].fillna(0),
          FLGPENDIENTE=lambda x: np.where(x["TRABAJO"] == 1, 1, 0)
      )
      .drop(columns=["TRABAJO"])
)

df_pendientes_cef_final = (
    df_pendientes_cef_final
      .merge(trabajo_dias, on=["ANALISTA", "FECHA"], how="left")
      .assign(
          TRABAJO=lambda x: x["TRABAJO"].fillna(0),
          FLGPENDIENTE=lambda x: np.where(x["TRABAJO"] == 1, 1, 0)
      )
      .drop(columns=["TRABAJO"])
)

""" LOP - MEJORA
for name in ["df_pendientes_tcstock_final", "df_pendientes_cef_final"]:
    df = locals()[name]
    df = (
        df.merge(trabajo_dias, on=["ANALISTA", "FECHA"], how="left")
          .assign(
              TRABAJO=lambda x: x["TRABAJO"].fillna(0),
              FLGPENDIENTE=lambda x: np.where(x["TRABAJO"] == 1, 1, 0)
          )
          .drop(columns=["TRABAJO"])
    )
    locals()[name] = df
"""











df_diario["FLGPENDIENTE"] = 0

df_final_validado = pd.concat(
    [df_diario, df_pendientes_tcstock_final, df_pendientes_cef_final],
    ignore_index=True
)

df_final_validado["FLGPENDIENTE"] = df_final_validado["FLGPENDIENTE"].map({0: "NO", 1: "SI"})





df_final_validado.to_csv(
    "OUTPUT/REPORTE_FINAL_VALIDADO.csv", index=False, encoding="utf-8-sig"
)








from datetime import time

df_final_validado_logica = df_final_validado.copy()

df_final_validado_logica["FECHAHORA"] = pd.to_datetime(df_final_validado_logica["FECHAHORA"], errors="coerce")
df_final_validado_logica = (
    df_final_validado_logica
      .sort_values(["OPORTUNIDAD", "FECHAHORA"])
      .groupby("OPORTUNIDAD", as_index=False)
      .tail(1)
)

df_final_validado_logica["RESULTADOANALISTA"] = df_final_validado_logica["RESULTADOANALISTA"].astype(str).str.upper().str.strip()
df_final_validado_logica["TIPOPRODUCTO"] = df_final_validado_logica["TIPOPRODUCTO"].astype(str).str.upper().str.strip()
df_final_validado_logica["FLGPENDIENTE"] = df_final_validado_logica["FLGPENDIENTE"].astype(str).str.upper().str.strip()

# CLASIF SIN TOLERANCIA
def clasificar_estado(row):
    res = row["RESULTADOANALISTA"]
    tipo = row["TIPOPRODUCTO"]
    flg = row["FLGPENDIENTE"]
    hora = row["FECHAHORA"].time() if pd.notnull(row["FECHAHORA"]) else None

    if res in ["APROBADO POR ANALISTA DE CREDITO", "DENEGADO POR ANALISTA DE CREDITO"]:
        return "ATENCIONES"

    if res in ["DEVUELTO"]:
        return "DEVUELTO"

    if res == "FACA":
        return "FACA"

    if res == "DEVOLVER AL GESTOR" and tipo == "CRÉDITO VEHICULAR":
        return "ATENCIONES"

    if res in ["PENDIENTE", "ENVIADO A ANALISTA DE CRÉDITOS"] or flg in ["SI", "1"]:
        if hora:
            if time(8, 0) <= hora <= time(19, 30):
                return "PENDIENTE (HORA HABIL)"
            else:
                return "PENDIENTE (HORA NO HABIL)"
        else:
            return "PENDIENTE"


    # Resto
    return "EN PROCESO"

df_final_validado_logica["ESTADO_OPORTUNIDAD"] = df_final_validado_logica.apply(clasificar_estado, axis=1)







df_final_validado_logica.to_csv(
    "OUTPUT/REPORTE_FINAL_VALIDADO_LOGICA.csv", index=False, encoding="utf-8-sig"
)
