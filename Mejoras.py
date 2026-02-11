import pandas as pd
import numpy as np
import re
from unidecode import unidecode
from datetime import time

pd.set_option("display.max_columns", None)
pd.set_option("display.max_rows", None)
pd.set_option("display.width", None)

# =========================================================
# 1) POWERAPP -> df_diario (igual que antes)
# =========================================================
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
).drop_duplicates(subset=["NOMBRE"])

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
      .dt.floor("min")  # igual que tu notebook original
)
df_tp["FECHA"] = created_lima.dt.date
df_tp["HORA"]  = created_lima.dt.time
df_tp["FECHAHORA"] = created_lima.dt.tz_localize(None)

df_diario = df_tp[[
    "OPORTUNIDAD", "TIPOPRODUCTO", "RESULTADOANALISTA",
    "ANALISTA", "FECHA", "HORA", "EXPERTISE", "EQUIPO", "FECHAHORA"
]].drop_duplicates()


# =========================================================
# 2) Parser de fechas (mejorado: soporta segundos)
# =========================================================
def parse_fecha_hora_esp(series):
    """
    Convierte strings como:
    - '01/10/2025 08:42 a. m.'
    - '01/10/2025 08:42:15 a. m.'
    a datetime (24h), respetando día/mes.
    """
    def clean_and_parse(value):
        if pd.isna(value):
            return pd.NaT
        s = str(value).strip().lower()
        s = re.sub(r"\s+", " ", s)
        s = (s.replace("a. m.", "AM")
               .replace("p. m.", "PM")
               .replace("a. m", "AM")
               .replace("p. m", "PM")
               .replace("a m", "AM")
               .replace("p m", "PM")
               .strip())

        dt = pd.to_datetime(s, format="%d/%m/%Y %I:%M:%S %p", errors="coerce", dayfirst=True)
        if pd.isna(dt):
            dt = pd.to_datetime(s, format="%d/%m/%Y %I:%M %p", errors="coerce", dayfirst=True)
        if pd.isna(dt):
            dt = pd.to_datetime(s, errors="coerce", dayfirst=True)
        return dt

    return series.apply(clean_and_parse)


# =========================================================
# 3) Matching analista (igual que antes)
#    Usaremos el texto de INFORME_PRODUCTO["Analista de crédito"]
# =========================================================
def normalize_for_matching(s):
    if pd.isna(s):
        return ""
    s = unidecode(str(s)).upper()
    s = s.replace("\r", ";").replace("\n", ";").replace(",", ";")
    s = re.sub(r"\s+", " ", s).strip()
    s = re.sub(r"[;]+", ";", s)
    s = re.sub(r"\s*;\s*", ";", s).strip(";")
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
    found = [name_to_analyst[k] for k in sorted_name_keys if (";" + k + ";") in txt_sep or k in txt]
    unique_found = []
    for v in found:
        if v not in unique_found:
            unique_found.append(v)
    return "; ".join(unique_found) if unique_found else ""


# =========================================================
# 4) INFORME_ESTADO (reemplaza REPORT_TC) -> df_tcstock_vigente
#    Aquí se define el estado vigente y la FECHA oficial (inicio del paso)
# =========================================================
df_tcstock = pd.read_csv("INPUT/INFORME_ESTADO.csv", encoding="latin1")

df_tcstock = df_tcstock[[
    "Nombre del registro",
    "Estado",
    "Estado del paso",
    "Fecha de inicio del paso",
    "Fecha de finalización del paso",
    "Paso: Nombre",
    "Proceso de aprobación: Nombre",
    "Último actor: Nombre completo",
    "Último actor del paso: Nombre completo",
]].rename(columns={
    "Nombre del registro": "OPORTUNIDAD",
    "Estado": "ESTADO",
    "Estado del paso": "ESTADO_PASO",
    "Fecha de inicio del paso": "FECINICIOPASO",
    "Fecha de finalización del paso": "FECFINPASO",
    "Paso: Nombre": "PASO",
    "Proceso de aprobación: Nombre": "PROCESO",
    "Último actor: Nombre completo": "ULTIMO_ACTOR",
    "Último actor del paso: Nombre completo": "ULTIMO_ACTOR_PASO",
})

df_tcstock["FECINICIOPASO"] = parse_fecha_hora_esp(df_tcstock["FECINICIOPASO"])
df_tcstock["FECFINPASO"] = parse_fecha_hora_esp(df_tcstock["FECFINPASO"])

# normalización de estado para comparaciones
df_tcstock["ESTADO"] = df_tcstock["ESTADO"].astype(str).str.strip()
df_tcstock["ESTADO_PASO"] = df_tcstock["ESTADO_PASO"].astype(str).str.strip()

# "Última foto" por oportunidad (vigente)
df_tcstock_vigente = (
    df_tcstock.sort_values(["OPORTUNIDAD", "FECINICIOPASO", "FECFINPASO"], ascending=[True, False, False])
             .groupby("OPORTUNIDAD", as_index=False)
             .head(1)
)

df_tcstock_vigente["ESTADO_N"] = df_tcstock_vigente["ESTADO"].astype(str).str.upper().str.strip()
df_tcstock_vigente["ESTADO_PASO_N"] = df_tcstock_vigente["ESTADO_PASO"].astype(str).str.upper().str.strip()

# Pendiente si cualquiera indica pendiente
df_tcstock_vigente["ES_PENDIENTE"] = (
    df_tcstock_vigente["ESTADO_N"].eq("PENDIENTE") |
    df_tcstock_vigente["ESTADO_PASO_N"].eq("PENDIENTE")
)

# Dejamos ESTADO como la versión normalizada (para que filtre igual que antes)
df_tcstock_vigente["ESTADO"] = df_tcstock_vigente["ESTADO_N"]


# =========================================================
# 5) INFORME_PRODUCTO (reemplaza REPORT_CEF_TC + REPORT_CEF)
#    Aquí tomamos:
#      - Analista de crédito (texto) -> lo mapeamos a ANALISTA (código)
#      - Tipo de Acción (para TC)
#      - Nombre del Producto (para CEF)
# =========================================================
df_prod = pd.read_csv("INPUT/INFORME_PRODUCTO.csv", encoding="latin1", low_memory=False)

df_prod = df_prod.rename(columns={
    "Nombre de la oportunidad": "OPORTUNIDAD",
    "Nombre del Producto": "DESPRODUCTO",
    "Tipo de Acción": "DESTIPACCION",
    "Analista de crédito": "ANALISTACREDITO",
    "Fecha de creación": "FECCREACION",
    "Fecha de inicio de evaluación": "FECINICIOEVALUACION",  # la leemos por si quieres mirar, pero no la usamos como FECHAHORA
})

# normaliza textos
df_prod["DESPRODUCTO"] = df_prod["DESPRODUCTO"].astype(str).str.strip().str.upper()
df_prod["DESTIPACCION"] = df_prod["DESTIPACCION"].astype(str).str.strip().str.upper()
df_prod["ANALISTACREDITO"] = df_prod["ANALISTACREDITO"].astype(str).str.strip()

# dedup 1 fila por oportunidad (más reciente por FECCREACION)
# (si FECCREACION viene vacío, quedará NaT y el sort no ayuda, pero no rompe)
df_prod["FECCREACION"] = parse_fecha_hora_esp(df_prod["FECCREACION"]) if "FECCREACION" in df_prod.columns else pd.NaT

df_prod_1 = (
    df_prod.sort_values(["OPORTUNIDAD", "FECCREACION"], ascending=[True, False])
          .groupby("OPORTUNIDAD", as_index=False)
          .head(1)
).copy()

# mapeo a código (igual que antes)
df_prod_1["ANALISTA_MATCH"] = df_prod_1["ANALISTACREDITO"].apply(find_analysts_in_cell)


# =========================================================
# 6) Base equivalente a df_pendientes_tcstock (join estado + producto)
# =========================================================
df_pendientes_tcstock = df_tcstock_vigente.merge(df_prod_1, on="OPORTUNIDAD", how="left")[
    ["OPORTUNIDAD", "ESTADO", "DESPRODUCTO", "DESTIPACCION", "ANALISTACREDITO", "ANALISTA_MATCH", "FECINICIOPASO"]
].drop_duplicates()


# =========================================================
# 7) Pendientes TCSTOCK final (como antes, con equipo/expertise)
# =========================================================
df_pendientes_tcstock_base = df_pendientes_tcstock.loc[
    df_pendientes_tcstock["ESTADO"] == "PENDIENTE",
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

df_pendientes_tcstock_final = df_pendientes_tcstock_base[[
    "OPORTUNIDAD", "DESTIPACCION", "ESTADO", "FECINICIOPASO", "FECHA", "HORA",
    "ANALISTA_MATCH", "EXPERTISE", "EQUIPO"
]].copy()

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


# =========================================================
# 8) Pendientes CEF final (equivalente a tu df_pendientes_cef_final)
#    Ahora se construye desde INFORME_PRODUCTO + el ESTADO vigente del maestro
#    FECHAHORA = FECINICIOPASO (regla nueva)
# =========================================================
prod_cef = {
    "CRÉDITOS PERSONALES MICROCREDITOS",
    "CREDITOS PERSONALES MICROCREDITOS",
    "CRÉDITOS PERSONALES EFECTIVO MP",
    "CREDITOS PERSONALES EFECTIVO MP",
    "CONVENIO DESCUENTOS POR PLANILLA"
}

df_pendientes_cef_base = df_pendientes_tcstock.loc[
    (df_pendientes_tcstock["ESTADO"] == "PENDIENTE") &
    (df_pendientes_tcstock["DESPRODUCTO"].astype(str).str.upper().isin(prod_cef)),
    ["OPORTUNIDAD", "DESPRODUCTO", "ESTADO", "ANALISTA_MATCH", "FECINICIOPASO"]
].copy()

df_pendientes_cef_base["FECHA"] = df_pendientes_cef_base["FECINICIOPASO"].dt.date
df_pendientes_cef_base["HORA"] = df_pendientes_cef_base["FECINICIOPASO"].dt.time

df_pendientes_cef_base = (
    df_pendientes_cef_base
      .merge(df_equipos[["ANALISTA", "EQUIPO"]],
             left_on="ANALISTA_MATCH", right_on="ANALISTA", how="left")
      .drop(columns=["ANALISTA"])
      .merge(df_clasificacion[["NOMBRE", "EXPERTISE"]],
             left_on="ANALISTA_MATCH", right_on="NOMBRE", how="left")
      .drop(columns=["NOMBRE"])
)

df_pendientes_cef_final = df_pendientes_cef_base.rename(columns={
    "DESPRODUCTO": "TIPOPRODUCTO",
    "ESTADO": "RESULTADOANALISTA",
    "FECINICIOPASO": "FECHAHORA",
    "ANALISTA_MATCH": "ANALISTA"
})[[
    "OPORTUNIDAD", "TIPOPRODUCTO", "RESULTADOANALISTA",
    "FECHAHORA", "FECHA", "HORA", "ANALISTA", "EXPERTISE", "EQUIPO"
]].copy()

df_pendientes_cef_final["FLGPENDIENTE"] = 1
df_pendientes_cef_final = df_pendientes_cef_final.drop_duplicates()


# =========================================================
# 9) Validación por trabajo_dias (igual que antes)
# =========================================================
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


# =========================================================
# 10) Output 1: REPORTE_FINAL_VALIDADO.csv (igual que antes)
# =========================================================
df_diario["FLGPENDIENTE"] = 0

df_final_validado = pd.concat(
    [df_diario, df_pendientes_tcstock_final, df_pendientes_cef_final],
    ignore_index=True
)

df_final_validado["FLGPENDIENTE"] = df_final_validado["FLGPENDIENTE"].map({0: "NO", 1: "SI"})

df_final_validado.to_csv(
    "OUTPUT/REPORTE_FINAL_VALIDADO.csv", index=False, encoding="utf-8-sig"
)


# =========================================================
# 11) Output 2: última foto por oportunidad + clasificar_estado (igual que antes)
# =========================================================
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

    return "EN PROCESO"

df_final_validado_logica["ESTADO_OPORTUNIDAD"] = df_final_validado_logica.apply(clasificar_estado, axis=1)

df_final_validado_logica.to_csv(
    "OUTPUT/REPORTE_FINAL_VALIDADO_LOGICA.csv", index=False, encoding="utf-8-sig"
)