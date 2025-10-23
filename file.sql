import pandas as pd

pd.set_option("display.max_columns", None)
pd.set_option('display.max_rows', None)
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

df_clasificacion = pd.read_csv(
    "INPUT/CLASIFICACION_ANALISTAS.csv",
    encoding="utf-8-sig",
    usecols=["NOMBRE", "EXPERTISE"]
)

df_equipos = pd.read_csv(
    "INPUT/EQUIPOS.csv",
    delimiter=";",
    encoding="utf-8-sig",
    usecols=["Analista nombre completo", "ANALISTA", "equipo"]
).rename(columns={"Analista nombre completo" : "NOMBRECOMPLETO", "equipo": "EQUIPO"})
df_equipos["ANALISTA"] = df_equipos["ANALISTA"].astype(str).str.strip().str.upper()
df_equipos["EQUIPO"] = df_equipos["EQUIPO"].astype(str).str.strip().str.upper()

df_tp = df.merge(df_clasificacion, left_on="ANALISTA", right_on="NOMBRE", how="left") \
          .merge(df_equipos, on="ANALISTA", how="left")

created = pd.to_datetime(df_tp["CREATED"], utc=True, errors="coerce").dt.tz_convert("America/Lima")
df_tp["FECHA"] = created.dt.date
df_tp["HORA"] = created.dt.time
df_tp["FECHAHORA"] = created.dt.floor("min")

df_diario = df_tp[[
    "OPORTUNIDAD", "TIPOPRODUCTO", "RESULTADOANALISTA",
    "ANALISTA", "FECHA", "HORA", "EXPERTISE", "EQUIPO", "FECHAHORA"
]]

df_diario = df_diario.drop_duplicates()





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
    "JOHN MARTIN MARTIN RAMIREZ GALINDO",
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
    df_cef_tc[["OPORTUNIDAD", "ESTADOAPROBACION"]],
    on="OPORTUNIDAD",
    how="left",
)

df_cef["ANALISTA"] = df_cef["ANALISTA_FINAL"].apply(find_analysts_in_cell)

prod_set = {
    "CREDITOS PERSONALES EFECTIVO MP",
    "CRÉDITOS PERSONALES MICROCREDITOS",
    "CONVENIO DESCUENTOS POR PLANILLA",
}
aprob_set = {
    "Aprobado por Analista de créditos",
    "Aprobado por Gerente-Firmas y Desembolso",
}
rech_set = {
    "Rechazado por Gerente-Documentación Adicional",
    "Rechazado por Gerente-Firmas y Desembolso",
}
enviado_set = {
    "Enviado a Gerente-Documentación Adicional",
    "Enviado a Analista de créditos",
    "Enviado a Gerente-Firmas y Desembolso",
}

aceptado = (
    (df_cef["DESPRODUCTO"].isin(prod_set) & df_cef["ESTADOAPROBACION"].isin(aprob_set))
    | (df_cef["ESTADOAPROBACION"] == "Aprobado por Analista de créditos")
    | (df_cef["ETAPA"] == "Desembolsado/Activado")
)
denegado = (
    (df_cef["DESPRODUCTO"].isin(prod_set) & df_cef["ESTADOAPROBACION"].isin(rech_set))
    | (df_cef["ESTADOAPROBACION"].isin(enviado_set) & (df_cef["ETAPA"] == "Desestimada"))
    | (df_cef["ETAPA"].isin(["Desestimada", "Denegada"]))
)
pendiente = df_cef["ETAPA"] == "Evaluación Centralizada"

df_cef["ESTADO"] = np.select([aceptado, denegado, pendiente], ["Aceptado", "Denegado", "Pendiente"], default="")

df_pendientes_cef = df_cef[[
    "OPORTUNIDAD", "DESPRODUCTO", "ESTADOAPROBACION",
    "ETAPA", "ANALISTA_FINAL", "ANALISTA",
    "FECINICIOEVALUACION", "ESTADO"
]]

df_pendientes_cef = df_pendientes_cef.drop_duplicates()










# 1) Base filtrada
df_pendientes_tcstock_base = df_pendientes_tcstock.loc[
    df_pendientes_tcstock["ESTADO"] == "Pendiente",
    ["OPORTUNIDAD", "DESTIPACCION", "ESTADO", "ANALISTA_MATCH", "FECINICIOPASO"]
].copy()

# 2) Partes de fecha/hora
df_pendientes_tcstock_base["FECHA"] = df_pendientes_tcstock_base["FECINICIOPASO"].astype(str).str[:10]
df_pendientes_tcstock_base["HORA"] = df_pendientes_tcstock_base["FECINICIOPASO"].astype(str).str[11:]
df_pendientes_tcstock_base["FECHAHORA"] = df_pendientes_tcstock_base["FECINICIOPASO"].astype(str).str[:16]

# 3) Enriquecimientos: trae EQUIPO y EXPERTISE sin duplicar columnas clave
df_pendientes_tcstock_base = (
    df_pendientes_tcstock_base
      .merge(df_equipos[["ANALISTA", "EQUIPO"]],
             left_on="ANALISTA_MATCH", right_on="ANALISTA", how="left")
      .drop(columns=["ANALISTA"])  # evita duplicar ANALISTA (luego renombramos ANALISTA_MATCH)
      .merge(df_clasificacion[["NOMBRE", "EXPERTISE"]],
             left_on="ANALISTA_MATCH", right_on="NOMBRE", how="left")
      .drop(columns=["NOMBRE"])    # limpieza
)

# 4) Seleccionar columnas y luego renombrar (más claro y seguro)
df_pendientes_tcstock_final = df_pendientes_tcstock_base[
    ["OPORTUNIDAD", "DESTIPACCION", "ESTADO",
     "ANALISTA_MATCH", "FECHA", "HORA", "EXPERTISE", "FECHAHORA", "EQUIPO"]
].copy()

df_pendientes_tcstock_final.rename(columns={
    "DESTIPACCION": "TIPOPRODUCTO",
    "ESTADO": "RESULTADOANALISTA",
    "ANALISTA_MATCH": "ANALISTA"
}, inplace=True)

# 5) Filtro de producto
df_pendientes_tcstock_final = df_pendientes_tcstock_final[
    df_pendientes_tcstock_final["TIPOPRODUCTO"].isin(
        ["TC", "UPGRADE", "AMPLIACION", "ADICIONAL", "BT", "VENTA COMBO TC"]
    )
].copy()

# 6) Flag y limpieza final
df_pendientes_tcstock_final["FLGPENDIENTE"] = 1
df_pendientes_tcstock_final = df_pendientes_tcstock_final.drop_duplicates()











df_pendientes_cef_base = df_pendientes_cef.loc[
    df_pendientes_cef["ESTADO"] == "Pendiente",
    ["OPORTUNIDAD", "DESPRODUCTO", "ESTADOAPROBACION", "ANALISTA", "FECINICIOEVALUACION"]
].copy()

df_pendientes_cef_base["FECHA"] = df_pendientes_cef_base["FECINICIOEVALUACION"].astype(str).str[:10]
df_pendientes_cef_base["HORA"] = df_pendientes_cef_base["FECINICIOEVALUACION"].astype(str).str[11:]
df_pendientes_cef_base["FECHAHORA"] = df_pendientes_cef_base["FECINICIOEVALUACION"].astype(str).str[:16]
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
    "ESTADOAPROBACION": "RESULTADOANALISTA"
})[[
    "OPORTUNIDAD", "TIPOPRODUCTO", "RESULTADOANALISTA",
    "ANALISTA", "FECHA", "HORA", "EXPERTISE", "FECHAHORA", "EQUIPO"
]].copy()

df_pendientes_cef_final["FLGPENDIENTE"] = 1

df_pendientes_cef_final = df_pendientes_cef_final.drop_duplicates()










df_diario["ANALISTA"] = df_diario["ANALISTA"].astype(str).str.upper().str.strip()

df_pendientes_tcstock_final["FECHA"] = pd.to_datetime(
    df_pendientes_tcstock_final["FECHA"], dayfirst=True, errors="coerce"
).dt.date

df_pendientes_cef_final["FECHA"] = pd.to_datetime(
    df_pendientes_cef_final["FECHA"], dayfirst=True, errors="coerce"
).dt.date

df_pendientes_tcstock_sin_validar = df_pendientes_tcstock_final.copy()
df_pendientes_cef_sin_validar = df_pendientes_cef_final.copy()

trabajo_dias = (
    df_diario[["ANALISTA", "FECHA"]]
    .drop_duplicates()
    .assign(TRABAJO=1)
)

df_pendientes_tcstock_final = df_pendientes_tcstock_final.merge(trabajo_dias, on=["ANALISTA", "FECHA"], how="left")
df_pendientes_tcstock_final["TRABAJO"] = df_pendientes_tcstock_final["TRABAJO"].fillna(0)
df_pendientes_tcstock_final["FLGPENDIENTE"] = np.where(df_pendientes_tcstock_final["TRABAJO"] == 1, 1, 0)
df_pendientes_tcstock_final.drop(columns=["TRABAJO"], inplace=True)

df_pendientes_cef_final = df_pendientes_cef_final.merge(trabajo_dias, on=["ANALISTA", "FECHA"], how="left")
df_pendientes_cef_final["TRABAJO"] = df_pendientes_cef_final["TRABAJO"].fillna(0)
df_pendientes_cef_final["FLGPENDIENTE"] = np.where(df_pendientes_cef_final["TRABAJO"] == 1, 1, 0)
df_pendientes_cef_final.drop(columns=["TRABAJO"], inplace=True)





print("TCStock coincidencias:", (pd.merge(
    df_pendientes_tcstock_final[["ANALISTA","FECHA"]].drop_duplicates(),
    trabajo_dias, on=["ANALISTA","FECHA"], how="inner"
).shape[0]))

print("CEF coincidencias:", (pd.merge(
    df_pendientes_cef_final[["ANALISTA","FECHA"]].drop_duplicates(),
    trabajo_dias, on=["ANALISTA","FECHA"], how="inner"
).shape[0]))







df_diario["FLGPENDIENTE"] = 0

df_final_validado = pd.concat(
    [df_diario, df_pendientes_tcstock_final, df_pendientes_cef_final],
    ignore_index=True
)

df_final_validado["FLGPENDIENTE"] = df_final_validado["FLGPENDIENTE"].map({0: "NO", 1: "SI"})







# ==============================
# Post-proceso final antes del export (VALIDADO)
# Reglas de prioridad (de mayor a menor):
# 1) APROBADO (en RESULTADOANALISTA o ESTADOAPROBACION) → todo el grupo (OPORTUNIDAD, FECHA) = "NO"
# 2) Si no hay aprobado, pero hay FACA → último del grupo = "FACA"; resto = "NO"
# 3) Si no hay aprobado ni FACA:
#    - multi-analista → "REASIGNADO"
#    - pendiente (flag/texto) → "SI"
#    - en otro caso → "NO"
# Además: si dentro de la misma OPORTUNIDAD existe un APROBADO posterior,
#          cualquier "ENVIADO A ANALISTA" anterior de esa OPORTUNIDAD se fuerza a "NO".
# ==============================
# 1) FECHAHORA = FECHA + " " + HORA (sin UTC)
if "FECHAHORA" in df_final_validado.columns:
   df_final_validado = df_final_validado.drop(columns=["FECHAHORA"])
df_final_validado["FECHAHORA"] = (
   df_final_validado["FECHA"].astype(str).str.strip()
   + " "
   + df_final_validado["HORA"].astype(str).str.strip()
).str.strip()
# 2) Preparar señales base (sin normalizar ANALISTA)
#    Trabajamos sobre copias en mayúsculas para matching textual
res_up = df_final_validado["RESULTADOANALISTA"].astype(str).str.upper().str.strip()
est_up = df_final_validado["ESTADOAPROBACION"].astype(str).str.upper().str.strip() if "ESTADOAPROBACION" in df_final_validado.columns else pd.Series("", index=df_final_validado.index)
mask_text_pend = (
   res_up.str.contains("PENDIENTE", na=False)
   | res_up.str.contains(r"ENVIADO\s+A\s+ANALISTA", na=False)
   | res_up.str.contains(r"ENVIADO\s+A\s+GERENTE", na=False)
)
mask_flag_pend = (
   (df_final_validado["FLGPENDIENTE"] == 1)
   | df_final_validado["FLGPENDIENTE"].astype(str).str.upper().eq("SI")
)
# Señales a nivel de grupo (OPORTUNIDAD, FECHA)
mask_group_has_faca = (
   df_final_validado
   .assign(_FACA_=res_up.str.contains(r"\bFACA\b", na=False))
   .groupby(["OPORTUNIDAD", "FECHA"])["_FACA_"]
   .transform("any")
)
# APROBADO (en cualquiera de los campos, tolerando "APROBADO/APROBADA")
mask_group_has_aprob = (
   df_final_validado
   .assign(_APR_=(res_up.str.contains(r"\bAPROBAD[OA]\b", na=False) | est_up.str.contains(r"\bAPROBAD[OA]\b", na=False)))
   .groupby(["OPORTUNIDAD", "FECHA"])["_APR_"]
   .transform("any")
)
mask_group_multi_analyst = (
   df_final_validado
   .groupby(["OPORTUNIDAD", "FECHA"])["ANALISTA"]
   .transform(lambda s: s.nunique() > 1)
)
# 3) Índice del último registro por grupo (para el caso FACA)
df_final_validado["_RID_"] = np.arange(len(df_final_validado))
_ordered = df_final_validado.sort_values(["OPORTUNIDAD", "FECHA", "FECHAHORA", "_RID_"])
idx_last_per_group = _ordered.groupby(["OPORTUNIDAD", "FECHA"]).tail(1).index
mask_is_group_last = df_final_validado.index.isin(idx_last_per_group)
# 4) Asignación con prioridades
# Base
df_final_validado["FLGPENDIENTE"] = "NO"
# 4.1) Grupos SIN aprobado NI FACA → aplicar SI / REASIGNADO
no_aprob_ni_faca = (~mask_group_has_aprob) & (~mask_group_has_faca)
df_final_validado.loc[no_aprob_ni_faca & (mask_flag_pend | mask_text_pend), "FLGPENDIENTE"] = "SI"
df_final_validado.loc[no_aprob_ni_faca & mask_group_multi_analyst, "FLGPENDIENTE"] = "REASIGNADO"
# 4.2) Grupos CON FACA y SIN aprobado → último = FACA; resto = NO
solo_faca = (~mask_group_has_aprob) & (mask_group_has_faca)
df_final_validado.loc[solo_faca & mask_is_group_last, "FLGPENDIENTE"] = "FACA"
# 4.3) Grupos CON APROBADO → todo el grupo = "NO" (domina sobre todo lo anterior)
df_final_validado.loc[mask_group_has_aprob, "FLGPENDIENTE"] = "NO"
# 5) Corrección adicional a nivel OPORTUNIDAD (no solo por FECHA):
#    Si existe un APROBADO posterior en la misma OPORTUNIDAD,
#    cualquier "ENVIADO A ANALISTA" anterior de esa OPORTUNIDAD debe ser "NO".
aprob_row_mask = res_up.str.contains(r"\bAPROBAD[OA]\b", na=False) | est_up.str.contains(r"\bAPROBAD[OA]\b", na=False)
last_aprob_por_opp = (
   df_final_validado.loc[aprob_row_mask, ["OPORTUNIDAD", "FECHAHORA"]]
   .groupby("OPORTUNIDAD", as_index=False)["FECHAHORA"].max()
   .rename(columns={"FECHAHORA": "_FECHAHORA_APROBADO_"})
)
# Merge para conocer la última hora de aprobado por OPORTUNIDAD
df_final_validado = df_final_validado.merge(last_aprob_por_opp, on="OPORTUNIDAD", how="left")
enviado_mask = res_up.str.contains(r"ENVIADO\s+A\s+ANALISTA", na=False)
tiene_aprob_en_opp = df_final_validado["_FECHAHORA_APROBADO_"].notna()
es_antes_que_aprob = df_final_validado["FECHAHORA"] < df_final_validado["_FECHAHORA_APROBADO_"]
# Forzar NO en enviados anteriores a un aprobado posterior (misma OPORTUNIDAD)
df_final_validado.loc[enviado_mask & tiene_aprob_en_opp & es_antes_que_aprob, "FLGPENDIENTE"] = "NO"
# Limpieza
df_final_validado.drop(columns=["_RID_", "_FECHAHORA_APROBADO_"], inplace=True, errors="ignore")







df_final_validado.to_csv(
    "OUTPUT/REPORTE_FINAL_VALIDADO.csv", index=False, encoding="utf-8-sig"
)
