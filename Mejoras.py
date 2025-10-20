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






df_pendientes_tcstock_base = df_pendientes_tcstock.loc[
    df_pendientes_tcstock["ESTADO"] == "Pendiente",
    ["OPORTUNIDAD", "DESTIPACCION", "ESTADO", "ANALISTA_MATCH", "FECINICIOPASO"]
].copy()

df_pendientes_tcstock_base["FECHA"] = df_pendientes_tcstock_base["FECINICIOPASO"].astype(str).str[:10]
df_pendientes_tcstock_base["HORA"] = df_pendientes_tcstock_base["FECINICIOPASO"].astype(str).str[11:]
df_pendientes_tcstock_base["FECHAHORA"] = df_pendientes_tcstock_base["FECINICIOPASO"].astype(str).str[:16]

df_pendientes_tcstock_base = df_pendientes_tcstock_base.merge(
    df_equipos[["ANALISTA", "EQUIPO"]],
    left_on="ANALISTA_MATCH", right_on="ANALISTA", how="left"
).merge(
    df_clasificacion[["NOMBRE", "EXPERTISE"]],
    left_on="ANALISTA_MATCH", right_on="NOMBRE", how="left"
)

df_pendientes_tcstock_final = df_pendientes_tcstock_base.rename(columns={
    "DESTIPACCION": "TIPOPRODUCTO",
    "ESTADO": "RESULTADOANALISTA",
    "ANALISTA_MATCH": "ANALISTA"
})[[
    "OPORTUNIDAD", "TIPOPRODUCTO", "RESULTADOANALISTA",
    "ANALISTA", "FECHA", "HORA", "EXPERTISE", "FECHAHORA", "EQUIPO"
]].copy()

df_pendientes_tcstock_final = df_pendientes_tcstock_final[
    df_pendientes_tcstock_final["TIPOPRODUCTO"].isin(["TC", "UPGRADE", "AMPLIACION", "ADICIONAL", "BT", "VENTA COMBO TC"])
]
df_pendientes_tcstock_final["FLGPENDIENTE"] = 1

cols_analista = df_pendientes_tcstock_final.loc[:, 'ANALISTA']
iguales = cols_analista.iloc[:, 0].equals(cols_analista.iloc[:, 1])

if iguales:
    df_pendientes_tcstock_final = df_pendientes_tcstock_final.loc[:, ~df_pendientes_tcstock_final.columns.duplicated()]
df_pendientes_tcstock_final.head()

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



df_diario["FUENTE"] = "POWERAPP"
df_pendientes_tcstock_final["FUENTE"] = "TCSTOCK"
df_pendientes_cef_final["FUENTE"] = "CEF"








# ====== NORMALIZACIÓN Y ÚLTIMO ESTADO POR OPORTUNIDAD (sin mezclar tz) ======
df = df_final_validado.copy()

# 0) Normaliza strings clave
for col in ["OPORTUNIDAD", "ANALISTA", "TIPOPRODUCTO", "RESULTADOANALISTA", "EQUIPO", "EXPERTISE", "FUENTE"]:
    if col in df.columns:
        df[col] = df[col].astype(str).str.strip()

# 1) (CLAVE) Reconstruye SIEMPRE FECHAHORA desde FECHA + HORA -> todo naive (hora Lima)
#    Evita mezclar tz-aware/naive que venía de distintas fuentes
if "FECHA" in df.columns and "HORA" in df.columns:
    df["FECHAHORA"] = pd.to_datetime(
        df["FECHA"].astype(str).str.strip() + " " + df["HORA"].astype(str).str.strip(),
        errors="coerce"  # , dayfirst=True  # activa si tus FECHAs están en D/M/A
    )
else:
    # Fallback extremo: si no tienes FECHA/HORA, limpia FECHAHORA y quita tz si la hubiera
    df["FECHAHORA"] = pd.to_datetime(df.get("FECHAHORA"), errors="coerce")
    df["FECHAHORA"] = df["FECHAHORA"].dt.tz_localize(None)

# 2) Asegura FUENTE (para prioridad). Si no existe, marca 'OTRO'
if "FUENTE" not in df.columns:
    df["FUENTE"] = "OTRO"
prioridad = {"POWERAPP": 3, "CEF": 2, "TCSTOCK": 1, "OTRO": 0}
df["PRIORIDAD"] = df["FUENTE"].map(prioridad).fillna(0)

# 3) Normaliza FLGPENDIENTE a 0/1 si viene como texto
if "FLGPENDIENTE" in df.columns and df["FLGPENDIENTE"].dtype == object:
    df["FLGPENDIENTE"] = (
        df["FLGPENDIENTE"].astype(str).str.upper().map({"SI": 1, "NO": 0})
        .fillna(0).astype(int)
    )

# (Opcional) FLGFACA a partir del resultado, si quieres tenerlo ya aquí
if "RESULTADOANALISTA" in df.columns:
    df["FLGFACA"] = df["RESULTADOANALISTA"].astype(str).str.upper().str.contains("FACA", na=False)\
        .map({True: "SI", False: "NO"})

# 4) Quédate con el ÚLTIMO por OPORTUNIDAD (ordena por fecha y prioridad)
df = (
    df.dropna(subset=["FECHAHORA"])
      .sort_values(["OPORTUNIDAD", "FECHAHORA", "PRIORIDAD"], ascending=[True, True, False])
      .drop_duplicates(subset="OPORTUNIDAD", keep="last")
)

# 5) Devuelve FLGPENDIENTE a "SI/NO" si lo exportas como texto
if "FLGPENDIENTE" in df.columns:
    df["FLGPENDIENTE"] = df["FLGPENDIENTE"].map({1: "SI", 0: "NO"})

# 6) Reemplaza el consolidado
df_final_validado = df
# ====== FIN BLOQUE ======

print(df_final_validado["FECHAHORA"].head(5))
print(df_final_validado["FECHAHORA"].astype(str).str.contains(r"[+-]\d{2}:\d{2}|Z").value_counts())






df_final_validado.to_csv(
    "OUTPUT/REPORTE_FINAL_VALIDADO.csv", index=False, encoding="utf-8-sig"
)

df_final_sin_validacion = pd.concat(
    [df_diario, df_pendientes_tcstock_sin_validar, df_pendientes_cef_sin_validar],
    ignore_index=True
)

df_final_sin_validacion["FLGPENDIENTE"] = df_final_sin_validacion["FLGPENDIENTE"].map({0: "NO", 1: "SI"})

df_final_sin_validacion.to_csv(
    "OUTPUT/REPORTE_FINAL_SIN_VALIDACION.csv", index=False, encoding="utf-8-sig"
)
