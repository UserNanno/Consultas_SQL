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
