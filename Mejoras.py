# ============================================
# LIBRERÍAS Y CONFIG
# ============================================
import os
import numpy as np
import pandas as pd

pd.set_option("display.max_columns", None)
pd.set_option("display.width", None)

TZ_PERU = "America/Lima"

# ============================================
# PARÁMETROS / RUTAS
# ============================================
RUTA_POWERAPP = "INPUT/POWERAPP.csv"
RUTA_CENTRALIZADO = "INPUT/BASECENTRALIZADO.xlsx"
HOJA_CENTRALIZADO = "CENTRALIZADO"
RUTA_ORGANICO_DIR = "INPUT/ORGANICO"   # carpeta que contiene los 1n_Activos_YYYYMM.xlsx
RANGO_MESES_ORGANICO = range(202505, 202511)  # mayo (202505) a oct (202510) 2025
EXCLUIR_TIPO_PRODUCTO = ["Crédito Vehicular", "Crédito Estudios"]

COLUMNAS_CENTRALIZADO = [
    'NroSolicitud','TipoProducto','TipoOperación','Producto','Campaña','TipoEvaluación','Estado',
    'FecSolicitud','FecAprobación','FecUltEstado','CodSucAge','NombreSucAge','CanalMic','MatVendedor',
    'NombreVendedor','DNIVendedorExterno','MatFiscalizador','NombreFiscalizador','EstadoFiscalia',
    'OBSERVACION','MatEvaluador','NombreEvaluador','DesTipoJustificacion','MatUsuEvalVerif',
    'NombreUsuEvaluadorVerificador','IdcTitular','TipoIdcTitular','NombreCompletoTitular','FecNacTitular',
    'EdadTitular','TipoRenta','IngresosTitular','OtrosIngresosTitular','SueldoPromedioTitular',
    'IdcConyuge','TipoIdcConyuge','NombreCompletoConyuge','IngresosConyuge','OtrosIngresosConyuge',
    'SueldoPromedioConyuge','FecNacConyuge','EdadConyuge','MontoSolicitado','MontoAprobado','Cuota',
    'Plazo','Tasa','TipoMoneda','TipoCambio$','MontoAprobadoSOLES','TipoVariante','SegmentoTitular',
    'RucEmpresa','Origen','AreaEvaluador','Equipo','NombreApellido','Rango_MontoAprobadoSOLES',
    'Area_Vendedor','Servicio_Vendedor','Unidad Organizativa','Rango_MontoAprobadoSOLES2','Dia','Función',
    'DetalleProducto','DetalleStock','ResSTOCK','RangoTiempo_IngSol-FinEva_24Hrs','RANGO HORAS X6',
    'Monto desembolsado','flg_des','DESCRIP. CAMP','.Res','..Mot','…Cod','Montodesembolsado',
    'a.fecdesembolso','¿Adjuntaron documentos?','MontoSolicitado_Soles','Canal','EstadoFinal','MES',
    'N_Semana','AÑO','dFechaSolicitud','T_Inicio_Evaluacion','hora_analista_1','T_Fin_Evaluacion',
    'Tiempo_Asesor','Tiempo_Analista','Tiempo_Cliente','Dia_FechaCreacion','Mes_FechaCreacion',
    'Dia_Util_FechaCreacion','FechaCreacion_sin hora','Hora_número_FechaCreacion','Corte_FechaCreacion',
    'Atendido en horario_FechaCreacion','Dia_Inicio_Evaluacion','Mes_Inicio_Evaluacion',
    'Dia_Util_Inicio_Evaluacion','Inicio_Evaluacion_sin hora','Hora_número_Inicio_Evaluacion',
    'Corte_Inicio_Evaluacion','Atendido en horario_Inicio_Evaluacion','Corte_Bandeja del Analista',
    'Dia_Fin_Evaluacion','Mes_Fin_Evaluacion','Dia_Util_Fin_Evaluacion','Fin_Evaluacion_sin hora',
    'Hora_número_Fin_Evaluacion','Corte_Fin_Evaluacion','Atendido en horario_Fin_Evaluacion',
    'Atendido en horario_Fin_Evaluacion_SabyDom','Atendido en horario_Fin_Evaluacion2',
    'RangoHrs_Tiempo_Analista','TIEMPO CLIENTE X12 HORAS- CLIENTE','TIEMPO CLIENTE X24 HORAS- CLIENTE',
    'RangoHrs_Tiempo_Cliente','.','destipestadosolicitud','ResultadoAnalista','Etapa','Flag_Desembolso',
    'Flag_Desestimada_automatico','Segmento','DesProducto','Rango_MontoSolicitadoSolesAntes',
    'Rango_MontoAprobadoSolesAntes','Rango_MontoAprobadoSolesAntesOtroRango',
    'RangoHrs_Tiempo_Cliente_Nuevo','Mal Derivada','Mal derivada (sub motivo)','Filtro Killer',
    'mesfiltro','3ra ','Llave','Motivo Resultado Analista','Codclavecic'
]

# ============================================
# 1) POWERAPP: LECTURA, LIMPIEZA Y BAD DERIVADAS
# ============================================
df_powerapp = (
    pd.read_csv(
        RUTA_POWERAPP,
        encoding="utf-8-sig",
        usecols=["Title", "Tipo de Producto", "ResultadoAnalista", "Created", "Motivo_MD", "Submotivo_MD"]
    )
    .rename(columns={
        "Title": "OPORTUNIDAD",
        "Tipo de Producto": "TIPOPRODUCTO",
        "ResultadoAnalista": "RESULTADOANALISTA",
        "Created": "CREATED",
        "Motivo_MD": "MOTIVO",
        "Submotivo_MD": "SUBMOTIVO",
    })
)

# Normalizaciones
df_powerapp["MOTIVO"] = df_powerapp["MOTIVO"].astype(str).str.strip().str.upper()
df_powerapp["SUBMOTIVO"] = df_powerapp["SUBMOTIVO"].astype(str).str.strip().str.upper()

# Fechas (CREATED → FECHA / CODMES_CREATION si se necesita)
created = pd.to_datetime(df_powerapp["CREATED"], utc=True, errors="coerce").dt.tz_convert(TZ_PERU)
df_powerapp["FECHA"] = created.dt.date
df_powerapp["CODMES_CREATION"] = pd.to_datetime(df_powerapp["FECHA"]).dt.strftime("%Y%m")

# BAD DERIVADAS (excluyendo productos y motivo no nulo)
tp_bad_derivadas = df_powerapp[
    (~df_powerapp["TIPOPRODUCTO"].isin(EXCLUIR_TIPO_PRODUCTO)) &
    (df_powerapp["RESULTADOANALISTA"] == "Denegado por Analista de credito") &
    (df_powerapp["MOTIVO"] != "NAN")
][["OPORTUNIDAD", "MOTIVO", "SUBMOTIVO"]].copy()

tp_bad_derivadas["FLGMALDERIVADO"] = 1
tp_bad_derivadas["OPORTUNIDAD_str"] = tp_bad_derivadas["OPORTUNIDAD"].astype(str).str.strip()
tp_bad_derivadas = tp_bad_derivadas.drop_duplicates(subset=["OPORTUNIDAD_str"])

# ============================================
# 2) CENTRALIZADO: LECTURA Y NORMALIZACIONES
# ============================================
tp_centralizado = pd.read_excel(
    RUTA_CENTRALIZADO, sheet_name=HOJA_CENTRALIZADO, usecols=COLUMNAS_CENTRALIZADO
).copy()

# Claves limpias
tp_centralizado["NroSolicitud_str"] = tp_centralizado["NroSolicitud"].astype(str).str.strip()
tp_centralizado["MatVendedor"] = tp_centralizado["MatVendedor"].astype(str).str.strip()

# Fechas para cálculos posteriores
tp_centralizado["dFechaSolicitud"] = pd.to_datetime(tp_centralizado["dFechaSolicitud"], errors="coerce")
tp_centralizado["T_Inicio_Evaluacion"] = pd.to_datetime(tp_centralizado["T_Inicio_Evaluacion"], errors="coerce")

# CODMES (a partir de dFechaSolicitud) y LLAVEMATRICULA
tp_centralizado["CODMES"] = tp_centralizado["dFechaSolicitud"].dt.strftime("%Y%m")
tp_centralizado["LLAVEMATRICULA"] = tp_centralizado["CODMES"].fillna("").astype(str).str.strip() + \
                                     tp_centralizado["MatVendedor"].fillna("").astype(str).str.strip()

# ============================================
# 3) MERGE: MAL DERIVADAS → CENTRALIZADO (por NroSolicitud ↔ OPORTUNIDAD)
# ============================================
tp_centralizado = tp_centralizado.merge(
    tp_bad_derivadas[["OPORTUNIDAD_str", "FLGMALDERIVADO", "MOTIVO"]],
    left_on="NroSolicitud_str",
    right_on="OPORTUNIDAD_str",
    how="left"
)

tp_centralizado["FLGMALDERIVADO"] = tp_centralizado["FLGMALDERIVADO"].fillna(0).astype(int)
tp_centralizado["MOTIVO_MALDERIVADO"] = np.where(
    tp_centralizado["FLGMALDERIVADO"].eq(1),
    tp_centralizado["MOTIVO"],
    np.nan
)
tp_centralizado.drop(columns=["OPORTUNIDAD_str", "MOTIVO"], inplace=True, errors="ignore")

# ============================================
# 4) ORGÁNICO: LECTURA (VARIOS MESES) Y MERGE POR LLAVE
# ============================================
lista_organico = []
for mes in RANGO_MESES_ORGANICO:
    ruta = os.path.join(RUTA_ORGANICO_DIR, f"1n_Activos_{mes}.xlsx")
    try:
        lista_organico.append(pd.read_excel(ruta))
    except Exception as e:
        print(f"[AVISO] No se pudo leer {ruta}: {e}")

if len(lista_organico) == 0:
    # Si no hay orgánico, crear df vacío con columnas esperadas
    df_organico = pd.DataFrame(columns=[
        "LlaveCodMat", "AGENCIA", "Nombre Corto Superior",
        "Unidad Organizativa", "Servicio/Tribu/COE"
    ])
else:
    df_organico = pd.concat(lista_organico, ignore_index=True)

org_cols_necesarias = ["LlaveCodMat", "AGENCIA", "Nombre Corto Superior", "Unidad Organizativa", "Servicio/Tribu/COE"]
faltantes = [c for c in org_cols_necesarias if c not in df_organico.columns]
if faltantes:
    print("[AVISO] Faltan columnas en df_organico:", faltantes)

df_organico["LlaveCodMat_str"] = df_organico.get("LlaveCodMat", "").astype(str).str.strip()

tp_centralizado = tp_centralizado.merge(
    df_organico[["LlaveCodMat_str"] + [c for c in org_cols_necesarias if c in df_organico.columns]],
    left_on="LLAVEMATRICULA",
    right_on="LlaveCodMat_str",
    how="left"
)

# Renombrados de salida
rename_org = {
    "Nombre Corto Superior": "GERENTE_AGENCIA",
    "Unidad Organizativa": "UNIDAD_ORGANICA",
    "Servicio/Tribu/COE": "SERVICIO_TRIBU_COE"
}
tp_centralizado.rename(columns={k: v for k, v in rename_org.items() if k in tp_centralizado.columns}, inplace=True)
tp_centralizado.drop(columns=["LlaveCodMat_str"], inplace=True, errors="ignore")

# ============================================
# 5) CÁLCULOS: TIEMPO_ASESOR y FLG_TIEMPO
# ============================================
tp_centralizado["TIEMPO_ASESOR"] = tp_centralizado["T_Inicio_Evaluacion"] - tp_centralizado["dFechaSolicitud"]
tp_centralizado["FLG_TIEMPO"] = tp_centralizado["T_Inicio_Evaluacion"].isna().astype(int)

# (Opcional) En horas:
# tp_centralizado["TIEMPO_ASESOR_HORAS"] = tp_centralizado["TIEMPO_ASESOR"].dt.total_seconds() / 3600

# ============================================
# 6) ORDEN / VISTA RÁPIDA
# ============================================
cols_nuevas = [
    "FLGMALDERIVADO","MOTIVO_MALDERIVADO","CODMES","LLAVEMATRICULA",
    "AGENCIA","GERENTE_AGENCIA","UNIDAD_ORGANICA","SERVICIO_TRIBU_COE",
    "TIEMPO_ASESOR","FLG_TIEMPO"
]
# Mover nuevas al final si existen
for c in cols_nuevas:
    if c in tp_centralizado.columns:
        tp_centralizado = tp_centralizado[[col for col in tp_centralizado.columns if col != c] + [c]]

print(tp_centralizado[cols_nuevas].head(10))
# Si quieres exportar:
# tp_centralizado.to_parquet("OUTPUT/tp_centralizado_enriquecido.parquet", index=False)
# tp_centralizado.to_excel("OUTPUT/tp_centralizado_enriquecido.xlsx", index=False)
