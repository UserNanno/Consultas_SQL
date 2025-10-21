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
RUTA_ORGANICO_DIR = "INPUT/ORGANICO"
RANGO_MESES_ORGANICO = range(202505, 202511)  # 202505..202510
EXCLUIR_TIPO_PRODUCTO = ["Crédito Vehicular", "Crédito Estudios"]

# ============================================
# 1) POWERAPP: LECTURA Y BAD DERIVADAS
#    (sin tocar la clave OPORTUNIDAD)
# ============================================
df_powerapp = (
    pd.read_csv(
        RUTA_POWERAPP,
        encoding="utf-8-sig",
        usecols=[
            'Title','FechaAsignacion','Tipo de Producto','ResultadoAnalista','ResultadoCDA',
            'Motivo Derivación - Centralizado','Analista','Motivo Resultado Analista','Largo','AñoMes',
            'Created','Mail','Motivo_MD','Submotivo_MD'
        ]
    )
    .rename(columns={
        'Title': 'OPORTUNIDAD',
        'Tipo de Producto': 'TIPOPRODUCTO',
        'Created': 'CREATED',
        'Motivo_MD': 'MOTIVO',
        'Submotivo_MD': 'SUBMOTIVO',
    })
)

# Normalizaciones SOLO en campos no-clave
df_powerapp['MOTIVO'] = df_powerapp['MOTIVO'].astype(str).str.strip().str.upper()
df_powerapp['SUBMOTIVO'] = df_powerapp['SUBMOTIVO'].astype(str).str.strip().str.upper()

# Fecha auxiliar (no se usa como llave)
created = pd.to_datetime(df_powerapp['CREATED'], utc=True, errors='coerce').dt.tz_convert(TZ_PERU)
df_powerapp['FECHA'] = created.dt.date

# Filtrado "mal derivadas" (no vehículos/estudios, denegado analista y motivo no NaN)
tp_bad_derivadas = df_powerapp[
    (~df_powerapp['TIPOPRODUCTO'].isin(EXCLUIR_TIPO_PRODUCTO)) &
    (df_powerapp['ResultadoAnalista'] == 'Denegado por Analista de credito') &
    (df_powerapp['MOTIVO'] != 'NAN')
][['OPORTUNIDAD', 'MOTIVO', 'SUBMOTIVO']].copy()

tp_bad_derivadas['FLGMALDERIVADO'] = 1
tp_bad_derivadas = tp_bad_derivadas.drop_duplicates(subset=['OPORTUNIDAD'])

# ============================================
# 2) CENTRALIZADO: LECTURA Y CAMPOS BASE
#    (sin tocar la clave NroSolicitud)
# ============================================
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

tp_centralizado = pd.read_excel(
    RUTA_CENTRALIZADO, sheet_name=HOJA_CENTRALIZADO, usecols=COLUMNAS_CENTRALIZADO
).copy()

# Fechas necesarias
tp_centralizado['dFechaSolicitud'] = pd.to_datetime(tp_centralizado['dFechaSolicitud'], errors='coerce')
tp_centralizado['T_Inicio_Evaluacion'] = pd.to_datetime(tp_centralizado['T_Inicio_Evaluacion'], errors='coerce')

# CODMES (de dFechaSolicitud) y LLAVEMATRICULA (= CODMES + MatVendedor)
tp_centralizado['CODMES'] = tp_centralizado['dFechaSolicitud'].dt.strftime('%Y%m')
# No normalizamos MatVendedor (si quieres, puedes aplicar .astype(str).str.strip())
tp_centralizado['LLAVEMATRICULA'] = tp_centralizado['CODMES'].fillna('') + tp_centralizado['MatVendedor'].astype(str)

# ============================================
# 3) MERGE BAD DERIVADAS (NroSolicitud ↔ OPORTUNIDAD)
# ============================================
tp_centralizado = tp_centralizado.merge(
    tp_bad_derivadas[['OPORTUNIDAD', 'FLGMALDERIVADO', 'MOTIVO']],
    left_on='NroSolicitud',
    right_on='OPORTUNIDAD',
    how='left'
)

tp_centralizado['FLGMALDERIVADO'] = tp_centralizado['FLGMALDERIVADO'].fillna(0).astype(int)
tp_centralizado['MOTIVO_MALDERIVADO'] = np.where(
    tp_centralizado['FLGMALDERIVADO'].eq(1),
    tp_centralizado['MOTIVO'],
    np.nan
)
tp_centralizado.drop(columns=['OPORTUNIDAD', 'MOTIVO'], inplace=True, errors='ignore')

# ============================================
# 4) ORGÁNICO: LECTURA MULTIMES Y MERGE POR LLAVE
#    (traer: Agencia → AGENCIA, Nombre Corto Superior → GERENTE_AGENCIA,
#            Unidad Organizativa → UNIDAD_ORGANICA, Servicio/Tribu/COE → SERVICIO_TRIBU_COE)
# ============================================
lista_org = []
for mes in RANGO_MESES_ORGANICO:
    ruta = os.path.join(RUTA_ORGANICO_DIR, f"1n_Activos_{mes}.xlsx")
    try:
        lista_org.append(pd.read_excel(ruta))
    except Exception as e:
        print(f"[AVISO] No se pudo leer {ruta}: {e}")

if len(lista_org) == 0:
    df_organico = pd.DataFrame(columns=[
        'LlaveCodMat','Agencia','Nombre Corto Superior','Unidad Organizativa','Servicio/Tribu/COE','CODMES'
    ])
else:
    df_organico = pd.concat(lista_org, ignore_index=True)

# Llave orgánico (no es clave sensible del usuario; si quieres, puedes strip)
df_organico['LlaveCodMat'] = df_organico.get('LlaveCodMat', '').astype(str)

# Selección de columnas orgánico reales
cols_org = ['LlaveCodMat', 'Agencia', 'Nombre Corto Superior', 'Unidad Organizativa', 'Servicio/Tribu/COE']
faltan = [c for c in cols_org if c not in df_organico.columns]
if faltan:
    print("[AVISO] Faltan columnas en orgánico:", faltan)

tp_centralizado = tp_centralizado.merge(
    df_organico[[c for c in cols_org if c in df_organico.columns]],
    left_on='LLAVEMATRICULA',
    right_on='LlaveCodMat',
    how='left',
    suffixes=('', '_ORG')
)

# Renombres de salida
ren_org = {
    'Agencia': 'AGENCIA',
    'Nombre Corto Superior': 'GERENTE_AGENCIA',
    'Unidad Organizativa': 'UNIDAD_ORGANICA_ORG',   # evitar choque con col homónima en centralizado
    'Servicio/Tribu/COE': 'SERVICIO_TRIBU_COE'
}
tp_centralizado.rename(columns={k: v for k, v in ren_org.items() if k in tp_centralizado.columns}, inplace=True)

# Si quieres priorizar el orgánico sobre la columna 'Unidad Organizativa' del centralizado:
tp_centralizado['UNIDAD_ORGANICA'] = tp_centralizado.get('UNIDAD_ORGANICA_ORG', np.nan)
tp_centralizado.drop(columns=['LlaveCodMat', 'UNIDAD_ORGANICA_ORG'], inplace=True, errors='ignore')

# ============================================
# 5) CÁLCULOS: TIEMPO_ASESOR y FLG_TIEMPO
# ============================================
tp_centralizado['TIEMPO_ASESOR'] = tp_centralizado['T_Inicio_Evaluacion'] - tp_centralizado['dFechaSolicitud']
# FLG_TIEMPO = 1 si T_Inicio_Evaluacion es nulo, caso contrario 0
tp_centralizado['FLG_TIEMPO'] = tp_centralizado['T_Inicio_Evaluacion'].isna().astype(int)

# (Opcional) En horas:
# tp_centralizado['TIEMPO_ASESOR_HORAS'] = tp_centralizado['TIEMPO_ASESOR'].dt.total_seconds() / 3600

# ============================================
# 6) ORDEN / VISTA RÁPIDA
# ============================================
cols_vista = [
    'FLGMALDERIVADO', 'MOTIVO_MALDERIVADO',
    'CODMES', 'LLAVEMATRICULA',
    'AGENCIA', 'GERENTE_AGENCIA', 'UNIDAD_ORGANICA', 'SERVICIO_TRIBU_COE',
    'TIEMPO_ASESOR', 'FLG_TIEMPO'
]
# Mover estas al final (si existen)
for c in cols_vista:
    if c in tp_centralizado.columns:
        tp_centralizado = tp_centralizado[[col for col in tp_centralizado.columns if col != c] + [c]]

print(tp_centralizado[[c for c in cols_vista if c in tp_centralizado.columns]].head(10))
# Export opcional:
# tp_centralizado.to_excel("OUTPUT/tp_centralizado_enriquecido.xlsx", index=False)
# tp_centralizado.to_parquet("OUTPUT/tp_centralizado_enriquecido.parquet", index=False)
