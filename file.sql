import pandas as pd

pd.set_option("display.max_columns", None)
pd.set_option("display.width", None)

df = pd.read_csv(
    "INPUT/POWERAPP.csv",
    encoding="utf-8-sig",
    usecols=["Title", "Tipo de Producto", "ResultadoAnalista", "Created", "Motivo_MD", "Submotivo_MD"]
).rename(columns={
    "Title": "OPORTUNIDAD",
    "Tipo de Producto": "TIPOPRODUCTO",
    "ResultadoAnalista": "RESULTADOANALISTA",
    "Created": "CREATED",
    "Motivo_MD" : "MOTIVO",
    "Submotivo_MD" : "SUBMOTIVO",
})

df["MOTIVO"] = df["MOTIVO"].astype(str).str.strip().str.upper()
df["SUBMOTIVO"] = df["SUBMOTIVO"].astype(str).str.strip().str.upper()
created = pd.to_datetime(df["CREATED"], utc=True, errors="coerce").dt.tz_convert("America/Lima")
df["FECHA"] = created.dt.date
df["CODMES"] = pd.to_datetime(df["FECHA"]).dt.strftime("%Y%m")


excluir = ["Crédito Vehicular", "Crédito Estudios"]

tp_bad_derivadas = df[
    (~df["TIPOPRODUCTO"].isin(excluir)) &
    (df["RESULTADOANALISTA"] == "Denegado por Analista de credito") &
    (df["MOTIVO"] != "NAN")
][["OPORTUNIDAD", "MOTIVO", "SUBMOTIVO", "FECHA", "CODMES"]]

tp_bad_derivadas["FLGMALDERIVADO"] = 1

columnas_deseadas = [
    'NroSolicitud', 'TipoProducto', 'TipoOperación', 'Producto', 'Campaña', 'TipoEvaluación', 'Estado',
    'FecSolicitud', 'FecAprobación', 'FecUltEstado', 'CodSucAge', 'NombreSucAge', 'CanalMic', 'MatVendedor',
    'NombreVendedor', 'DNIVendedorExterno', 'MatFiscalizador', 'NombreFiscalizador', 'EstadoFiscalia',
    'OBSERVACION', 'MatEvaluador', 'NombreEvaluador', 'DesTipoJustificacion', 'MatUsuEvalVerif',
    'NombreUsuEvaluadorVerificador', 'IdcTitular', 'TipoIdcTitular', 'NombreCompletoTitular', 'FecNacTitular',
    'EdadTitular', 'TipoRenta', 'IngresosTitular', 'OtrosIngresosTitular', 'SueldoPromedioTitular',
    'IdcConyuge', 'TipoIdcConyuge', 'NombreCompletoConyuge', 'IngresosConyuge', 'OtrosIngresosConyuge',
    'SueldoPromedioConyuge', 'FecNacConyuge', 'EdadConyuge', 'MontoSolicitado', 'MontoAprobado', 'Cuota',
    'Plazo', 'Tasa', 'TipoMoneda', 'TipoCambio$', 'MontoAprobadoSOLES', 'TipoVariante', 'SegmentoTitular',
    'RucEmpresa', 'Origen', 'AreaEvaluador', 'Equipo', 'NombreApellido', 'Rango_MontoAprobadoSOLES',
    'Area_Vendedor', 'Servicio_Vendedor', 'Unidad Organizativa', 'Rango_MontoAprobadoSOLES2', 'Dia', 'Función',
    'DetalleProducto', 'DetalleStock', 'ResSTOCK', 'RangoTiempo_IngSol-FinEva_24Hrs', 'RANGO HORAS X6',
    'Monto desembolsado', 'flg_des', 'DESCRIP. CAMP', '.Res', '..Mot', '…Cod', 'Montodesembolsado',
    'a.fecdesembolso', '¿Adjuntaron documentos?', 'MontoSolicitado_Soles', 'Canal', 'EstadoFinal', 'MES',
    'N_Semana', 'AÑO', 'dFechaSolicitud', 'T_Inicio_Evaluacion', 'hora_analista_1', 'T_Fin_Evaluacion',
    'Tiempo_Asesor', 'Tiempo_Analista', 'Tiempo_Cliente', 'Dia_FechaCreacion', 'Mes_FechaCreacion',
    'Dia_Util_FechaCreacion', 'FechaCreacion_sin hora', 'Hora_número_FechaCreacion', 'Corte_FechaCreacion',
    'Atendido en horario_FechaCreacion', 'Dia_Inicio_Evaluacion', 'Mes_Inicio_Evaluacion',
    'Dia_Util_Inicio_Evaluacion', 'Inicio_Evaluacion_sin hora', 'Hora_número_Inicio_Evaluacion',
    'Corte_Inicio_Evaluacion', 'Atendido en horario_Inicio_Evaluacion', 'Corte_Bandeja del Analista',
    'Dia_Fin_Evaluacion', 'Mes_Fin_Evaluacion', 'Dia_Util_Fin_Evaluacion', 'Fin_Evaluacion_sin hora',
    'Hora_número_Fin_Evaluacion', 'Corte_Fin_Evaluacion', 'Atendido en horario_Fin_Evaluacion',
    'Atendido en horario_Fin_Evaluacion_SabyDom', 'Atendido en horario_Fin_Evaluacion2',
    'RangoHrs_Tiempo_Analista', 'TIEMPO CLIENTE X12 HORAS- CLIENTE', 'TIEMPO CLIENTE X24 HORAS- CLIENTE',
    'RangoHrs_Tiempo_Cliente', '.', 'destipestadosolicitud', 'ResultadoAnalista', 'Etapa', 'Flag_Desembolso',
    'Flag_Desestimada_automatico', 'Segmento', 'DesProducto', 'Rango_MontoSolicitadoSolesAntes',
    'Rango_MontoAprobadoSolesAntes', 'Rango_MontoAprobadoSolesAntesOtroRango',
    'RangoHrs_Tiempo_Cliente_Nuevo', 'Mal Derivada', 'Mal derivada (sub motivo)', 'Filtro Killer',
    'mesfiltro', '3ra ', 'Llave', 'Motivo Resultado Analista', 'Codclavecic'
]

tp_centralizado_u6m = pd.read_excel('INPUT/BASECENTRALIZADO.xlsx', sheet_name='CENTRALIZADO', usecols=columnas_deseadas)

df["dFechaSolicitud"] = pd.to_datetime(df["dFechaSolicitud"], errors='coerce')
df["CODMES"] = df["dFechaSolicitud"].dt.strftime("%Y%m").astype(int)


import os

meses = range(202505, 202511)  # de mayo a octubre 2025

lista_df = []

for mes in meses:
    archivo = os.path.join('INPUT/ORGANICO/', f'1n_Activos_{mes}.xlsx')
    try:
        df_mes = pd.read_excel(archivo)
        lista_df.append(df_mes)
    except Exception as e:
        print(f"No se pudo leer {archivo}: {e}")

df_organico_u6m = pd.concat(lista_df, ignore_index=True)

print(df_organico_u6m['CODMES'].value_counts())



mi df to_centralizado es el principal a eso quiero unirle los otros dos con algunas condiciones:
Primero, como puedes ver tengo tp_bad_derivadas["FLGMALDERIVADO"] = 1. Ahora en tp_centralizado también debe haber una columna de FLGMALDERIVADO que sea 0 si no coincide con tp_bad_derivadas["OPORTUNIDAD"] Ya que de mis casos, aca los marco como mal derivados.
    También quiero obtener el MOTIVO de tp_bad_derivadas (Si es mal derivado). 
Luego en una columan calculada LLAVEMATRICULA que sea el CODMES
