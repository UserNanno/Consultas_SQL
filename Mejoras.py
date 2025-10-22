# 1) POWERAPP: LECTURA Y BAD DERIVADAS
import glob

# Columnas esperadas en los PowerApps
COLS_POWERAPP_RAW = [
    'Title','FechaAsignacion','Tipo de Producto','ResultadoAnalista','ResultadoCDA',
    'Motivo Derivación - Centralizado','Analista','Motivo Resultado Analista','Largo','AñoMes',
    'Created','Mail','Motivo_MD','Submotivo_MD'
]
RENAME_POWERAPP = {
    'Title': 'OPORTUNIDAD',
    'Tipo de Producto': 'TIPOPRODUCTO',
    'Created': 'CREATED',
    'Motivo_MD': 'MOTIVO',
    'Submotivo_MD': 'SUBMOTIVO',
}

# Buscar todos los CSV que empiecen con "1n_Apps_" dentro de la carpeta de PowerApp
apps_files = sorted(glob.glob(os.path.join(RUTA_POWERAPP_DIR, "1n_Apps_*.csv")))

dfs_powerapps = []
if apps_files:
    print(f"Se encontraron {len(apps_files)} archivos PowerApp: {apps_files}")
    for f in apps_files:
        try:
            # Intentar leer solo las columnas esperadas
            df_tmp = pd.read_csv(f, encoding='utf-8-sig', usecols=COLS_POWERAPP_RAW)
        except ValueError:
            # Si no trae todas las columnas, seleccionar solo las que existan
            df_tmp = pd.read_csv(f, encoding='utf-8-sig')
            cols_presentes = [c for c in COLS_POWERAPP_RAW if c in df_tmp.columns]
            df_tmp = df_tmp[cols_presentes]
            for c in COLS_POWERAPP_RAW:
                if c not in df_tmp.columns:
                    df_tmp[c] = pd.NA
        df_tmp = df_tmp.rename(columns=RENAME_POWERAPP)
        dfs_powerapps.append(df_tmp)

    df_powerapp = pd.concat(dfs_powerapps, ignore_index=True)
else:
    # Fallback: archivo único (por compatibilidad con versiones anteriores)
    print("[AVISO] No se encontraron archivos 1n_Apps_*.csv, usando POWERAPP.csv por defecto.")
    df_powerapp = (
        pd.read_csv(
            os.path.join(RUTA_POWERAPP_DIR, "POWERAPP.csv"),
            encoding="utf-8-sig",
            usecols=COLS_POWERAPP_RAW
        ).rename(columns=RENAME_POWERAPP)
    )

# Normalizaciones SOLO en campos no-clave
df_powerapp['MOTIVO'] = df_powerapp['MOTIVO'].astype(str).str.strip().str.upper()
df_powerapp['SUBMOTIVO'] = df_powerapp['SUBMOTIVO'].astype(str).str.strip().str.upper()

# Fecha auxiliar (no se usa como llave)
created = pd.to_datetime(df_powerapp['CREATED'], utc=True, errors='coerce').dt.tz_convert(TZ_PERU)
df_powerapp['FECHA'] = created.dt.date
df_powerapp["CODMES"] = pd.to_datetime(df_powerapp["FECHA"]).dt.strftime("%Y%m")

# Filtrado "mal derivadas" (no vehículos/estudios, denegado analista y motivo no NaN)
tp_bad_derivadas = df_powerapp[
    (~df_powerapp['TIPOPRODUCTO'].isin(EXCLUIR_TIPO_PRODUCTO)) &
    (df_powerapp['ResultadoAnalista'] == 'Denegado por Analista de credito') &
    (df_powerapp['MOTIVO'] != 'NAN')
][['OPORTUNIDAD', "CODMES", "FECHA", 'MOTIVO', 'SUBMOTIVO']].copy()

tp_bad_derivadas['FLGMALDERIVADO'] = 1
tp_bad_derivadas = tp_bad_derivadas.drop_duplicates(subset=['OPORTUNIDAD'])



Se encontraron 10 archivos PowerApp: ['INPUT/POWERAPP\\1n_Apps_202501.csv', 'INPUT/POWERAPP\\1n_Apps_202502.csv', 'INPUT/POWERAPP\\1n_Apps_202503.csv', 'INPUT/POWERAPP\\1n_Apps_202504.csv', 'INPUT/POWERAPP\\1n_Apps_202505.csv', 'INPUT/POWERAPP\\1n_Apps_202506.csv', 'INPUT/POWERAPP\\1n_Apps_202507.csv', 'INPUT/POWERAPP\\1n_Apps_202508.csv', 'INPUT/POWERAPP\\1n_Apps_202509.csv', 'INPUT/POWERAPP\\1n_Apps_202510.csv']


tp_bad_derivadas["CODMES"].unique()
array(['202501', '202502', '202503', '202504', '202505', '202507',
       '202508', '202509', '202510'], dtype=object)
