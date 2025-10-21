# ============================================
# 1) POWERAPP (MULTI-CSV): LECTURA Y BAD DERIVADAS
#    Lee todos los INPUT/POWERAPP/1n_Apps_*.csv y los une
#    (sin tocar la clave OPORTUNIDAD)
# ============================================
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

# --- Limpieza / normalización ---
df_powerapp['MOTIVO'] = df_powerapp['MOTIVO'].astype(str).str.strip().str.upper()
df_powerapp['SUBMOTIVO'] = df_powerapp['SUBMOTIVO'].astype(str).str.strip().str.upper()

created = pd.to_datetime(df_powerapp['CREATED'], utc=True, errors='coerce').dt.tz_convert(TZ_PERU)
df_powerapp['FECHA'] = created.dt.date

# --- BAD DERIVADAS ---
tp_bad_derivadas = df_powerapp[
    (~df_powerapp['TIPOPRODUCTO'].isin(EXCLUIR_TIPO_PRODUCTO)) &
    (df_powerapp['ResultadoAnalista'] == 'Denegado por Analista de credito') &
    (df_powerapp['MOTIVO'] != 'NAN')
][['OPORTUNIDAD', 'MOTIVO', 'SUBMOTIVO']].copy()

tp_bad_derivadas['FLGMALDERIVADO'] = 1
tp_bad_derivadas = tp_bad_derivadas.drop_duplicates(subset=['OPORTUNIDAD'])
