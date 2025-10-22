import pandas as pd
import glob
import os


pd.set_option("display.max_columns", None)
pd.set_option('display.max_rows', None)
pd.set_option("display.width", None)
TZ_PERU = "America/Lima"


# Ruta de la carpeta
ruta = "INPUT/POWERAPP"

# Patrón de archivos (ajústalo si cambia el nombre base)
patron = os.path.join(ruta, "1n_Apps_2025*.csv")

# Lista de archivos que coinciden con el patrón
archivos = glob.glob(patron)

# Leer y concatenar todos los CSV
dataframes = [pd.read_csv(archivo) for archivo in archivos]
df_unido = pd.concat(dataframes, ignore_index=True)

created = pd.to_datetime(df_unido['Created'], utc=True, errors='coerce').dt.tdf_unidoz_convert(TZ_PERU)
df_unido['CREATED_LOCAL'] = created.dt.strftime('%Y-%m-%d %H:%M:%S')

# Verificamos el resultado
print(f"Se han unido {len(archivos)} archivos.")
print(df_unido.shape)
print(df_unido.head())


---------------------------------------------------------------------------
AttributeError                            Traceback (most recent call last)
Cell In[157], line 14
     11 dataframes = [pd.read_csv(archivo) for archivo in archivos]
     12 df_unido = pd.concat(dataframes, ignore_index=True)
---> 14 created = pd.to_datetime(df_unido['Created'], utc=True, errors='coerce').dt.tdf_unidoz_convert(TZ_PERU)
     15 df_unido['CREATED_LOCAL'] = created.dt.strftime('%Y-%m-%d %H:%M:%S')
     17 # Verificamos el resultado

AttributeError: 'DatetimeProperties' object has no attribute 'tdf_unidoz_convert'


Created viene 2025-01-02T15:26:35Z
