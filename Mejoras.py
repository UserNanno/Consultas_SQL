import pandas as pd
import glob
import os

# Ruta de la carpeta
ruta = "INPUT/POWERAPP"

# Patrón de archivos (ajústalo si cambia el nombre base)
patron = os.path.join(ruta, "1n_Apps_2025*.csv")

# Lista de archivos que coinciden con el patrón
archivos = glob.glob(patron)

# Leer y concatenar todos los CSV
dataframes = [pd.read_csv(archivo) for archivo in archivos]
df_unido = pd.concat(dataframes, ignore_index=True)

# Verificamos el resultado
print(f"Se han unido {len(archivos)} archivos.")
print(df_unido.shape)
print(df_unido.head())
