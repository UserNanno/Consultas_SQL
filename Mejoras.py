import pandas as pd
import glob
import os

# Ruta donde están los archivos Excel
PATH_ORGANICO = "INPUT/ORGANICO/"
FILES_XLSX = glob.glob(os.path.join(PATH_ORGANICO, "1n_Activos_2025*.xlsx"))

if not FILES_XLSX:
    raise FileNotFoundError("⚠️ No se encontraron archivos 1n_Activos_2025*.xlsx en INPUT/ORGANICO/")

# Leer y concatenar todos los archivos
df_organico = pd.concat(
    (pd.read_excel(f, sheet_name=0) for f in FILES_XLSX),
    ignore_index=True
)

# Mostrar las primeras filas para verificar
df_organico.head()
