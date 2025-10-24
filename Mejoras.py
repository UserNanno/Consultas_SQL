import pandas as pd
import glob
import os

# Ruta donde están tus archivos CSV
ruta = "INPUT/POWERAPP/"

# Buscar todos los archivos CSV que empiecen con '1n_Apps_2025'
archivos = glob.glob(os.path.join(ruta, "1n_Apps_2025*.csv"))

# Leer y concatenar todos los CSV en un solo DataFrame
df = pd.concat((pd.read_csv(f) for f in archivos), ignore_index=True)

# Mostrar los nombres de las columnas para ver cuáles quieres conservar
# Ejemplo: seleccionar solo algunas columnas
columnas_deseadas = ['Nombre', 'ID', 'Fecha', 'Estado']  # cambia por tus columnas reales
df_filtrado = df[columnas_deseadas]

# Ver los primeros registros
df_filtrado.head()
