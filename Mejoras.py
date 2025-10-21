import pandas as pd

print(pd.read_csv("INPUT/POWERAPP.csv", nrows=0).columns.tolist())
print(pd.read_excel("INPUT/BASECENTRALIZADO.xlsx", sheet_name="CENTRALIZADO", nrows=0).columns.tolist())
print(pd.read_excel("INPUT/ORGANICO/1n_Activos_202505.xlsx", nrows=0).columns.tolist())
