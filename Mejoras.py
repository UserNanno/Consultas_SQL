import re

# Quitar ceros iniciales antes de una letra
df_organico["MATRICULA_SUPERIOR"] = (
    df_organico["MATRICULA_SUPERIOR"]
    .astype(str)
    .str.strip()
    .str.upper()
    .str.replace(r'^0(?=[A-Z]\d{5})', '', regex=True)
)
