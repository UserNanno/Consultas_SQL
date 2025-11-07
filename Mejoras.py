import pandas as pd

# 1) Normalizar llaves
def _norm(df):
    df = df.copy()
    df["CORREO"] = df["CORREO"].str.strip().str.lower()
    # Asegurar tipo consistente de CODMES (elige uno: string o int). Aquí string.
    df["CODMES"] = df["CODMES"].astype(str).str.strip()
    return df

tp = _norm(tp_powerapp_clean)
org = _norm(df_organico)

# 2) Dejar único df_organico por (CODMES, CORREO)
#    ⚠️ Si existe más de un registro por llave, nos quedamos con el "último".
#    Si tienes una columna de fecha de vigencia/actualización, ordénala aquí para que "último" signifique “más reciente”.
#    Ejemplo alternativo (si tuvieras FECHA_ACT): .sort_values(["CODMES","CORREO","FECHA_ACT"])
org_unique = (
    org
    .sort_values(["CODMES","CORREO"])  # ajusta el criterio si tienes una fecha mejor
    .drop_duplicates(subset=["CODMES","CORREO"], keep="last")
)

# 2.1) Comprobar si aún quedan llaves duplicadas en org (no debería)
dups = (org.groupby(["CODMES","CORREO"]).size()
        .reset_index(name="n").query("n>1"))
if not dups.empty:
    print("Ojo: hay llaves duplicadas en df_organico. Revisa estas combinaciones:")
    print(dups.head(20))

# 3) Merge many-to-one (añadir solo MATORGANICO; agrega más columnas si quieres)
cols_to_add = ["CODMES", "CORREO", "MATORGANICO"]
org_for_merge = org_unique[cols_to_add]

# validate='many_to_one' hará raise si org_for_merge rompe unicidad de la llave.
tp_enriquecido = tp.merge(
    org_for_merge,
    on=["CODMES","CORREO"],
    how="left",
    validate="many_to_one",
    suffixes=("", "_org")
)

# 4) Chequeos útiles (opcionales)
# 4a) ¿Cuántos analistas quedaron sin MATORGANICO?
faltantes = tp_enriquecido["MATORGANICO"].isna().sum()
print(f"Registros sin MATORGANICO tras el cruce: {faltantes}")

# 4b) Ver si el merge hubiera expandido filas (no debería)
assert len(tp_enriquecido) == len(tp), "El merge duplicó filas: revisa unicidad en df_organico."
