# 1) Normalizar
tp_powerapp_clean["CODMES"] = tp_powerapp_clean["CODMES"].astype(str)
df_organico["CODMES"] = df_organico["CODMES"].astype(str)

tp_powerapp_clean["CORREO"] = tp_powerapp_clean["CORREO"].str.lower().str.strip()
df_organico["CORREO"] = df_organico["CORREO"].str.lower().str.strip()

# 2) Asegurar llave única en df_organico
# (a) Detectar si hay más de un MATORGANICO por CODMES+CORREO
conflictos = (
    df_organico.groupby(["CODMES", "CORREO"])["MATORGANICO"]
    .nunique()
    .reset_index(name="n_unicos")
)
conflictos = conflictos[conflictos["n_unicos"] > 1]

print(conflictos)
        CODMES                    CORREO  n_unicos
0       202501                         -        78
26019   202502                         -        80
52152   202503                         -        77
78434   202504                         -        91
104782  202505                         -       337
113954  202505                    correo         2
131085  202506                         -        72
157915  202507                         -        86
185008  202508                         -        82
212451  202509                         -        64
240289  202510                         -        89

# Si hay conflictos, decide tu regla. Ejemplos de reglas:
# - Quedarte con la última fila según algún criterio temporal si existiera (p.ej. FECHA_ALTA)
# - O, si no hay fecha, quedarte con la última aparición en el propio df (mantener consistencia)

# (b) Resolver duplicados (sin columna temporal, nos quedamos con la última aparición)
df_org_unico = (
    df_organico
    .sort_values(["CODMES", "CORREO"])                # ajusta el orden si tienes una fecha y prefieres "más reciente"
    .drop_duplicates(["CODMES", "CORREO"], keep="last")
)

# 3) Merge protegido
tp_powerapp_clean = tp_powerapp_clean.merge(
    df_org_unico[["CODMES", "CORREO", "MATORGANICO"]],
    on=["CODMES", "CORREO"],
    how="left",
    validate="m:1"   # m:1 = muchas filas de tp_powerapp_clean a 1 de df_org_unico
)

# (Opcional) Chequeo de seguridad: asegurar que no creció el número de filas
assert len(tp_powerapp_clean) == len(tp_powerapp_clean.drop_duplicates(subset=["CODSOLICITUD"])), \
       "Advertencia: se podrían haber generado duplicados de solicitudes."
