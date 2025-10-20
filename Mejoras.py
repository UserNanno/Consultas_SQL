# ====== NORMALIZACIÓN Y ÚLTIMO ESTADO POR OPORTUNIDAD (sin mezclar tz) ======
df = df_final_validado.copy()

# 0) Normaliza strings clave
for col in ["OPORTUNIDAD", "ANALISTA", "TIPOPRODUCTO", "RESULTADOANALISTA", "EQUIPO", "EXPERTISE", "FUENTE"]:
    if col in df.columns:
        df[col] = df[col].astype(str).str.strip()

# 1) (CLAVE) Reconstruye SIEMPRE FECHAHORA desde FECHA + HORA -> todo naive (hora Lima)
#    Evita mezclar tz-aware/naive que venía de distintas fuentes
if "FECHA" in df.columns and "HORA" in df.columns:
    df["FECHAHORA"] = pd.to_datetime(
        df["FECHA"].astype(str).str.strip() + " " + df["HORA"].astype(str).str.strip(),
        errors="coerce"  # , dayfirst=True  # activa si tus FECHAs están en D/M/A
    )
else:
    # Fallback extremo: si no tienes FECHA/HORA, limpia FECHAHORA y quita tz si la hubiera
    df["FECHAHORA"] = pd.to_datetime(df.get("FECHAHORA"), errors="coerce")
    df["FECHAHORA"] = df["FECHAHORA"].dt.tz_localize(None)

# 2) Asegura FUENTE (para prioridad). Si no existe, marca 'OTRO'
if "FUENTE" not in df.columns:
    df["FUENTE"] = "OTRO"
prioridad = {"POWERAPP": 3, "CEF": 2, "TCSTOCK": 1, "OTRO": 0}
df["PRIORIDAD"] = df["FUENTE"].map(prioridad).fillna(0)

# 3) Normaliza FLGPENDIENTE a 0/1 si viene como texto
if "FLGPENDIENTE" in df.columns and df["FLGPENDIENTE"].dtype == object:
    df["FLGPENDIENTE"] = (
        df["FLGPENDIENTE"].astype(str).str.upper().map({"SI": 1, "NO": 0})
        .fillna(0).astype(int)
    )

# (Opcional) FLGFACA a partir del resultado, si quieres tenerlo ya aquí
if "RESULTADOANALISTA" in df.columns:
    df["FLGFACA"] = df["RESULTADOANALISTA"].astype(str).str.upper().str.contains("FACA", na=False)\
        .map({True: "SI", False: "NO"})

# 4) Quédate con el ÚLTIMO por OPORTUNIDAD (ordena por fecha y prioridad)
df = (
    df.dropna(subset=["FECHAHORA"])
      .sort_values(["OPORTUNIDAD", "FECHAHORA", "PRIORIDAD"], ascending=[True, True, False])
      .drop_duplicates(subset="OPORTUNIDAD", keep="last")
)

# 5) Devuelve FLGPENDIENTE a "SI/NO" si lo exportas como texto
if "FLGPENDIENTE" in df.columns:
    df["FLGPENDIENTE"] = df["FLGPENDIENTE"].map({1: "SI", 0: "NO"})

# 6) Reemplaza el consolidado
df_final_validado = df
# ====== FIN BLOQUE ======






print(df_final_validado["FECHAHORA"].head(5))
print(df_final_validado["FECHAHORA"].astype(str).str.contains(r"[+-]\d{2}:\d{2}|Z").value_counts())
