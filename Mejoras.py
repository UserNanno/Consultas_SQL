# --- BLOQUE DE NORMALIZACIÓN Y ÚLTIMO ESTADO POR OPORTUNIDAD ---

# 🔹 Asegura que no haya conflicto de zona horaria
if "FECHAHORA" in df_final_validado.columns:
    df_final_validado["FECHAHORA"] = pd.to_datetime(df_final_validado["FECHAHORA"], errors="coerce").dt.tz_localize(None)

# 🔹 Normaliza tipos mínimos
df = df_final_validado.copy()
for col in ["OPORTUNIDAD", "ANALISTA", "TIPOPRODUCTO", "RESULTADOANALISTA", "EQUIPO", "EXPERTISE", "FUENTE"]:
    if col in df.columns:
        df[col] = df[col].astype(str).str.strip()

# 🔹 FECHAHORA a datetime; si falta, intenta FECHA + HORA
df["FECHAHORA"] = pd.to_datetime(df.get("FECHAHORA"), errors="coerce")
if "FECHA" in df.columns and "HORA" in df.columns:
    mask_nat = df["FECHAHORA"].isna() & df["FECHA"].notna() & df["HORA"].notna()
    if mask_nat.any():
        df.loc[mask_nat, "FECHAHORA"] = pd.to_datetime(
            df.loc[mask_nat, "FECHA"].astype(str).str.strip() + " " +
            df.loc[mask_nat, "HORA"].astype(str).str.strip(),
            errors="coerce"
            # , dayfirst=True  # <- activa si tus fechas están en formato D/M/A
        )

# 🔹 Prioridad de fuentes para desempatar en mismo minuto
prioridad = {"POWERAPP": 3, "CEF": 2, "TCSTOCK": 1}
df["PRIORIDAD"] = df["FUENTE"].map(prioridad).fillna(0)

# 🔹 Si FLGPENDIENTE viene como "SI/NO", pásalo a 0/1 (así no se rompe el sort)
if "FLGPENDIENTE" in df.columns and df["FLGPENDIENTE"].dtype == object:
    df["FLGPENDIENTE"] = df["FLGPENDIENTE"].str.upper().map({"SI": 1, "NO": 0}).fillna(0).astype(int)

# 🔹 QUÉDATE CON EL ÚLTIMO POR OPORTUNIDAD
df = (
    df.dropna(subset=["FECHAHORA"])
      .sort_values(["OPORTUNIDAD", "FECHAHORA", "PRIORIDAD"], ascending=[True, True, False])
      .drop_duplicates(subset="OPORTUNIDAD", keep="last")
)

# 🔹 Deja FLGPENDIENTE en "SI/NO" para exportar (si la usas así)
if "FLGPENDIENTE" in df.columns:
    df["FLGPENDIENTE"] = df["FLGPENDIENTE"].map({1: "SI", 0: "NO"})

# 🔹 Reemplaza el consolidado final
df_final_validado = df

# --- FIN DEL BLOQUE ---

# Y luego exportas como ya lo hacías:
df_final_validado.to_csv(
    "OUTPUT/REPORTE_FINAL_VALIDADO.csv", index=False, encoding="utf-8-sig"
)
