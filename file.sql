import pandas as pd

pd.set_option("display.max_columns", None)
pd.set_option("display.max_rows", None)
pd.set_option("display.width", None)

# Base PowerApp
df = pd.read_csv(
    "INPUT/POWERAPP.csv",
    encoding="utf-8-sig",
    usecols=["Title", "Tipo de Producto", "ResultadoAnalista", "Analista", "Created"]
).rename(columns={
    "Title": "OPORTUNIDAD",
    "Tipo de Producto": "TIPOPRODUCTO",
    "ResultadoAnalista": "RESULTADOANALISTA",
    "Analista": "ANALISTA",
    "Created": "CREATED",
})
df["ANALISTA"] = df["ANALISTA"].astype(str).str.strip().str.upper()
df["TIPOPRODUCTO"] = df["TIPOPRODUCTO"].astype(str).str.strip().str.upper()
df["RESULTADOANALISTA"] = df["RESULTADOANALISTA"].astype(str).str.strip().str.upper()

# Catálogos
df_clasificacion = pd.read_csv(
    "INPUT/CLASIFICACION_ANALISTAS.csv",
    encoding="utf-8-sig",
    usecols=["NOMBRE", "EXPERTISE"]
)
df_clasificacion = df_clasificacion.drop_duplicates(subset=["NOMBRE"])

df_equipos = pd.read_csv(
    "INPUT/EQUIPOS.csv",
    delimiter=";",
    encoding="utf-8-sig",
    usecols=["Analista nombre completo", "ANALISTA", "equipo"]
).rename(columns={"Analista nombre completo": "NOMBRECOMPLETO", "equipo": "EQUIPO"})
df_equipos["ANALISTA"] = df_equipos["ANALISTA"].astype(str).str.strip().str.upper()
df_equipos["EQUIPO"] = df_equipos["EQUIPO"].astype(str).str.strip().str.upper()
df_equipos = df_equipos.drop_duplicates(subset=["ANALISTA"])

# Enriquecimiento
df_tp = (
    df.merge(df_clasificacion, left_on="ANALISTA", right_on="NOMBRE", how="left")
      .merge(df_equipos, on="ANALISTA", how="left")
)

# Conversión temporal: UTC → Lima → limpiar zona
created_lima = (
    pd.to_datetime(df_tp["CREATED"], utc=True, errors="coerce")
      .dt.tz_convert("America/Lima")
      .dt.floor("min")
)
df_tp["FECHA"] = created_lima.dt.date
df_tp["HORA"]  = created_lima.dt.time
df_tp["FECHAHORA"] = created_lima.dt.tz_localize(None)
