import pandas as pd

pd.set_option("display.max_columns", None)
pd.set_option('display.max_rows', None)
pd.set_option("display.width", None)

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

df_clasificacion = pd.read_csv(
    "INPUT/CLASIFICACION_ANALISTAS.csv",
    encoding="utf-8-sig",
    usecols=["NOMBRE", "EXPERTISE"]
)

df_equipos = pd.read_csv(
    "INPUT/EQUIPOS.csv",
    delimiter=";",
    encoding="utf-8-sig",
    usecols=["Analista nombre completo", "ANALISTA", "equipo"]
).rename(columns={"Analista nombre completo" : "NOMBRECOMPLETO", "equipo": "EQUIPO"})
df_equipos["ANALISTA"] = df_equipos["ANALISTA"].astype(str).str.strip().str.upper()
df_equipos["EQUIPO"] = df_equipos["EQUIPO"].astype(str).str.strip().str.upper()

df_tp = df.merge(df_clasificacion, left_on="ANALISTA", right_on="NOMBRE", how="left") \
          .merge(df_equipos, on="ANALISTA", how="left")

created = pd.to_datetime(df_tp["CREATED"], utc=True, errors="coerce").dt.tz_convert("America/Lima")
df_tp["FECHA"] = created.dt.date
df_tp["HORA"] = created.dt.time
df_tp["FECHAHORA"] = created.dt.floor("min")
