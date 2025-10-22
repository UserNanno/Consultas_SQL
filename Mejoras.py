df_unido["FECHA_ASIGNACION"] = pd.to_datetime(df_unido["FECHA_ASIGNACION"], errors="coerce")
df_unido["codmes"] = df_unido["FECHA_ASIGNACION"].dt.strftime("%Y%m")
conteo_codmes = (
    df_unido["codmes"]
    .value_counts()
    .reset_index()
    .rename(columns={"index": "codmes", "codmes": "conteo"})
    .sort_values("codmes")
)
