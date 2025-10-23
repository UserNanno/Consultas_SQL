# 1) Base filtrada
df_pendientes_tcstock_base = df_pendientes_tcstock.loc[
    df_pendientes_tcstock["ESTADO"] == "Pendiente",
    ["OPORTUNIDAD", "DESTIPACCION", "ESTADO", "ANALISTA_MATCH", "FECINICIOPASO"]
].copy()

# 2) Partes de fecha/hora
df_pendientes_tcstock_base["FECHA"] = df_pendientes_tcstock_base["FECINICIOPASO"].astype(str).str[:10]
df_pendientes_tcstock_base["HORA"] = df_pendientes_tcstock_base["FECINICIOPASO"].astype(str).str[11:]
df_pendientes_tcstock_base["FECHAHORA"] = df_pendientes_tcstock_base["FECINICIOPASO"].astype(str).str[:16]

# 3) Enriquecimientos: trae EQUIPO y EXPERTISE sin duplicar columnas clave
df_pendientes_tcstock_base = (
    df_pendientes_tcstock_base
      .merge(df_equipos[["ANALISTA", "EQUIPO"]],
             left_on="ANALISTA_MATCH", right_on="ANALISTA", how="left")
      .drop(columns=["ANALISTA"])  # evita duplicar ANALISTA (luego renombramos ANALISTA_MATCH)
      .merge(df_clasificacion[["NOMBRE", "EXPERTISE"]],
             left_on="ANALISTA_MATCH", right_on="NOMBRE", how="left")
      .drop(columns=["NOMBRE"])    # limpieza
)

# 4) Seleccionar columnas y luego renombrar (más claro y seguro)
df_pendientes_tcstock_final = df_pendientes_tcstock_base[
    ["OPORTUNIDAD", "DESTIPACCION", "ESTADO",
     "ANALISTA_MATCH", "FECHA", "HORA", "EXPERTISE", "FECHAHORA", "EQUIPO"]
].copy()

df_pendientes_tcstock_final.rename(columns={
    "DESTIPACCION": "TIPOPRODUCTO",
    "ESTADO": "RESULTADOANALISTA",
    "ANALISTA_MATCH": "ANALISTA"
}, inplace=True)

# 5) Filtro de producto
df_pendientes_tcstock_final = df_pendientes_tcstock_final[
    df_pendientes_tcstock_final["TIPOPRODUCTO"].isin(
        ["TC", "UPGRADE", "AMPLIACION", "ADICIONAL", "BT", "VENTA COMBO TC"]
    )
].copy()

# 6) Flag y limpieza final
df_pendientes_tcstock_final["FLGPENDIENTE"] = 1
df_pendientes_tcstock_final = df_pendientes_tcstock_final.drop_duplicates()
