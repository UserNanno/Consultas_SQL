# ================================
# Consolidación lógica por oportunidad (último estado)
# ================================
df_final_validado_logica = df_final_validado.copy()

# Asegurar FECHAHORA como datetime y quedarnos con el último registro por OPORTUNIDAD
df_final_validado_logica["FECHAHORA"] = pd.to_datetime(df_final_validado_logica["FECHAHORA"], errors="coerce")
df_final_validado_logica = (
    df_final_validado_logica
      .sort_values(["OPORTUNIDAD", "FECHAHORA"])
      .groupby("OPORTUNIDAD", as_index=False)
      .tail(1)
)

# Normalización a mayúsculas para garantizar consistencia
df_final_validado_logica["RESULTADOANALISTA"] = df_final_validado_logica["RESULTADOANALISTA"].astype(str).str.upper().str.strip()
df_final_validado_logica["TIPOPRODUCTO"] = df_final_validado_logica["TIPOPRODUCTO"].astype(str).str.upper().str.strip()
df_final_validado_logica["FLGPENDIENTE"] = df_final_validado_logica["FLGPENDIENTE"].astype(str).str.upper().str.strip()

# Clasificación estricta sin tolerancia
def clasificar_estado(row):
    res = row["RESULTADOANALISTA"]
    tipo = row["TIPOPRODUCTO"]
    flg = row["FLGPENDIENTE"]

    # Aprobado / Denegado → RESUELTA
    if res in ["APROBADO POR ANALISTA DE CRÉDITO", "DENEGADO POR ANALISTA DE CRÉDITO"]:
        return "RESUELTA"

    # FACA
    if res == "FACA":
        return "FACA"

    # Devolver al gestor (solo si es Crédito Vehicular)
    if res == "DEVOLVER AL GESTOR" and tipo == "CRÉDITO VEHICULAR":
        return "DEVUELTO AL GESTOR"

    # Pendiente → incluye Enviado a Analista de Créditos o FLG = SI
    if res in ["PENDIENTE", "ENVIADO A ANALISTA DE CRÉDITOS"] or flg in ["SI", "1"]:
        return "PENDIENTE"

    # Resto → en proceso
    return "EN PROCESO"

df_final_validado_logica["ESTADO_OPORTUNIDAD"] = df_final_validado_logica.apply(clasificar_estado, axis=1)
