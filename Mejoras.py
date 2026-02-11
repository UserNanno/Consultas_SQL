# CLASIF SIN TOLERANCIA
def clasificar_estado(row):
    res = row["RESULTADOANALISTA"]
    tipo = row["TIPOPRODUCTO"]
    flg = row["FLGPENDIENTE"]
    hora = row["FECHAHORA"].time() if pd.notnull(row["FECHAHORA"]) else None

    if res in ["APROBADO POR ANALISTA DE CREDITO", "DENEGADO POR ANALISTA DE CREDITO"]:
        return "ATENCIONES"

    if res in ["DEVUELTO"]:
        return "DEVUELTO"

    if res == "FACA":
        return "FACA"

    if res == "DEVOLVER AL GESTOR" and tipo == "CRÉDITO VEHICULAR":
        return "ATENCIONES"

    if res in ["PENDIENTE", "ENVIADO A ANALISTA DE CRÉDITOS"] or flg in ["SI", "1"]:
        if hora:
            if time(8, 0) <= hora <= time(19, 30):
                return "PENDIENTE (HORA HABIL)"
            else:
                return "PENDIENTE (HORA NO HABIL)"
        else:
            return "PENDIENTE"


    # Resto
    return "EN PROCESO"

df_final_validado_logica["ESTADO_OPORTUNIDAD"] = df_final_validado_logica.apply(clasificar_estado, axis=1)
