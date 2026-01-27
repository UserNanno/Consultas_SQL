def limpiar_cesado(col):
    c = col.cast("string")
    # borra "(CESADO)" o "CESADO" como token
    c = F.regexp_replace(c, r"(?i)\(?\s*CESADO\s*\)?", "")
    c = F.regexp_replace(c, r"\s+", " ")
    return F.trim(c)
