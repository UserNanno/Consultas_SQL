# Nueva columna para marcar FACA
df_diario["FLGFACA"] = df_diario["RESULTADOANALISTA"].astype(str).str.upper().str.contains("FACA", na=False)
df_diario["FLGFACA"] = df_diario["FLGFACA"].map({True: "SI", False: "NO"})
