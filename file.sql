# ==============================
# Post-proceso final antes del export (VALIDADO)
# Reglas de prioridad (de mayor a menor):
# 1) APROBADO (en RESULTADOANALISTA o ESTADOAPROBACION) → todo el grupo (OPORTUNIDAD, FECHA) = "NO"
# 2) Si no hay aprobado, pero hay FACA → último del grupo = "FACA"; resto = "NO"
# 3) Si no hay aprobado ni FACA:
#    - multi-analista → "REASIGNADO"
#    - pendiente (flag/texto) → "SI"
#    - en otro caso → "NO"
# Además: si dentro de la misma OPORTUNIDAD existe un APROBADO posterior,
#          cualquier "ENVIADO A ANALISTA" anterior de esa OPORTUNIDAD se fuerza a "NO".
# ==============================
# 1) FECHAHORA = FECHA + " " + HORA (sin UTC)
if "FECHAHORA" in df_final_validado.columns:
   df_final_validado = df_final_validado.drop(columns=["FECHAHORA"])
df_final_validado["FECHAHORA"] = (
   df_final_validado["FECHA"].astype(str).str.strip()
   + " "
   + df_final_validado["HORA"].astype(str).str.strip()
).str.strip()
# 2) Preparar señales base (sin normalizar ANALISTA)
#    Trabajamos sobre copias en mayúsculas para matching textual
res_up = df_final_validado["RESULTADOANALISTA"].astype(str).str.upper().str.strip()
est_up = df_final_validado["ESTADOAPROBACION"].astype(str).str.upper().str.strip() if "ESTADOAPROBACION" in df_final_validado.columns else pd.Series("", index=df_final_validado.index)
mask_text_pend = (
   res_up.str.contains("PENDIENTE", na=False)
   | res_up.str.contains(r"ENVIADO\s+A\s+ANALISTA", na=False)
   | res_up.str.contains(r"ENVIADO\s+A\s+GERENTE", na=False)
)
mask_flag_pend = (
   (df_final_validado["FLGPENDIENTE"] == 1)
   | df_final_validado["FLGPENDIENTE"].astype(str).str.upper().eq("SI")
)
# Señales a nivel de grupo (OPORTUNIDAD, FECHA)
mask_group_has_faca = (
   df_final_validado
   .assign(_FACA_=res_up.str.contains(r"\bFACA\b", na=False))
   .groupby(["OPORTUNIDAD", "FECHA"])["_FACA_"]
   .transform("any")
)
# APROBADO (en cualquiera de los campos, tolerando "APROBADO/APROBADA")
mask_group_has_aprob = (
   df_final_validado
   .assign(_APR_=(res_up.str.contains(r"\bAPROBAD[OA]\b", na=False) | est_up.str.contains(r"\bAPROBAD[OA]\b", na=False)))
   .groupby(["OPORTUNIDAD", "FECHA"])["_APR_"]
   .transform("any")
)
mask_group_multi_analyst = (
   df_final_validado
   .groupby(["OPORTUNIDAD", "FECHA"])["ANALISTA"]
   .transform(lambda s: s.nunique() > 1)
)
# 3) Índice del último registro por grupo (para el caso FACA)
df_final_validado["_RID_"] = np.arange(len(df_final_validado))
_ordered = df_final_validado.sort_values(["OPORTUNIDAD", "FECHA", "FECHAHORA", "_RID_"])
idx_last_per_group = _ordered.groupby(["OPORTUNIDAD", "FECHA"]).tail(1).index
mask_is_group_last = df_final_validado.index.isin(idx_last_per_group)
# 4) Asignación con prioridades
# Base
df_final_validado["FLGPENDIENTE"] = "NO"
# 4.1) Grupos SIN aprobado NI FACA → aplicar SI / REASIGNADO
no_aprob_ni_faca = (~mask_group_has_aprob) & (~mask_group_has_faca)
df_final_validado.loc[no_aprob_ni_faca & (mask_flag_pend | mask_text_pend), "FLGPENDIENTE"] = "SI"
df_final_validado.loc[no_aprob_ni_faca & mask_group_multi_analyst, "FLGPENDIENTE"] = "REASIGNADO"
# 4.2) Grupos CON FACA y SIN aprobado → último = FACA; resto = NO
solo_faca = (~mask_group_has_aprob) & (mask_group_has_faca)
df_final_validado.loc[solo_faca & mask_is_group_last, "FLGPENDIENTE"] = "FACA"
# 4.3) Grupos CON APROBADO → todo el grupo = "NO" (domina sobre todo lo anterior)
df_final_validado.loc[mask_group_has_aprob, "FLGPENDIENTE"] = "NO"
# 5) Corrección adicional a nivel OPORTUNIDAD (no solo por FECHA):
#    Si existe un APROBADO posterior en la misma OPORTUNIDAD,
#    cualquier "ENVIADO A ANALISTA" anterior de esa OPORTUNIDAD debe ser "NO".
aprob_row_mask = res_up.str.contains(r"\bAPROBAD[OA]\b", na=False) | est_up.str.contains(r"\bAPROBAD[OA]\b", na=False)
last_aprob_por_opp = (
   df_final_validado.loc[aprob_row_mask, ["OPORTUNIDAD", "FECHAHORA"]]
   .groupby("OPORTUNIDAD", as_index=False)["FECHAHORA"].max()
   .rename(columns={"FECHAHORA": "_FECHAHORA_APROBADO_"})
)
# Merge para conocer la última hora de aprobado por OPORTUNIDAD
df_final_validado = df_final_validado.merge(last_aprob_por_opp, on="OPORTUNIDAD", how="left")
enviado_mask = res_up.str.contains(r"ENVIADO\s+A\s+ANALISTA", na=False)
tiene_aprob_en_opp = df_final_validado["_FECHAHORA_APROBADO_"].notna()
es_antes_que_aprob = df_final_validado["FECHAHORA"] < df_final_validado["_FECHAHORA_APROBADO_"]
# Forzar NO en enviados anteriores a un aprobado posterior (misma OPORTUNIDAD)
df_final_validado.loc[enviado_mask & tiene_aprob_en_opp & es_antes_que_aprob, "FLGPENDIENTE"] = "NO"
# Limpieza
df_final_validado.drop(columns=["_RID_", "_FECHAHORA_APROBADO_"], inplace=True, errors="ignore")
