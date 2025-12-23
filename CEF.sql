# =========================
# CLASIFICACION TC / CEF
# =========================
is_tc  = F.col("PROCESO").like("%APROBACION CREDITOS TC%")
is_cef = F.col("PROCESO").isin(
   "CO SOLICITUD APROBACIONES TLMK",
   "SFCP APROBACIONES EDUCATIVO",
   "CO SOLICITUD APROBACIONES"
)
# Pasos relevantes por producto
paso_tc_analista   = (F.col("NBRPASO") == "APROBACION DE CREDITOS ANALISTA")
paso_tc_supervisor = (F.col("NBRPASO") == "APROBACION DE CREDITOS SUPERVISOR")
paso_tc_gerente    = (F.col("NBRPASO") == "APROBACION DE CREDITOS GERENTE")
paso_cef_analista  = (F.col("NBRPASO") == "EVALUACION DE SOLICITUD")
paso_cef_aprobador = (F.col("NBRPASO") == "EVALUACION DE SOLICITUD APROBADOR")
