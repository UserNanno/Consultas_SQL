Pero por qué lo de conyuge no es igual a titular?
por ejemplo aca son diferentes

 # ==========================================================
       # 3) RBM TITULAR (Consumos + CEM) + extracción de datos
       # ==========================================================
       logging.info("== FLUJO RBM (TITULAR) INICIO ==")
       rbm_titular = RbmFlow(driver).run(
           dni=dni_titular,
           consumos_img_path=rbm_consumos_img_path,
           cem_img_path=rbm_cem_img_path,
       )
       rbm_inicio_tit = rbm_titular.get("inicio", {}) if isinstance(rbm_titular, dict) else {}
       rbm_cem_tit = rbm_titular.get("cem", {}) if isinstance(rbm_titular, dict) else {}
       logging.info("RBM titular inicio=%s", rbm_inicio_tit)
       logging.info("RBM titular cem=%s", rbm_cem_tit)
       logging.info("== FLUJO RBM (TITULAR) FIN ==")
       # ==========================================================
       # 4) RBM CONYUGE en pestaña nueva (NO SUNAT)
       # ==========================================================
       logging.info("== FLUJO RBM (CONYUGE) INICIO ==")
       original_handle_rbm = driver.current_window_handle
       rbm_conyuge = {}
       try:
           driver.switch_to.new_window("tab")
           rbm_conyuge = RbmFlow(driver).run(
               dni=dni_conyuge,
               consumos_img_path=rbm_consumos_cony_path,
               cem_img_path=rbm_cem_cony_path,
           )
       finally:
           try:
               driver.close()
           except Exception:
               pass
           try:
               driver.switch_to.window(original_handle_rbm)
           except Exception:
               pass
       rbm_inicio_cony = rbm_conyuge.get("inicio", {}) if isinstance(rbm_conyuge, dict) else {}
       rbm_cem_cony = rbm_conyuge.get("cem", {}) if isinstance(rbm_conyuge, dict) else {}
       logging.info("RBM conyuge inicio=%s", rbm_inicio_cony)
       logging.info("RBM conyuge cem=%s", rbm_cem_cony)
       logging.info("== FLUJO RBM (CONYUGE) FIN ==")





también aca:

logging.info("== ESCRITURA XLSM INICIO ==")
       with XlsmSessionWriter(macro_path) as writer:
           # ------------------ RBM -> Hoja Inicio (titular) ------------------
           try:
               writer.write_cell("Inicio", "C11", rbm_inicio_tit.get("segmento"))
               writer.write_cell("Inicio", "C12", rbm_inicio_tit.get("segmento_riesgo"))
               writer.write_cell("Inicio", "C13", rbm_inicio_tit.get("pdh"))  # Si/No
               writer.write_cell("Inicio", "C15", rbm_inicio_tit.get("score_rcc"))
           except Exception as e:
               logging.exception("No se pudieron escribir campos RBM titular en hoja Inicio: %r", e)
           # ------------------ CEM titular -> Hoja Inicio ------------------
           cem_row_map = [
               ("hipotecario", 26),
               ("cef", 27),
               ("vehicular", 28),
               ("pyme", 29),
               ("comercial", 30),
               ("deuda_indirecta", 31),
               ("tarjeta", 32),
               ("linea_no_utilizada", 33),
           ]
           try:
               for key, row in cem_row_map:
                   item = rbm_cem_tit.get(key, {}) or {}
                   writer.write_cell("Inicio", f"C{row}", item.get("cuota_bcp", 0))
                   writer.write_cell("Inicio", f"D{row}", item.get("cuota_sbs", 0))
                   writer.write_cell("Inicio", f"E{row}", item.get("saldo_sbs", 0))
           except Exception as e:
               logging.exception("No se pudo escribir tabla CEM titular en hoja Inicio: %r", e)
           # ------------------ RBM cónyuge -> Hoja RBM (celdas pedidas) ------------------
           try:
               writer.write_cell("RBM", "D11", rbm_inicio_cony.get("segmento"))
               writer.write_cell("RBM", "D12", rbm_inicio_cony.get("segmento_riesgo"))
               writer.write_cell("RBM", "D15", rbm_inicio_cony.get("score_rcc"))
           except Exception as e:
               logging.exception("No se pudieron escribir campos RBM cónyuge en hoja RBM: %r", e)
           # ------------------ CEM cónyuge -> Hoja RBM desde G26/H26/I26 ------------------
           try:
               for key, row in cem_row_map:
                   item = rbm_cem_cony.get(key, {}) or {}
                   writer.write_cell("RBM", f"G{row}", item.get("cuota_bcp", 0))
                   writer.write_cell("RBM", f"H{row}", item.get("cuota_sbs", 0))
                   writer.write_cell("RBM", f"I{row}", item.get("saldo_sbs", 0))
           except Exception as e:
               logging.exception("No se pudo escribir tabla CEM cónyuge en hoja RBM: %r", e)








Esta bien esto? 
