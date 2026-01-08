from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select, WebDriverWait
from selenium.common.exceptions import TimeoutException
from pages.base_page import BasePage
from config.settings import URL_SUNAT

class SunatPage(BasePage):
    BTN_POR_DOCUMENTO = (By.ID, "btnPorDocumento")
    CMB_TIPO_DOC = (By.ID, "cmbTipoDoc")
    TXT_NUM_DOC = (By.ID, "txtNumeroDocumento")
    BTN_BUSCAR = (By.ID, "btnAceptar")

    # Resultado positivo (lista de contribuyentes)
    RESULT_ITEM = (By.CSS_SELECTOR, "a.aRucs.list-group-item, a.aRucs")

    # Panel genérico (para screenshot/validación)
    PANEL_RESULTADO = (By.CSS_SELECTOR, "div.panel.panel-primary")

    # ✅ Resultado negativo (no registra RUC)
    NO_RUC_STRONG = (By.XPATH, "//strong[contains(translate(., 'abcdefghijklmnopqrstuvwxyz', 'ABCDEFGHIJKLMNOPQRSTUVWXYZ'), 'NO REGISTRA')]")

    def open(self):
        self.driver.get(URL_SUNAT)
        self.wait.until(EC.presence_of_element_located(self.BTN_POR_DOCUMENTO))

    def buscar_por_dni(self, dni: str, timeout_result: int = 8) -> bool:
        """
        Retorna:
          True  -> encontró RUC y navegó (hizo click)
          False -> DNI sin RUC (no navega)
        """
        # 1) Por Documento
        self.wait.until(EC.element_to_be_clickable(self.BTN_POR_DOCUMENTO)).click()

        # 2) Tipo doc = DNI (value="1")
        sel = Select(self.wait.until(EC.presence_of_element_located(self.CMB_TIPO_DOC)))
        sel.select_by_value("1")

        # 3) Número documento
        inp = self.wait.until(EC.element_to_be_clickable(self.TXT_NUM_DOC))
        inp.click()
        inp.clear()
        inp.send_keys(dni)

        # 4) Buscar
        self.wait.until(EC.element_to_be_clickable(self.BTN_BUSCAR)).click()

        # 5) Esperar resultado: O hay RUC (link) O sale mensaje NO REGISTRA
        w = WebDriverWait(self.driver, timeout_result)

        try:
            # Primero intentamos detectar el caso SIN RUC (si aparece rápido, cortamos)
            w.until(EC.presence_of_element_located(self.NO_RUC_STRONG))

            # Si llegó aquí, es SIN RUC
            self.wait.until(EC.presence_of_element_located(self.PANEL_RESULTADO))
            return False

        except TimeoutException:
            # No apareció el "NO REGISTRA" -> asumimos que sí hay RUC
            first = w.until(EC.element_to_be_clickable(self.RESULT_ITEM))
            first.click()

            # 6) Esperar panel final
            self.wait.until(EC.presence_of_element_located(self.PANEL_RESULTADO))
            return True

    def screenshot_panel_resultado(self, out_path):
        panel = self.wait.until(EC.presence_of_element_located(self.PANEL_RESULTADO))
        self.driver.execute_script("arguments[0].scrollIntoView({block:'start'});", panel)
        panel.screenshot(str(out_path))














from pathlib import Path
from pages.sunat.sunat_page import SunatPage

class SunatFlow:
    def __init__(self, driver):
        self.page = SunatPage(driver)

    def run(self, dni: str, out_img_path: Path) -> dict:
        """
        Retorna dict con estado del flujo SUNAT.
        {
          "status": "OK" | "SIN_RUC",
          "dni": "...",
          "has_ruc": bool
        }
        """
        self.page.open()
        has_ruc = self.page.buscar_por_dni(dni)

        if not has_ruc:
            # Si quieres igual evidencia del mensaje "NO REGISTRA", puedes dejar screenshot aquí.
            # Si NO quieres screenshot, comenta esta línea.
            self.page.screenshot_panel_resultado(out_img_path)
            return {"status": "SIN_RUC", "dni": dni, "has_ruc": False}

        # Caso OK
        self.page.screenshot_panel_resultado(out_img_path)
        return {"status": "OK", "dni": dni, "has_ruc": True}













sunat_result = SunatFlow(driver).run(dni=dni_titular, out_img_path=sunat_img_path)
logging.info("SUNAT result=%s", sunat_result)









if sunat_result.get("status") == "OK":
    writer.add_image_to_range("SUNAT", sunat_img_path, "C5", "O51")
else:
    # Si prefieres dejar evidencia del "NO REGISTRA", entonces SÍ insertes la imagen igual.
    # Aquí lo dejo como NO insertar (porque pediste no avanzar como si fuera OK).
    logging.warning("SUNAT: DNI sin RUC, no se inserta panel en XLSM.")
