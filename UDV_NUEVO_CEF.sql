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

    # Caso con RUC
    RESULT_ITEM = (By.CSS_SELECTOR, "a.aRucs.list-group-item, a.aRucs")

    # Panel genérico
    PANEL_RESULTADO = (By.CSS_SELECTOR, "div.panel.panel-primary")

    # Caso SIN RUC (texto)
    NO_RUC_STRONG = (
        By.XPATH,
        "//strong[contains(translate(., 'abcdefghijklmnopqrstuvwxyz', 'ABCDEFGHIJKLMNOPQRSTUVWXYZ'), 'NO REGISTRA')]"
    )

    # Para screenshot full page visible (body)
    BODY = (By.TAG_NAME, "body")

    def open(self):
        self.driver.get(URL_SUNAT)
        self.wait.until(EC.presence_of_element_located(self.BTN_POR_DOCUMENTO))

    def buscar_por_dni(self, dni: str, timeout_result: int = 8) -> dict:
        """
        Retorna dict:
          {
            "status": "OK" | "SIN_RUC",
            "dni": dni
          }
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

        # 5) Esperar resultado: SIN_RUC (strong NO REGISTRA) o link aRucs
        w = WebDriverWait(self.driver, timeout_result)

        # Intentamos SIN_RUC primero (suele aparecer rápido)
        try:
            w.until(EC.presence_of_element_located(self.NO_RUC_STRONG))
            # Asegura que el panel ya esté
            self.wait.until(EC.presence_of_element_located(self.PANEL_RESULTADO))
            return {"status": "SIN_RUC", "dni": dni}
        except TimeoutException:
            # No apareció NO REGISTRA -> flujo normal con RUC
            first = w.until(EC.element_to_be_clickable(self.RESULT_ITEM))
            first.click()
            self.wait.until(EC.presence_of_element_located(self.PANEL_RESULTADO))
            return {"status": "OK", "dni": dni}

    def screenshot_panel_resultado(self, out_path):
        panel = self.wait.until(EC.presence_of_element_located(self.PANEL_RESULTADO))
        self.driver.execute_script("arguments[0].scrollIntoView({block:'start'});", panel)
        panel.screenshot(str(out_path))

    def screenshot_body(self, out_path):
        """
        Captura el <body> visible completo (lo que entra en el viewport).
        Para full-page real (altura total), lo ideal es CDP.
        """
        body = self.wait.until(EC.presence_of_element_located(self.BODY))
        self.driver.execute_script("arguments[0].scrollIntoView({block:'start'});", body)
        body.screenshot(str(out_path))













from pathlib import Path
from pages.sunat.sunat_page import SunatPage

class SunatFlow:
    def __init__(self, driver):
        self.page = SunatPage(driver)

    def run(self, dni: str, out_img_path: Path) -> dict:
        """
        Retorna dict:
          {"status": "OK"|"SIN_RUC", "dni": "..."}
        """
        self.page.open()
        result = self.page.buscar_por_dni(dni)

        if result["status"] == "SIN_RUC":
            # ✅ Evidencia completa (body)
            self.page.screenshot_body(out_img_path)
        else:
            # ✅ Evidencia del panel final normal
            self.page.screenshot_panel_resultado(out_img_path)

        return result










sunat_result = SunatFlow(driver).run(dni=dni_titular, out_img_path=sunat_img_path)
logging.info("SUNAT result=%s", sunat_result)
