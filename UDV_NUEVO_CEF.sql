pages/sunat/sunat_page.py
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


    RESULT_ITEM = (By.CSS_SELECTOR, "a.aRucs.list-group-item, a.aRucs")

    PANEL_RESULTADO = (By.CSS_SELECTOR, "div.panel.panel-primary")


    NO_RUC_STRONG = (
        By.XPATH,
        "//strong[contains(translate(., 'abcdefghijklmnopqrstuvwxyz', 'ABCDEFGHIJKLMNOPQRSTUVWXYZ'), 'NO REGISTRA')]"
    )


    FORM_SELEC = (By.NAME, "selecXNroRuc")

    def open(self):
        self.driver.get(URL_SUNAT)
        self.wait.until(EC.presence_of_element_located(self.BTN_POR_DOCUMENTO))

    def _submit_ruc_form(self, ruc: str):
        """
        Hace lo mismo que el JS de la página:
          document.selecXNroRuc.nroRuc.value = ruc;
          document.selecXNroRuc.submit();
        Mucho más estable que click en <a>.
        """
        # asegura que el form exista
        self.wait.until(EC.presence_of_element_located(self.FORM_SELEC))

        js = """
        const ruc = arguments[0];
        if (!document.selecXNroRuc) { return "NO_FORM"; }
        document.selecXNroRuc.nroRuc.value = ruc;
        document.selecXNroRuc.submit();
        return "OK";
        """
        return self.driver.execute_script(js, ruc)

    def buscar_por_dni(self, dni: str, timeout_result: int = 10) -> dict:
        """
        Retorna:
          {"status": "OK"|"SIN_RUC", "dni": dni, "ruc": "..."/None}
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

        # 5) Esperar outcome: SIN_RUC o lista con RUC
        w = WebDriverWait(self.driver, timeout_result)
        try:
            w.until(EC.presence_of_element_located(self.NO_RUC_STRONG))
            self.wait.until(EC.presence_of_element_located(self.PANEL_RESULTADO))
            return {"status": "SIN_RUC", "dni": dni, "ruc": None}
        except TimeoutException:
            # Caso OK: obtener primer RUC
            first = w.until(EC.presence_of_element_located(self.RESULT_ITEM))

            # a veces está, pero click no funciona; tomamos el data-ruc y enviamos form
            ruc = first.get_attribute("data-ruc")
            if not ruc:
                # fallback: intenta click normal si no hay data-ruc por algún motivo
                self.driver.execute_script("arguments[0].scrollIntoView({block:'center'});", first)
                self.driver.execute_script("arguments[0].click();", first)
            else:
                self._submit_ruc_form(ruc)

            # 6) Esperar que cambie a la página de detalle del RUC.
            # En la práctica, aquí cambia el contenido y/o URL. Esperamos que desaparezca la lista
            # o que cambie el panel a uno distinto (si tienes un selector mejor del detalle, úsalo).
            w.until(EC.staleness_of(first))

            # y esperamos el panel final
            self.wait.until(EC.presence_of_element_located(self.PANEL_RESULTADO))
            return {"status": "OK", "dni": dni, "ruc": ruc}

    def screenshot_panel_resultado(self, out_path):
        panel = self.wait.until(EC.presence_of_element_located(self.PANEL_RESULTADO))
        self.driver.execute_script("arguments[0].scrollIntoView({block:'start'});", panel)
        panel.screenshot(str(out_path))

    def screenshot_body(self, out_path):
        body = self.wait.until(EC.presence_of_element_located((By.TAG_NAME, "body")))
        self.driver.execute_script("arguments[0].scrollIntoView({block:'start'});", body)
        body.screenshot(str(out_path))







services/sunat_flow.py
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
            self.page.screenshot_body(out_img_path)
        else:
            self.page.screenshot_panel_resultado(out_img_path)

        return result
