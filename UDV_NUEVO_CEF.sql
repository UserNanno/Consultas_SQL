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

    # Caso con RUC (lista)
    RESULT_ITEM = (By.CSS_SELECTOR, "a.aRucs.list-group-item, a.aRucs")

    # Panel genérico (sirve tanto en OK como SIN_RUC)
    PANEL_RESULTADO = (By.CSS_SELECTOR, "div.panel.panel-primary")

    # Caso SIN RUC
    NO_RUC_STRONG = (
        By.XPATH,
        "//strong[contains(translate(., 'abcdefghijklmnopqrstuvwxyz', 'ABCDEFGHIJKLMNOPQRSTUVWXYZ'), 'NO REGISTRA')]"
    )

    # Form que usa la página cuando hay RUC
    FORM_SELEC = (By.NAME, "selecXNroRuc")

    # Body (para screenshot completo)
    BODY = (By.TAG_NAME, "body")

    def open(self):
        self.driver.get(URL_SUNAT)
        self.wait.until(EC.presence_of_element_located(self.BTN_POR_DOCUMENTO))

    def _submit_ruc_form(self, ruc: str):
        """
        Replica:
          document.selecXNroRuc.nroRuc.value = ruc
          document.selecXNroRuc.submit()
        """
        self.wait.until(EC.presence_of_element_located(self.FORM_SELEC))
        js = """
        const ruc = arguments[0];
        if (!document.selecXNroRuc) return "NO_FORM";
        document.selecXNroRuc.nroRuc.value = ruc;
        document.selecXNroRuc.submit();
        return "OK";
        """
        return self.driver.execute_script(js, ruc)

    def _wait_result_outcome(self, timeout: int = 10) -> str:
        """
        Espera el primero que ocurra (race):
          - aparece RESULT_ITEM -> "HAS_RUC"
          - aparece NO_RUC_STRONG -> "NO_RUC"
        """
        w = WebDriverWait(self.driver, timeout)
        try:
            w.until(lambda d: (
                len(d.find_elements(*self.RESULT_ITEM)) > 0
                or len(d.find_elements(*self.NO_RUC_STRONG)) > 0
            ))
        except TimeoutException:
            raise TimeoutException("SUNAT: no apareció ni lista de RUCs ni mensaje 'NO REGISTRA'.")

        if len(self.driver.find_elements(*self.NO_RUC_STRONG)) > 0:
            return "NO_RUC"
        return "HAS_RUC"

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

        # 5) Esperar outcome inmediato (sin esperar 10s por NO_RUC cuando sí hay RUC)
        outcome = self._wait_result_outcome(timeout=timeout_result)

        if outcome == "NO_RUC":
            self.wait.until(EC.presence_of_element_located(self.PANEL_RESULTADO))
            return {"status": "SIN_RUC", "dni": dni, "ruc": None}

        # 6) Caso con RUC: tomar primer item + enviar form (más estable que click)
        w = WebDriverWait(self.driver, timeout_result)
        first = w.until(EC.presence_of_element_located(self.RESULT_ITEM))
        ruc = first.get_attribute("data-ruc") or None

        # Guardamos el form actual para esperar navegación real (más confiable que staleness del <a>)
        form_el = self.wait.until(EC.presence_of_element_located(self.FORM_SELEC))

        if ruc:
            self._submit_ruc_form(ruc)
        else:
            # fallback extremo: click JS
            self.driver.execute_script("arguments[0].scrollIntoView({block:'center'});", first)
            self.driver.execute_script("arguments[0].click();", first)

        # 7) Esperar que la página cambie (submit). Aquí lo robusto es:
        #    - el form selecXNroRuc desaparece o queda stale
        try:
            w.until(EC.staleness_of(form_el))
        except TimeoutException:
            # fallback: si no queda stale, al menos que ya no exista el form
            w.until(lambda d: len(d.find_elements(*self.FORM_SELEC)) == 0)

        # 8) Panel final (siempre hay panel en las vistas)
        self.wait.until(EC.presence_of_element_located(self.PANEL_RESULTADO))

        return {"status": "OK", "dni": dni, "ruc": ruc}

    def screenshot_panel_resultado(self, out_path):
        panel = self.wait.until(EC.presence_of_element_located(self.PANEL_RESULTADO))
        self.driver.execute_script("arguments[0].scrollIntoView({block:'start'});", panel)
        panel.screenshot(str(out_path))

    def screenshot_body(self, out_path):
        body = self.wait.until(EC.presence_of_element_located(self.BODY))
        self.driver.execute_script("arguments[0].scrollIntoView({block:'start'});", body)
        body.screenshot(str(out_path))
