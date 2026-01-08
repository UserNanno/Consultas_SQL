from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select, WebDriverWait
from selenium.common.exceptions import TimeoutException, ElementClickInterceptedException, StaleElementReferenceException
# ... (resto igual)

class SunatPage(BasePage):
    # ... tus locators igual ...
    RESULT_ITEM = (By.CSS_SELECTOR, "a.aRucs.list-group-item, a.aRucs")
    NO_RUC_STRONG = (By.XPATH, "//strong[contains(translate(., 'abcdefghijklmnopqrstuvwxyz', 'ABCDEFGHIJKLMNOPQRSTUVWXYZ'), 'NO REGISTRA')]")
    PANEL_RESULTADO = (By.CSS_SELECTOR, "div.panel.panel-primary")

    def buscar_por_dni(self, dni: str, timeout_result: int = 8) -> dict:
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

        w = WebDriverWait(self.driver, timeout_result)

        # Caso SIN RUC (rápido)
        try:
            w.until(EC.presence_of_element_located(self.NO_RUC_STRONG))
            self.wait.until(EC.presence_of_element_located(self.PANEL_RESULTADO))
            return {"status": "SIN_RUC", "dni": dni}
        except TimeoutException:
            pass  # seguimos al caso con RUC

        # Caso CON RUC: NO esperes element_to_be_clickable (demora). Usa presencia + click rápido.
        first = w.until(EC.presence_of_element_located(self.RESULT_ITEM))

        # Scroll para evitar intercepts por header/botoneras
        try:
            self.driver.execute_script("arguments[0].scrollIntoView({block:'center'});", first)
        except Exception:
            pass

        # Intento de click rápido con fallback JS
        clicked = False
        for _ in range(3):
            try:
                first.click()
                clicked = True
                break
            except (ElementClickInterceptedException, StaleElementReferenceException):
                try:
                    first = self.driver.find_element(*self.RESULT_ITEM)
                    self.driver.execute_script("arguments[0].scrollIntoView({block:'center'});", first)
                    self.driver.execute_script("arguments[0].click();", first)  # JS click
                    clicked = True
                    break
                except Exception:
                    continue

        if not clicked:
            # último recurso: JS click directo
            self.driver.execute_script("arguments[0].click();", first)

        # 6) Esperar panel final (ya estabas esperando este)
        self.wait.until(EC.presence_of_element_located(self.PANEL_RESULTADO))
        return {"status": "OK", "dni": dni}
