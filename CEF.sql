from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select, WebDriverWait
from selenium.common.exceptions import TimeoutException, NoSuchElementException
import logging

from pages.base_page import BasePage


class RiesgosPage(BasePage):
    MENU = (By.ID, "Menu")

    # Tabs (cuando están habilitados son <a id="...">)
    TAB_HISTORICA = (By.ID, "idOp2")
    TAB_DETALLADA = (By.ID, "idOp4")
    TAB_OTROS_REPORTES = (By.ID, "idOp6")

    # Versión "deshabilitada" (cuando sale como <span>Detallada</span>)
    SPAN_DETALLADA = (By.XPATH, "//div[@id='Menu']//span[normalize-space()='Detallada']")
    SPAN_HISTORICA = (By.XPATH, "//div[@id='Menu']//span[normalize-space()='Historica' or normalize-space()='Histórica']")
    SPAN_OTROS_REPORTES = (By.XPATH, "//div[@id='Menu']//span[normalize-space()='Otros Reportes']")

    CONTENIDO = (By.ID, "Contenido")

    # Otros Reportes
    OTROS_LIST = (By.ID, "OtrosReportes")
    LNK_CARTERAS_TRANSFERIDAS = (
        By.XPATH,
        "//ul[@id='OtrosReportes']//a[normalize-space()='Carteras Transferidas']"
    )

    # Tabla contenedora "Otros Reportes"
    TBL_OTROS_REPORTES = (
        By.XPATH,
        "//table[contains(@class,'Crw')][.//span[normalize-space()='Otros Reportes']]"
    )

    # Carteras
    TBL_CARTERAS = (By.CSS_SELECTOR, "table#expand.Crw")
    ARROWS = (By.CSS_SELECTOR, "table#expand div.arrow[title*='Rectificaciones']")

    LINK_MODULO = (
        By.CSS_SELECTOR,
        "a.descripcion[onclick*=\"/criesgos/criesgos/criesgos.jsp\"]"
    )

    SELECT_TIPO_DOC = (By.ID, "as_tipo_doc")
    INPUT_DOC = (By.CSS_SELECTOR, "input[name='as_doc_iden']")
    BTN_CONSULTAR = (By.ID, "btnConsultar")

    TBL_DATOS_DEUDOR = (
        By.XPATH,
        "//table[contains(@class,'Crw')][.//b[contains(@class,'F') and contains(normalize-space(.),'Datos del Deudor')]]"
    )
    TBL_POSICION = (
        By.XPATH,
        "//table[contains(@class,'Crw')][.//span[contains(@class,'F') and contains(normalize-space(.),'Posición Consolidada del Deudor')]]"
    )

    LINK_SALIR_CRIESGOS = (By.CSS_SELECTOR, "a[href*='/criesgos/logout?c_c_producto=00002']")
    BTN_SALIR_PORTAL = (By.CSS_SELECTOR, "a[onclick*=\"goTo('logout')\"]")

    # ---------------- navegación básica ----------------

    def open_modulo_deuda(self):
        link = self.wait.until(EC.element_to_be_clickable(self.LINK_MODULO))
        link.click()

    def consultar_por_dni(self, dni: str):
        sel = self.wait.until(EC.presence_of_element_located(self.SELECT_TIPO_DOC))
        Select(sel).select_by_value("11")

        inp = self.wait.until(EC.element_to_be_clickable(self.INPUT_DOC))
        inp.click()
        inp.clear()
        inp.send_keys(dni)

        btn = self.wait.until(EC.element_to_be_clickable(self.BTN_CONSULTAR))
        btn.click()

        # ✅ si SBS muestra un JS alert, aceptarlo y continuar
        alert_txt = self.accept_alert_if_present(timeout=2.5)
        if alert_txt:
            logging.warning("[SBS] Alert al consultar DNI=%s: %s", dni, alert_txt)

    # ---------------- detección de modo ----------------

    def detallada_habilitada(self) -> bool:
        """
        'Detallada' está habilitada si existe <a id="idOp4">.
        Si aparece como <span>Detallada</span>, está deshabilitada.
        """
        try:
            return len(self.driver.find_elements(*self.TAB_DETALLADA)) > 0
        except Exception:
            return False

    def historica_habilitada(self) -> bool:
        """Histórica habilitada si existe <a id="idOp2">."""
        try:
            return len(self.driver.find_elements(*self.TAB_HISTORICA)) > 0
        except Exception:
            return False

    # ---------------- tabs ----------------

    def go_historica(self):
        self.wait.until(EC.presence_of_element_located(self.MENU))
        self.wait.until(EC.element_to_be_clickable(self.TAB_HISTORICA)).click()
        self._wait_tab_loaded()
        WebDriverWait(self.driver, 6).until(EC.presence_of_element_located(self.CONTENIDO))

    def go_detallada(self):
        self.wait.until(EC.presence_of_element_located(self.MENU))
        self.wait.until(EC.element_to_be_clickable(self.TAB_DETALLADA)).click()
        self._wait_tab_loaded()

    def go_otros_reportes(self):
        self.wait.until(EC.presence_of_element_located(self.MENU))
        self.wait.until(EC.element_to_be_clickable(self.TAB_OTROS_REPORTES)).click()

        # Espera corta: el contenido/tablas aparecen rápido
        short_wait = WebDriverWait(self.driver, 6)
        short_wait.until(EC.presence_of_element_located(self.CONTENIDO))
        short_wait.until(EC.presence_of_element_located(self.TBL_OTROS_REPORTES))

    # ===================== CLAVE: no demorar si no hay info =====================

    def otros_reportes_disponible(self) -> bool:
        """
        Determina si existen opciones en "Otros Reportes" SIN usar waits largos.
        - Si el texto contiene 'No existe información...' => False inmediato
        - Si existe UL#OtrosReportes => True
        """
        short_wait = WebDriverWait(self.driver, 3)

        try:
            tbl = short_wait.until(EC.presence_of_element_located(self.TBL_OTROS_REPORTES))
        except TimeoutException:
            return False

        txt = " ".join((tbl.text or "").split()).lower()
        if "no existe información en otros reportes" in txt:
            return False

        return len(tbl.find_elements(By.ID, "OtrosReportes")) > 0

    def click_carteras_transferidas(self) -> bool:
        """
        No bloqueante y SIN waits largos.
        - Si no hay info => return False inmediato
        - Si hay lista, intenta click con wait corto y valida carga con wait corto
        """
        short_wait = WebDriverWait(self.driver, 3)

        try:
            tbl = short_wait.until(EC.presence_of_element_located(self.TBL_OTROS_REPORTES))
        except TimeoutException:
            return False

        txt = " ".join((tbl.text or "").split()).lower()
        if "no existe información en otros reportes" in txt:
            return False

        ul_list = tbl.find_elements(By.ID, "OtrosReportes")
        if not ul_list:
            return False

        ul = ul_list[0]
        links = ul.find_elements(By.XPATH, ".//a[normalize-space()='Carteras Transferidas']")
        if not links:
            return False

        try:
            links[0].click()
        except Exception:
            self.driver.execute_script("arguments[0].click();", links[0])

        short_wait2 = WebDriverWait(self.driver, 5)

        def _loaded(d):
            try:
                if d.find_elements(*self.TBL_CARTERAS):
                    return True
            except Exception:
                pass
            try:
                return "buscarinfocarterastransferidas" in (d.current_url or "")
            except Exception:
                return False

        try:
            short_wait2.until(_loaded)
            short_wait2.until(EC.presence_of_element_located(self.CONTENIDO))
            return True
        except TimeoutException:
            return False

    def has_carteras_table(self) -> bool:
        try:
            self.driver.find_element(*self.TBL_CARTERAS)
            return True
        except NoSuchElementException:
            return False

    def expand_all_rectificaciones(self, expected: int = 2):
        arrows = self.driver.find_elements(*self.ARROWS)
        if not arrows:
            return

        for arrow in arrows[:expected]:
            try:
                self.driver.execute_script("arguments[0].scrollIntoView({block:'center'});", arrow)
                try:
                    arrow.click()
                except Exception:
                    self.driver.execute_script("arguments[0].click();", arrow)

                def _expanded(d):
                    try:
                        master_tr = arrow.find_element(By.XPATH, "./ancestor::tr[contains(@class,'master')]")
                        detail_tr = master_tr.find_element(
                            By.XPATH, "following-sibling::tr[contains(@class,'Vde')][1]"
                        )
                        style = (detail_tr.get_attribute("style") or "").lower()
                        return "display: none" not in style
                    except Exception:
                        return True

                try:
                    WebDriverWait(self.driver, 3).until(_expanded)
                except Exception:
                    pass
            except Exception:
                continue

    def screenshot_contenido(self, out_path):
        short_wait = WebDriverWait(self.driver, 4)
        try:
            el = short_wait.until(EC.presence_of_element_located(self.CONTENIDO))
            self.driver.execute_script("arguments[0].scrollIntoView({block:'start'});", el)
            try:
                el.screenshot(str(out_path))
            except Exception:
                self.driver.save_screenshot(str(out_path))
        except TimeoutException:
            self.driver.save_screenshot(str(out_path))

    def _wait_tab_loaded(self):
        self.wait.until(lambda d: d.execute_script("return document.readyState") == "complete")

    # ---------------- extracción ----------------

    def extract_datos_deudor(self) -> dict:
        alert_txt = self.accept_alert_if_present(timeout=0.6)
        if alert_txt:
            logging.warning("[SBS] Alert previo a extraer Datos del Deudor: %s", alert_txt)

        tbl = self.wait.until(EC.presence_of_element_located(self.TBL_DATOS_DEUDOR))
        rows = tbl.find_elements(By.CSS_SELECTOR, "tbody tr")
        data = {}
        for r in rows:
            tds = r.find_elements(By.CSS_SELECTOR, "td")
            if len(tds) < 2:
                continue
            i = 0
            while i < len(tds) - 1:
                label_el = None
                try:
                    label_el = tds[i].find_element(By.CSS_SELECTOR, "b.Dz")
                except Exception:
                    pass
                label = (label_el.text.strip() if label_el else tds[i].text.strip())
                value_text = tds[i + 1].text.strip()
                if label:
                    label = " ".join(label.split())
                    value_text = " ".join(value_text.split())
                    data[label] = value_text
                i += 2
        return data

    def extract_posicion_consolidada(self) -> list:
        alert_txt = self.accept_alert_if_present(timeout=0.6)
        if alert_txt:
            logging.warning("[SBS] Alert previo a extraer Posición Consolidada: %s", alert_txt)

        tbl = self.wait.until(EC.presence_of_element_located(self.TBL_POSICION))
        trs = tbl.find_elements(By.CSS_SELECTOR, "tbody tr")
        out = []
        for tr in trs:
            tds = tr.find_elements(By.CSS_SELECTOR, "td")
            if len(tds) != 4:
                continue
            row = [" ".join(td.text.split()) for td in tds]
            if row[0].upper() == "SALDOS":
                continue
            out.append(row)
        return out

    # ---------------- logout ----------------

    def logout_modulo(self):
        link = self.wait.until(EC.element_to_be_clickable(self.LINK_SALIR_CRIESGOS))
        link.click()
        self.wait.until(EC.presence_of_element_located(self.BTN_SALIR_PORTAL))

    def logout_portal(self):
        btn = self.wait.until(EC.element_to_be_clickable(self.BTN_SALIR_PORTAL))
        btn.click()
