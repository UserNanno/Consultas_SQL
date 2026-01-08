# pages/sbs/riesgos_page.py
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select, WebDriverWait
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from pages.base_page import BasePage


class RiesgosPage(BasePage):
    """
    Página post-login donde existe el link al módulo (criesgos),
    y luego la pantalla del formulario + resultados (tablas).
    """

    MENU = (By.ID, "Menu")

    TAB_DETALLADA = (By.ID, "idOp4")
    TAB_OTROS_REPORTES = (By.ID, "idOp6")

    # Contenedor de contenido (para screenshot de solo tablas)
    CONTENIDO = (By.ID, "Contenido")

    # Bloque "Otros Reportes" (tabla simple con lista)
    OTROS_LIST = (By.ID, "OtrosReportes")
    LNK_CARTERAS_TRANSFERIDAS = (
        By.XPATH,
        "//ul[@id='OtrosReportes']//a[normalize-space()='Carteras Transferidas']"
    )

    # NUEVO: tabla "Otros Reportes" y texto cuando no hay info
    TBL_OTROS_REPORTES = (
        By.XPATH,
        "//table[contains(@class,'Crw')][.//span[normalize-space()='Otros Reportes']]"
    )
    TXT_NO_INFO_OTROS = (
        By.XPATH,
        "//table[contains(@class,'Crw')][.//span[normalize-space()='Otros Reportes']]"
        "//td[contains(normalize-space(.),'No existe información en Otros Reportes')]"
    )

    # Tabla adicional (solo aparece en algunos DNI, pero en la vista de Carteras Transferidas)
    TBL_CARTERAS = (By.CSS_SELECTOR, "table#expand.Crw")

    # Flechas expand (dentro de la tabla #expand)
    ARROWS = (By.CSS_SELECTOR, "table#expand div.arrow[title*='Rectificaciones']")

    LINK_MODULO = (
        By.CSS_SELECTOR,
        "a.descripcion[onclick*=\"/criesgos/criesgos/criesgos.jsp\"]"
    )

    SELECT_TIPO_DOC = (By.ID, "as_tipo_doc")
    INPUT_DOC = (By.CSS_SELECTOR, "input[name='as_doc_iden']")
    BTN_CONSULTAR = (By.ID, "btnConsultar")

    # Tablas de resultado (por su header visible)
    TBL_DATOS_DEUDOR = (
        By.XPATH,
        "//table[contains(@class,'Crw')][.//b[contains(@class,'F') and contains(normalize-space(.),'Datos del Deudor')]]"
    )
    TBL_POSICION = (
        By.XPATH,
        "//table[contains(@class,'Crw')][.//span[contains(@class,'F') and contains(normalize-space(.),'Posición Consolidada del Deudor')]]"
    )

    # LOGOUT (2 PASOS) ===
    LINK_SALIR_CRIESGOS = (By.CSS_SELECTOR, "a[href*='/criesgos/logout?c_c_producto=00002']")
    BTN_SALIR_PORTAL = (By.CSS_SELECTOR, "a[onclick*=\"goTo('logout')\"]")

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

    def go_detallada(self):
        self.wait.until(EC.presence_of_element_located(self.MENU))
        self.wait.until(EC.element_to_be_clickable(self.TAB_DETALLADA)).click()
        self._wait_tab_loaded()

    def go_otros_reportes(self):
        self.wait.until(EC.presence_of_element_located(self.MENU))
        self.wait.until(EC.element_to_be_clickable(self.TAB_OTROS_REPORTES)).click()
        self.wait.until(EC.presence_of_element_located(self.CONTENIDO))

    # ===================== NUEVO =====================
    def otros_reportes_disponible(self) -> bool:
        """
        True si existe la lista <ul id='OtrosReportes'> (hay opciones).
        False si aparece el texto 'No existe información en Otros Reportes'
        o si no aparece nada concluyente (fallback).
        """
        short_wait = WebDriverWait(self.driver, 3)

        # Asegura que la sección de Otros Reportes ya está en DOM
        try:
            short_wait.until(EC.presence_of_element_located(self.TBL_OTROS_REPORTES))
        except TimeoutException:
            return False

        # Caso explícito: no hay información
        if self.driver.find_elements(*self.TXT_NO_INFO_OTROS):
            return False

        # Caso normal: hay lista con opciones
        if self.driver.find_elements(*self.OTROS_LIST):
            return True

        return False

    def click_carteras_transferidas(self) -> bool:
        """
        No bloqueante:
        - si no hay info en Otros Reportes => return False inmediato
        - si hay lista, intenta click y valida carga (wait corto)
        """
        if not self.otros_reportes_disponible():
            return False

        # aquí sí debería existir el UL
        try:
            self.wait.until(EC.element_to_be_clickable(self.LNK_CARTERAS_TRANSFERIDAS)).click()
        except TimeoutException:
            return False

        short_wait = WebDriverWait(self.driver, 5)

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
            short_wait.until(_loaded)
            short_wait.until(EC.presence_of_element_located(self.CONTENIDO))
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
        """
        No bloqueante: si no hay flechas, return inmediato.
        """
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
                        detail_tr = master_tr.find_element(By.XPATH, "following-sibling::tr[contains(@class,'Vde')][1]")
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
        """
        FIX REAL:
        - Antes: self.wait (30s) + element.screenshot (a veces pesado/lento/stale)
        - Ahora:
          1) intentamos localizar #Contenido con wait corto (4s)
          2) si no se puede (o está re-renderizando), hacemos fallback a screenshot completo
          => evita “pausas” largas antes del logout.
        """
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

    def extract_datos_deudor(self) -> dict:
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

    def logout_modulo(self):
        link = self.wait.until(EC.element_to_be_clickable(self.LINK_SALIR_CRIESGOS))
        link.click()
        self.wait.until(EC.presence_of_element_located(self.BTN_SALIR_PORTAL))

    def logout_portal(self):
        btn = self.wait.until(EC.element_to_be_clickable(self.BTN_SALIR_PORTAL))
        btn.click()











# services/sbs_flow.py
from pathlib import Path
import logging

from pages.sbs.login_page import LoginPage
from pages.sbs.riesgos_page import RiesgosPage
from pages.copilot_page import CopilotPage
from services.copilot_service import CopilotService
from config.settings import URL_LOGIN


class SbsFlow:
    def __init__(self, driver, usuario: str, clave: str):
        self.driver = driver
        self.usuario = usuario
        self.clave = clave

    def run(
        self,
        dni: str,
        captcha_img_path: Path,
        detallada_img_path: Path,
        otros_img_path: Path,
    ) -> dict:
        logging.info("[SBS] Ir a login")
        self.driver.get(URL_LOGIN)

        login_page = LoginPage(self.driver)

        logging.info("[SBS] Capturar captcha")
        login_page.capture_image(captcha_img_path)

        # --- Copilot en nueva pestaña (y luego cerrarla) ---
        original_handle = self.driver.current_window_handle
        self.driver.switch_to.new_window("tab")
        copilot = CopilotService(CopilotPage(self.driver))

        logging.info("[SBS] Resolver captcha con Copilot")
        captcha = copilot.resolve_captcha(captcha_img_path)

        try:
            self.driver.close()
        except Exception:
            pass
        self.driver.switch_to.window(original_handle)

        logging.info("[SBS] Login (usuario=%s) + ingresar captcha", self.usuario)
        login_page.fill_form(self.usuario, self.clave, captcha)

        riesgos = RiesgosPage(self.driver)

        logging.info("[SBS] Abrir módulo deuda")
        riesgos.open_modulo_deuda()

        logging.info("[SBS] Consultar DNI=%s", dni)
        riesgos.consultar_por_dni(dni)

        logging.info("[SBS] Extraer datos")
        datos_deudor = riesgos.extract_datos_deudor()
        posicion = riesgos.extract_posicion_consolidada()

        logging.info("[SBS] Ir a Detallada + screenshot")
        riesgos.go_detallada()
        self.driver.save_screenshot(str(detallada_img_path))

        logging.info("[SBS] Ir a Otros Reportes")
        riesgos.go_otros_reportes()

        logging.info("[SBS] Intentar Carteras Transferidas (no bloqueante)")
        try:
            disponible = riesgos.otros_reportes_disponible()
            logging.info("[SBS] Otros Reportes disponible=%s", disponible)

            loaded = riesgos.click_carteras_transferidas()
            logging.info("[SBS] Carteras Transferidas loaded=%s", loaded)

            if loaded and riesgos.has_carteras_table():
                logging.info("[SBS] Expandir rectificaciones (si hay)")
                riesgos.expand_all_rectificaciones(expected=2)

            logging.info("[SBS] Screenshot contenido (rápido + fallback)")
            riesgos.screenshot_contenido(str(otros_img_path))

        except Exception as e:
            logging.exception("[SBS] Error en Otros Reportes/Carteras: %r", e)
            riesgos.screenshot_contenido(str(otros_img_path))

        logging.info("[SBS] Logout módulo")
        riesgos.logout_modulo()

        logging.info("[SBS] Logout portal")
        riesgos.logout_portal()

        logging.info("[SBS] Fin flujo OK")
        return {
            "datos_deudor": datos_deudor,
            "posicion": posicion,
        }
