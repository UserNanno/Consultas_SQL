<body>
<b>La aplicación ha generado un error al validar el ingreso del usuario</b>
<p><font color="red">El código ingresado, no coincide con el código mostrado en la imagen.<br>Vuelva a intentarlo.</font></p>
<a href="/app/login.jsp">&lt;&lt; Regresar</a>

</body>








    

pages/sbs/cerrar_sesiones_page.py
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import TimeoutException

from pages.base_page import BasePage
from config.settings import URL_SBS_CERRAR_SESIONES


class CerrarSesionesPage(BasePage):
    # Inputs
    TXT_USUARIO = (By.ID, "Formulario:txtCodUsuario")
    TXT_CLAVE = (By.ID, "Formulario:txtClave")
    BTN_CONTINUAR = (By.ID, "Formulario:btnContinuar")

    # Keypad
    KEYPAD_DIV = (By.ID, "keypad-div")
    KEYPAD_CLEAR = (By.CSS_SELECTOR, "#keypad-div .keypad-key.keypad-clear")  # Limpiar

    # Resultado 1: cerró sesión
    H1_OK = (By.XPATH, "//h1[contains(.,'Ha cerrado la sesión activa satisfactoriamente')]")

    # Resultado 2: no había sesiones activas (esto también es OK)
    NO_SESIONES_MSG = (
        By.XPATH,
        "//span[contains(@class,'ui-messages-error-detail') and contains(.,'No existen sesiones activas')]"
    )

    def open(self):
        self.driver.get(URL_SBS_CERRAR_SESIONES)
        self.wait.until(EC.presence_of_element_located(self.TXT_USUARIO))

    def _open_keypad(self):
        self.wait.until(EC.element_to_be_clickable(self.TXT_CLAVE)).click()
        self.wait.until(EC.visibility_of_element_located(self.KEYPAD_DIV))

    def _clear(self):
        try:
            self.wait.until(EC.element_to_be_clickable(self.KEYPAD_CLEAR)).click()
        except Exception:
            pass

    def _press_digit(self, d: str):
        btn = (
            By.XPATH,
            f"//div[@id='keypad-div']//button[contains(@class,'keypad-key') and normalize-space(text())='{d}']"
        )
        self.wait.until(EC.element_to_be_clickable(btn)).click()

    def _wait_outcome(self, timeout: int = 12) -> str:
        """
        Espera cualquiera de los 2 outcomes y retorna:
          - "CERRADA" o "NO_ACTIVAS"
        """
        w = WebDriverWait(self.driver, timeout)

        # esperamos el primero que aparezca
        try:
            w.until(lambda d: (
                len(d.find_elements(*self.H1_OK)) > 0
                or len(d.find_elements(*self.NO_SESIONES_MSG)) > 0
            ))
        except TimeoutException:
            raise TimeoutException("No se detectó resultado de cierre de sesión (ni OK ni 'No existen sesiones activas').")

        if len(self.driver.find_elements(*self.H1_OK)) > 0:
            return "CERRADA"
        return "NO_ACTIVAS"

    def cerrar_sesion(self, usuario: str, clave: str) -> str:
        """
        Retorna:
          - "CERRADA"   -> cerró una sesión activa
          - "NO_ACTIVAS"-> no existían sesiones activas (esperado)
        """
        # usuario
        inp_user = self.wait.until(EC.element_to_be_clickable(self.TXT_USUARIO))
        inp_user.click()
        inp_user.clear()
        inp_user.send_keys(usuario)

        # clave por keypad (numérica)
        pwd = (clave or "").strip()
        if not pwd:
            raise ValueError("Clave SBS vacía.")
        if not pwd.isdigit():
            raise ValueError("La clave SBS para 'Cerrar Sesiones' debe ser numérica (keypad).")

        # keypad
        self._open_keypad()
        self._clear()
        for ch in pwd:
            self._press_digit(ch)

        # continuar
        self.wait.until(EC.element_to_be_clickable(self.BTN_CONTINUAR)).click()

        # outcome (ambos se consideran OK)
        return self._wait_outcome(timeout=12)




pages/sbs/login_page.py
import time

from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.keys import Keys

from pages.base_page import BasePage


class LoginPage(BasePage):

    def _reset_zoom(self):
        try:
            body = self.driver.find_element(By.TAG_NAME, "body")
            body.click()
            body.send_keys(Keys.CONTROL, "0")
        except Exception:
            pass

    def capture_image(self, img_path):
        self._reset_zoom()
        # Igual que tu monolito: ID correcto
        img_el = self.wait.until(EC.presence_of_element_located((By.ID, "CaptchaImgID")))
        img_el.screenshot(str(img_path))

    def fill_form(self, usuario, clave, captcha):
        # Igual que tu monolito: esperar el input y escribir captcha
        inp_test = self.wait.until(EC.presence_of_element_located((By.ID, "c_c_captcha")))
        inp_test.clear()
        inp_test.send_keys(captcha)

        inp_user = self.wait.until(EC.presence_of_element_located((By.ID, "c_c_usuario")))
        inp_user.clear()
        inp_user.send_keys(usuario)

        self.wait.until(EC.presence_of_element_located((By.ID, "ulKeypad")))

        # botón borrar (si existe)
        try:
            self.driver.find_element(By.CSS_SELECTOR, "#ulKeypad li.WEB_zonaIngresobtnBorrar").click()
        except Exception:
            pass

        for d in clave:
            tecla = self.wait.until(
                EC.element_to_be_clickable(
                    (By.XPATH, f"//ul[@id='ulKeypad']//li[normalize-space()='{d}']")
                )
            )
            tecla.click()
            time.sleep(0.2)

        btn = self.wait.until(EC.element_to_be_clickable((By.ID, "btnIngresar")))
        btn.click()









pages/sbs/riesgos_page.py

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

    # NUEVO: mensaje tipo "La persona no presenta saldos..."
    DIV_MENSAJE = (By.CSS_SELECTOR, "div.Mensaje")

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

    # OJO: este selector puede no existir en casos "sin saldos". Se usa solo en modo normal.
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

    def otros_reportes_habilitado(self) -> bool:
        """Otros Reportes habilitado si existe <a id='idOp6'> (no span)."""
        try:
            return len(self.driver.find_elements(*self.TAB_OTROS_REPORTES)) > 0
        except Exception:
            return False

    def mensaje_no_saldos(self) -> bool:
        """
        True si aparece el mensaje:
        'La persona no presenta saldos en la Posición Consolidada.'
        """
        try:
            el = WebDriverWait(self.driver, 1.5).until(
                EC.presence_of_element_located(self.DIV_MENSAJE)
            )
            txt = " ".join((el.text or "").split()).lower()
            return "no presenta saldos" in txt
        except TimeoutException:
            return False
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






services/sbs_flow.py
from pathlib import Path
import logging

from pages.sbs.cerrar_sesiones_page import CerrarSesionesPage
from pages.sbs.login_page import LoginPage
from pages.sbs.riesgos_page import RiesgosPage
from pages.copilot_page import CopilotPage
from services.copilot_service import CopilotService
from config.settings import URL_LOGIN


class SbsFlow:
    # Pre-step debe correr SOLO una vez por ejecución del proceso
    _prestep_done: bool = False

    def __init__(self, driver, usuario: str, clave: str):
        self.driver = driver
        self.usuario = usuario
        self.clave = clave

    def _pre_cerrar_sesion_activa(self):
        logging.info("[SBS] Pre-step: cerrar sesión activa (cerrarSesiones.jsf)")

        try:
            self.driver.delete_all_cookies()
        except Exception:
            pass

        page = CerrarSesionesPage(self.driver)
        page.open()
        outcome = page.cerrar_sesion(self.usuario, self.clave)

        if outcome == "CERRADA":
            logging.info("[SBS] Pre-step OK: sesión activa cerrada")
        else:
            logging.info("[SBS] Pre-step OK: no existían sesiones activas (continuar)")

    def run(
        self,
        dni: str,
        captcha_img_path: Path,
        detallada_img_path: Path,
        otros_img_path: Path,
    ) -> dict:
        # ==========================================================
        # 0) PRE-STEP: Cerrar sesión activa (SOLO 1 VEZ POR EJECUCIÓN)
        # ==========================================================
        if not SbsFlow._prestep_done:
            try:
                self._pre_cerrar_sesion_activa()
                SbsFlow._prestep_done = True
            except Exception as e:
                logging.warning("[SBS] Pre-step cerrar sesión falló, se continúa igual. Detalle=%r", e)
        else:
            logging.info("[SBS] Pre-step omitido (ya se ejecutó en esta corrida)")

        # ==========================================================
        # 1) Flujo SBS normal
        # ==========================================================
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

        # ==========================================================
        # 1.A0) CAMINO "SIN SALDOS" / "SOLO OTROS REPORTES"
        # - Detallada NO habilitada
        # - Histórica NO habilitada
        # - Consolidado muestra "no presenta saldos"
        # - Otros Reportes SÍ habilitado
        # -> Saltar el paso "Detallada screenshot" y ir directo a Otros Reportes
        # ==========================================================
        if (not riesgos.detallada_habilitada()) and (not riesgos.historica_habilitada()) and riesgos.otros_reportes_habilitado():
            if riesgos.mensaje_no_saldos():
                logging.warning(
                    "[SBS] Modo SIN_SALDOS/SOLO_OTROS: no hay Posición Consolidada; se omite Detallada y se va directo a Otros Reportes."
                )

                datos_deudor = {}
                try:
                    datos_deudor = riesgos.extract_datos_deudor()
                except Exception as e:
                    logging.warning("[SBS] No se pudo extraer Datos del Deudor (SIN_SALDOS): %r", e)

                # Evidencia del consolidado (con el mensaje). Reusamos detallada_img_path para no romper Excel.
                try:
                    riesgos.screenshot_contenido(str(detallada_img_path))
                except Exception:
                    try:
                        self.driver.save_screenshot(str(detallada_img_path))
                    except Exception:
                        pass

                # Ir a Otros Reportes y mantener lógica no bloqueante
                try:
                    riesgos.go_otros_reportes()

                    disponible = riesgos.otros_reportes_disponible()
                    logging.info("[SBS] Otros Reportes disponible=%s (SIN_SALDOS)", disponible)

                    loaded = riesgos.click_carteras_transferidas()
                    logging.info("[SBS] Carteras Transferidas loaded=%s (SIN_SALDOS)", loaded)

                    if loaded and riesgos.has_carteras_table():
                        riesgos.expand_all_rectificaciones(expected=2)

                    riesgos.screenshot_contenido(str(otros_img_path))

                except Exception as e:
                    logging.warning("[SBS] Otros Reportes falló (SIN_SALDOS): %r", e)
                    try:
                        riesgos.screenshot_contenido(str(otros_img_path))
                    except Exception:
                        pass

                # Logout y retorno parcial
                try:
                    logging.info("[SBS] Logout módulo")
                    riesgos.logout_modulo()
                    logging.info("[SBS] Logout portal")
                    riesgos.logout_portal()
                except Exception:
                    pass

                logging.info("[SBS] Fin flujo OK (modo SIN_SALDOS)")
                return {
                    "datos_deudor": datos_deudor,
                    "posicion": [],  # no existe posición consolidada en este caso
                    "modo": "SIN_SALDOS",
                }

        # ==========================================================
        # 1.A) CAMINO "HISTORICA" (Detallada NO habilitada)
        # ==========================================================
        if (not riesgos.detallada_habilitada()) and riesgos.historica_habilitada():
            logging.warning(
                "[SBS] Modo HISTORICA: Detallada no está habilitada. Se captura evidencia y se continúa sin Posición Consolidada."
            )

            # Asegurar que estamos en Histórica
            try:
                riesgos.go_historica()
            except Exception:
                pass

            # En Histórica sí existe "Datos del Deudor" -> lo extraemos si se puede
            datos_deudor = {}
            try:
                datos_deudor = riesgos.extract_datos_deudor()
            except Exception as e:
                logging.warning("[SBS] No se pudo extraer Datos del Deudor en Histórica: %r", e)

            # Evidencia principal (guardamos en detallada_img_path aunque sea Histórica)
            try:
                riesgos.screenshot_contenido(str(detallada_img_path))
            except Exception:
                try:
                    self.driver.save_screenshot(str(detallada_img_path))
                except Exception:
                    pass

            # Intentar Otros Reportes (puede estar habilitado)
            try:
                riesgos.go_otros_reportes()
                disponible = riesgos.otros_reportes_disponible()
                logging.info("[SBS] Otros Reportes disponible=%s (modo Histórica)", disponible)

                loaded = riesgos.click_carteras_transferidas()
                logging.info("[SBS] Carteras Transferidas loaded=%s (modo Histórica)", loaded)

                if loaded and riesgos.has_carteras_table():
                    riesgos.expand_all_rectificaciones(expected=2)

                riesgos.screenshot_contenido(str(otros_img_path))
            except Exception as e:
                logging.warning("[SBS] Otros Reportes falló en modo Histórica: %r", e)
                try:
                    riesgos.screenshot_contenido(str(otros_img_path))
                except Exception:
                    pass

            # Logout y retorno parcial
            try:
                logging.info("[SBS] Logout módulo")
                riesgos.logout_modulo()
                logging.info("[SBS] Logout portal")
                riesgos.logout_portal()
            except Exception:
                pass

            logging.info("[SBS] Fin flujo OK (modo HISTORICA)")
            return {
                "datos_deudor": datos_deudor,
                "posicion": [],  # no hay posición consolidada
                "modo": "HISTORICA",
            }

        # ==========================================================
        # 1.B) CAMINO NORMAL (Consolidado -> Detallada -> Otros Reportes)
        # ==========================================================
        logging.info("[SBS] Extraer datos (modo normal)")
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

        logging.info("[SBS] Fin flujo OK (modo normal)")
        return {
            "datos_deudor": datos_deudor,
            "posicion": posicion,
            "modo": "NORMAL",
        }





services/copilot_services.py
class CopilotService:

    def __init__(self, copilot_page):
        self.copilot_page = copilot_page

    def resolve_captcha(self, img_path):
        return self.copilot_page.ask_from_image(img_path)


main.py
from __future__ import annotations
from pathlib import Path
import os
import sys
import tempfile
import logging
from typing import Optional, Tuple

from config.settings import *
from config.credentials_store import load_sbs_credentials
from config.analyst_store import load_matanalista

from infrastructure.edge_debug import EdgeDebugLauncher
from infrastructure.selenium_driver import SeleniumDriverFactory
from services.sbs_flow import SbsFlow
from services.sunat_flow import SunatFlow
from services.rbm_flow import RbmFlow
from services.xlsm_session_writer import XlsmSessionWriter
from utils.logging_utils import setup_logging
from utils.decorators import log_exceptions


def _get_app_dir() -> Path:
    """Carpeta del exe (PyInstaller) o del proyecto (script)."""
    if getattr(sys, "frozen", False):
        return Path(sys.executable).resolve().parent
    return Path(__file__).resolve().parent


def _pick_results_dir(app_dir: Path) -> Path:
    """
    Intenta crear ./results al lado del exe/script.
    Si falla por permisos, usa %LOCALAPPDATA%/PrismaProject/results (o TEMP).
    """
    primary = app_dir / "results"
    try:
        primary.mkdir(parents=True, exist_ok=True)
        test_file = primary / ".write_test"
        test_file.write_text("ok", encoding="utf-8")
        test_file.unlink(missing_ok=True)
        return primary
    except Exception:
        base = Path(os.environ.get("LOCALAPPDATA", tempfile.gettempdir()))
        fallback = base / "PrismaProject" / "results"
        fallback.mkdir(parents=True, exist_ok=True)
        return fallback


def _safe_filename_part(s: str) -> str:
    """
    Limpia caracteres problemáticos para nombre de archivo (Windows).
    """
    s = (s or "").strip()
    invalid = '<>:"/\\|?*'
    for ch in invalid:
        s = s.replace(ch, "")
    # reduce espacios
    s = s.replace(" ", "")
    return s


@log_exceptions
def run_app(
    dni_titular: str,
    dni_conyuge: Optional[str] = None,
    numoportunidad: Optional[str] = None,
    producto: Optional[str] = None,
    desproducto: Optional[str] = None,
) -> Tuple[Path, Path]:
    app_dir = _get_app_dir()
    setup_logging(app_dir)

    logging.info("=== INICIO EJECUCION ===")
    logging.info("DNI_TITULAR=%s | DNI_CONYUGE=%s", dni_titular, dni_conyuge or "")
    logging.info(
        "NUMOPORTUNIDAD=%s | PRODUCTO=%s | DESPRODUCTO=%s",
        numoportunidad or "",
        producto or "",
        desproducto or "",
    )
    logging.info("APP_DIR=%s", app_dir)

    # Aunque UI/Controller validen, protegemos el motor
    if not (numoportunidad or "").strip():
        raise ValueError("NUMOPORTUNIDAD es obligatorio.")
    if not (producto or "").strip():
        raise ValueError("PRODUCTO es obligatorio.")
    if not (desproducto or "").strip():
        raise ValueError("DESPRODUCTO es obligatorio.")

    # ====== MATANALISTA persistente (obligatorio) ======
    matanalista = load_matanalista("").strip()
    if not matanalista:
        raise ValueError("No hay MATANALISTA configurado. Ve al botón 'MATANALISTA' y guárdalo.")
    logging.info("MATANALISTA runtime=%s", matanalista)

    # ====== CREDENCIALES SBS DESDE GUI (LOCALAPPDATA) ======
    sbs_user, sbs_pass = load_sbs_credentials("", "")
    if not sbs_user or not sbs_pass:
        raise ValueError("No hay credenciales SBS configuradas. Ve a 'Credenciales SBS' y guárdalas.")
    logging.info("SBS user runtime=%s", sbs_user)

    launcher = EdgeDebugLauncher()
    launcher.ensure_running()
    driver = SeleniumDriverFactory.create()

    try:
        macro_path = app_dir / "Macro.xlsm"
        if not macro_path.exists():
            raise FileNotFoundError(f"No se encontró Macro.xlsm junto al ejecutable: {macro_path}")

        results_dir = _pick_results_dir(app_dir)

        dni_conyuge = (dni_conyuge or "").strip() or None

        safe_numop = _safe_filename_part(numoportunidad)
        safe_mat = _safe_filename_part(matanalista)
        out_xlsm = results_dir / f"{safe_numop}_{safe_mat}.xlsm"

        captcha_img_path = results_dir / "captura.png"
        detallada_img_path = results_dir / "detallada.png"
        otros_img_path = results_dir / "otros_reportes.png"

        captcha_img_cony_path = results_dir / "sbs_captura_conyuge.png"
        detallada_img_cony_path = results_dir / "sbs_detallada_conyuge.png"
        otros_img_cony_path = results_dir / "sbs_otros_reportes_conyuge.png"

        sunat_img_path = results_dir / "sunat_panel.png"

        rbm_consumos_img_path = results_dir / "rbm_consumos.png"
        rbm_cem_img_path = results_dir / "rbm_cem.png"

        rbm_consumos_cony_path = results_dir / "rbm_consumos_conyuge.png"
        rbm_cem_cony_path = results_dir / "rbm_cem_conyuge.png"

        logging.info("RESULTS_DIR=%s", results_dir)
        logging.info("OUTPUT_XLSM=%s", out_xlsm)

        # ==========================================================
        # 1) SBS TITULAR
        # ==========================================================
        logging.info("== FLUJO SBS (TITULAR) INICIO ==")
        _ = SbsFlow(driver, sbs_user, sbs_pass).run(
            dni=dni_titular,
            captcha_img_path=captcha_img_path,
            detallada_img_path=detallada_img_path,
            otros_img_path=otros_img_path,
        )
        logging.info("== FLUJO SBS (TITULAR) FIN ==")

        # ==========================================================
        # 1.1) SBS CONYUGE (opcional)
        # ==========================================================
        if dni_conyuge:
            logging.info("== FLUJO SBS (CONYUGE) INICIO ==")
            _ = SbsFlow(driver, sbs_user, sbs_pass).run(
                dni=dni_conyuge,
                captcha_img_path=captcha_img_cony_path,
                detallada_img_path=detallada_img_cony_path,
                otros_img_path=otros_img_cony_path,
            )
            logging.info("== FLUJO SBS (CONYUGE) FIN ==")

        # ==========================================================
        # 2) SUNAT TITULAR
        # ==========================================================
        logging.info("== FLUJO SUNAT (TITULAR) INICIO ==")
        try:
            driver.delete_all_cookies()
        except Exception:
            pass
        SunatFlow(driver).run(dni=dni_titular, out_img_path=sunat_img_path)
        logging.info("== FLUJO SUNAT (TITULAR) FIN ==")

        # ==========================================================
        # 3) RBM TITULAR
        # ==========================================================
        logging.info("== FLUJO RBM (TITULAR) INICIO ==")
        rbm_titular = RbmFlow(driver).run(
            dni=dni_titular,
            consumos_img_path=rbm_consumos_img_path,
            cem_img_path=rbm_cem_img_path,
            numoportunidad=numoportunidad,
            producto=producto,
            desproducto=desproducto,
        )
        rbm_inicio_tit = rbm_titular.get("inicio", {}) if isinstance(rbm_titular, dict) else {}
        rbm_cem_tit = rbm_titular.get("cem", {}) if isinstance(rbm_titular, dict) else {}
        rbm_scores_tit = rbm_titular.get("scores", {}) if isinstance(rbm_titular, dict) else {}
        logging.info("RBM titular inicio=%s", rbm_inicio_tit)
        logging.info("RBM titular cem=%s", rbm_cem_tit)
        logging.info("RBM titular scores=%s", rbm_scores_tit)
        logging.info("== FLUJO RBM (TITULAR) FIN ==")

        # ==========================================================
        # 4) RBM CONYUGE (opcional, en nueva pestaña)
        # ==========================================================
        rbm_inicio_cony = {}
        rbm_cem_cony = {}
        if dni_conyuge:
            logging.info("== FLUJO RBM (CONYUGE) INICIO ==")
            original_handle_rbm = driver.current_window_handle
            rbm_conyuge = {}
            try:
                driver.switch_to.new_window("tab")
                rbm_conyuge = RbmFlow(driver).run(
                    dni=dni_conyuge,
                    consumos_img_path=rbm_consumos_cony_path,
                    cem_img_path=rbm_cem_cony_path,
                    numoportunidad=numoportunidad,
                    producto=producto,
                    desproducto=desproducto,
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

        # ==========================================================
        # 5) Escribir todo en XLSM
        #    (AHORA SÍ escribimos Inicio!C4 + scores C14/C83 según producto)
        # ==========================================================
        logging.info("== ESCRITURA XLSM INICIO ==")

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

        # Mapeo GUI PRODUCTO -> texto exacto en Inicio!C4
        producto_excel_c4 = None
        p = (producto or "").strip().upper()
        if p == "CREDITO EFECTIVO":
            producto_excel_c4 = "Credito Efectivo"
        elif p == "TARJETA DE CREDITO":
            producto_excel_c4 = "Tarjeta de Credito"

        with XlsmSessionWriter(macro_path) as writer:
            # NUEVO: C4 "selección" en plantilla
            if producto_excel_c4:
                writer.write_cell("Inicio", "C4", producto_excel_c4)

            # Campos Inicio existentes
            writer.write_cell("Inicio", "C11", rbm_inicio_tit.get("segmento"))
            writer.write_cell("Inicio", "C12", rbm_inicio_tit.get("segmento_riesgo"))
            writer.write_cell("Inicio", "C13", rbm_inicio_tit.get("pdh"))
            writer.write_cell("Inicio", "C15", rbm_inicio_tit.get("score_rcc"))

            # NUEVO: scores RBM a Inicio!C14 y C83
            if rbm_scores_tit.get("inicio_c14") is not None:
                writer.write_cell("Inicio", "C14", rbm_scores_tit.get("inicio_c14"))
            if rbm_scores_tit.get("inicio_c83") is not None:
                writer.write_cell("Inicio", "C83", rbm_scores_tit.get("inicio_c83"))

            # CEM titular
            for key, row in cem_row_map:
                item = rbm_cem_tit.get(key, {}) or {}
                writer.write_cell("Inicio", f"C{row}", item.get("cuota_bcp", 0))
                writer.write_cell("Inicio", f"D{row}", item.get("cuota_sbs", 0))
                writer.write_cell("Inicio", f"E{row}", item.get("saldo_sbs", 0))

            # Conyuge (si aplica)
            if dni_conyuge:
                writer.write_cell("Inicio", "D11", rbm_inicio_cony.get("segmento"))
                writer.write_cell("Inicio", "D12", rbm_inicio_cony.get("segmento_riesgo"))
                writer.write_cell("Inicio", "D15", rbm_inicio_cony.get("score_rcc"))

                for key, row in cem_row_map:
                    item = rbm_cem_cony.get(key, {}) or {}
                    writer.write_cell("Inicio", f"G{row}", item.get("cuota_bcp", 0))
                    writer.write_cell("Inicio", f"H{row}", item.get("cuota_sbs", 0))
                    writer.write_cell("Inicio", f"I{row}", item.get("saldo_sbs", 0))

            # Imágenes
            writer.add_image_to_range("SBS", detallada_img_path, "C64", "Z110")
            writer.add_image_to_range("SBS", otros_img_path, "C5", "Z50")
            if dni_conyuge:
                writer.add_image_to_range("SBS", detallada_img_cony_path, "AI64", "AY110")
                writer.add_image_to_range("SBS", otros_img_cony_path, "AI5", "AY50")

            writer.add_image_to_range("SUNAT", sunat_img_path, "C5", "O51")

            writer.add_image_to_range("RBM", rbm_consumos_img_path, "C5", "Z50")
            writer.add_image_to_range("RBM", rbm_cem_img_path, "C64", "Z106")
            if dni_conyuge:
                writer.add_image_to_range("RBM", rbm_consumos_cony_path, "AI5", "AY50")
                writer.add_image_to_range("RBM", rbm_cem_cony_path, "AI64", "AY106")

            writer.save(out_xlsm)

        logging.info("== ESCRITURA XLSM FIN ==")
        logging.info("XLSM final generado: %s", out_xlsm.resolve())
        logging.info("Evidencias guardadas en: %s", results_dir.resolve())
        print(f"XLSM final generado: {out_xlsm.resolve()}")
        print(f"Evidencias guardadas en: {results_dir.resolve()}")

        return out_xlsm, results_dir

    finally:
        try:
            driver.quit()
        except Exception:
            pass
        try:
            launcher.close()
        except Exception:
            pass

        logging.info("=== FIN EJECUCION ===")
        logging.shutdown()


@log_exceptions
def main():
    run_app(DNI_CONSULTA, DNI_CONYUGE_CONSULTA)


if __name__ == "__main__":
    main()
