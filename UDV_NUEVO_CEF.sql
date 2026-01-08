Perfecto ya me sale perfecto. Tengo un par de detalles nomas que agregar.

Por ejemplo ahorita el usuario con el que entro a SBS es unico y está definido en mi config
USUARIO = "T10595"
CLAVE = "44445555"  # solo números

Como puedo hacer para que haya un boton donde le abra otra ventana donde le saca por ejemplo un label de SBS y al lado dos input con su label arriba de usuario y contraseña y otro boton al lado de estos input que le permita editar estos campos y se tomen estos para ingresar a SBS en el login. Se podrá?

También tengo un tema que despues que se ejecuta la busqueda del conyuge en SBS antes de hacer logout la pagina se queda ahi como detenida por cierto tiempo para que pase al flujo de sunat? Con esta pagina nomas pasa esto.


pages/sbs/riesgos_page.py
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select
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
    LNK_CARTERAS_TRANSFERIDAS = (By.XPATH, "//ul[@id='OtrosReportes']//a[normalize-space()='Carteras Transferidas']")

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
    # 1) Sale del módulo riesgos
    LINK_SALIR_CRIESGOS = (By.CSS_SELECTOR, "a[href*='/criesgos/logout?c_c_producto=00002']")

    # 2) Sale del portal
    BTN_SALIR_PORTAL = (By.CSS_SELECTOR, "a[onclick*=\"goTo('logout')\"]")

    def open_modulo_deuda(self):
        # Click al link del módulo
        link = self.wait.until(EC.element_to_be_clickable(self.LINK_MODULO))
        link.click()

    def consultar_por_dni(self, dni: str):
        # Seleccionar tipo doc = DNI (value=11)
        sel = self.wait.until(EC.presence_of_element_located(self.SELECT_TIPO_DOC))
        Select(sel).select_by_value("11")

        # Escribir DNI
        inp = self.wait.until(EC.element_to_be_clickable(self.INPUT_DOC))
        inp.click()
        inp.clear()
        inp.send_keys(dni)

        # Click Consultar
        btn = self.wait.until(EC.element_to_be_clickable(self.BTN_CONSULTAR))
        btn.click()

    def go_detallada(self):
       self.wait.until(EC.presence_of_element_located(self.MENU))
       self.wait.until(EC.element_to_be_clickable(self.TAB_DETALLADA)).click()
       self._wait_tab_loaded()

    def go_otros_reportes(self):
       self.wait.until(EC.presence_of_element_located(self.MENU))
       self.wait.until(EC.element_to_be_clickable(self.TAB_OTROS_REPORTES)).click()
       # Espera a que cargue el contenido base
       self.wait.until(EC.presence_of_element_located(self.CONTENIDO))

    def click_carteras_transferidas(self):
        """
        Desde 'Otros Reportes' (buscaotrosreportes...), entra a 'Carteras Transferidas'
        (buscarinfocarterastransferidas...).
        """
        self.wait.until(EC.presence_of_element_located(self.OTROS_LIST))
        self.wait.until(EC.element_to_be_clickable(self.LNK_CARTERAS_TRANSFERIDAS)).click()
        # Espera la tabla #expand o al menos que cambie URL al endpoint esperado (si aplica)
        # No hacemos hard-fail si la URL no cambia por alguna razón; lo importante es el DOM.
        def _loaded(d):
            try:
                # Si existe #expand ya estamos en la vista nueva
                d.find_element(*self.TBL_CARTERAS)
                return True
            except Exception:
                # fallback: URL contiene buscarinfocarterastransferidas
                return "buscarinfocarterastransferidas" in (d.current_url or "")
        self.wait.until(_loaded)
        # Asegurar que el contenido exista
        self.wait.until(EC.presence_of_element_located(self.CONTENIDO))
    def has_carteras_table(self) -> bool:
        """
        Verifica si existe table#expand (Información de Carteras Transferidas) en la vista actual.
        """
        try:
            self.driver.find_element(*self.TBL_CARTERAS)
            return True
        except NoSuchElementException:
            return False
    def expand_all_rectificaciones(self, expected: int = 2):
        """
        Hace click en las flechas 'arrow' dentro de #expand.
        Por defecto intentamos expandir 2 (como mencionaste).
        """
        # Esperar que existan flechas (si no hay, simplemente no hace nada)
        try:
            self.wait.until(lambda d: len(d.find_elements(*self.ARROWS)) > 0)
        except TimeoutException:
            return
        arrows = self.driver.find_elements(*self.ARROWS)
        # click a las primeras `expected` flechas
        for i, arrow in enumerate(arrows[:expected]):
            try:
                self.driver.execute_script("arguments[0].scrollIntoView({block:'center'});", arrow)
                try:
                    arrow.click()
                except Exception:
                    self.driver.execute_script("arguments[0].click();", arrow)
                # Esperar que se muestre la fila detalle siguiente (tr.Vde) asociada:
                # la estructura suele ser: tr.master + tr.Vde (display:none -> display:table-row)
                def _expanded(d):
                    try:
                        master_tr = arrow.find_element(By.XPATH, "./ancestor::tr[contains(@class,'master')]")
                        detail_tr = master_tr.find_element(By.XPATH, "following-sibling::tr[contains(@class,'Vde')][1]")
                        style = (detail_tr.get_attribute("style") or "").lower()
                        # cuando se despliega, suele quitar display:none o poner display:table-row
                        return "display: none" not in style
                    except Exception:
                        return True  # si no podemos verificar, no bloqueamos el flujo
                self.wait.until(_expanded)
            except Exception:
                # no bloqueamos por un arrow que falle
                continue
    def screenshot_contenido(self, out_path):
        """
        Screenshot SOLO del div#Contenido (mejor que pantalla completa).
        """
        el = self.wait.until(EC.presence_of_element_located(self.CONTENIDO))
        self.driver.execute_script("arguments[0].scrollIntoView({block:'start'});", el)
        el.screenshot(str(out_path))

    def _wait_tab_loaded(self):
       """
       Espera simple post-click.
       Como no tenemos selector del contenido, usamos un par de esperas:
       - que el body exista
       - que termine el readyState
       """
       self.wait.until(lambda d: d.execute_script("return document.readyState") == "complete")


    def extract_datos_deudor(self) -> dict:
        """
        Devuelve dict: {campo: valor} usando los <b class="Dz"> como labels
        y <span class="Dz"> como valor (cuando aplique).
        """
        tbl = self.wait.until(EC.presence_of_element_located(self.TBL_DATOS_DEUDOR))
        rows = tbl.find_elements(By.CSS_SELECTOR, "tbody tr")

        data = {}
        for r in rows:
            # Tomamos solo filas con datos (tr.Def típicamente)
            tds = r.find_elements(By.CSS_SELECTOR, "td")
            if len(tds) < 2:
                continue

            # Recorremos en pares label/valor
            i = 0
            while i < len(tds) - 1:
                label_el = None
                try:
                    label_el = tds[i].find_element(By.CSS_SELECTOR, "b.Dz")
                except Exception:
                    pass

                label = (label_el.text.strip() if label_el else tds[i].text.strip())
                value_text = tds[i + 1].text.strip()

                # Si no parece label válido, salta
                if label:
                    # Limpieza típica (espacios/linebreaks)
                    label = " ".join(label.split())
                    value_text = " ".join(value_text.split())
                    data[label] = value_text

                i += 2

        return data

    def extract_posicion_consolidada(self) -> list:
        """
        Devuelve lista de filas: [[concepto, saldo_mn, saldo_me, total], ...]
        """
        tbl = self.wait.until(EC.presence_of_element_located(self.TBL_POSICION))
        trs = tbl.find_elements(By.CSS_SELECTOR, "tbody tr")

        out = []
        for tr in trs:
            tds = tr.find_elements(By.CSS_SELECTOR, "td")
            if len(tds) != 4:
                continue

            row = [" ".join(td.text.split()) for td in tds]
            # Filtrar header tipo "SALDOS / Saldo MN / ..."
            if row[0].upper() == "SALDOS":
                continue

            out.append(row)

        return out

    def logout_modulo(self):
        """
        1) Click en 'Salir' del módulo /criesgos/logout...
        2) Espera a que cargue la pantalla siguiente
        """
        link = self.wait.until(EC.element_to_be_clickable(self.LINK_SALIR_CRIESGOS))
        link.click()

        # tras salir del módulo, debería aparecer el botón de logout del portal
        self.wait.until(EC.presence_of_element_located(self.BTN_SALIR_PORTAL))

    def logout_portal(self):
        """
        Click en el botón final del portal: goTo('logout')
        """
        btn = self.wait.until(EC.element_to_be_clickable(self.BTN_SALIR_PORTAL))
        btn.click()





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



services/sbs_flow.py
from pathlib import Path
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
       self.driver.get(URL_LOGIN)
       login_page = LoginPage(self.driver)
       login_page.capture_image(captcha_img_path)
       # --- Copilot en nueva pestaña (y luego cerrarla) ---
       original_handle = self.driver.current_window_handle
       self.driver.switch_to.new_window("tab")
       copilot = CopilotService(CopilotPage(self.driver))
       captcha = copilot.resolve_captcha(captcha_img_path)
       # cerrar tab Copilot y volver a SBS
       try:
           self.driver.close()
       except Exception:
           pass
       self.driver.switch_to.window(original_handle)
       # Login SBS
       login_page.fill_form(self.usuario, self.clave, captcha)
       riesgos = RiesgosPage(self.driver)
       riesgos.open_modulo_deuda()
       riesgos.consultar_por_dni(dni)
       datos_deudor = riesgos.extract_datos_deudor()
       posicion = riesgos.extract_posicion_consolidada()
       # Detallada
       riesgos.go_detallada()
       self.driver.save_screenshot(str(detallada_img_path))
       # Otros Reportes (con lógica carteras)
       riesgos.go_otros_reportes()
       try:
           riesgos.click_carteras_transferidas()
           if riesgos.has_carteras_table():
               riesgos.expand_all_rectificaciones(expected=2)
           riesgos.screenshot_contenido(str(otros_img_path))
       except Exception:
           riesgos.screenshot_contenido(str(otros_img_path))
       # Logout SBS (2 pasos)
       riesgos.logout_modulo()
       riesgos.logout_portal()
       return {
           "datos_deudor": datos_deudor,
           "posicion": posicion,
       }



main.py
from __future__ import annotations
from pathlib import Path
import os
import sys
import tempfile
import logging
from typing import Optional, Tuple
from config.settings import *  # URLs, credenciales, etc.
from infrastructure.edge_debug import EdgeDebugLauncher
from infrastructure.selenium_driver import SeleniumDriverFactory
from services.sbs_flow import SbsFlow
from services.sunat_flow import SunatFlow
from services.rbm_flow import RbmFlow
from services.xlsm_session_writer import XlsmSessionWriter
from utils.logging_utils import setup_logging
from utils.decorators import log_exceptions

def _pick_results_dir(app_dir: Path) -> Path:
   """
   Intenta crear ./results al lado del exe/script.
   Si falla por permisos, usa %LOCALAPPDATA%/PrismaProject/results (o TEMP).
   """
   primary = app_dir / "results"
   try:
       primary.mkdir(parents=True, exist_ok=True)
       # prueba de escritura
       test_file = primary / ".write_test"
       test_file.write_text("ok", encoding="utf-8")
       test_file.unlink(missing_ok=True)
       return primary
   except Exception:
       base = Path(os.environ.get("LOCALAPPDATA", tempfile.gettempdir()))
       fallback = base / "PrismaProject" / "results"
       fallback.mkdir(parents=True, exist_ok=True)
       return fallback

@log_exceptions
def run_app(dni_titular: str, dni_conyuge: Optional[str] = None) -> Tuple[Path, Path]:
   """
   Motor reutilizable para GUI o ejecución programática.
   - dni_titular: obligatorio
   - dni_conyuge: opcional (si viene None/"" no se ejecuta cónyuge)
   Retorna:
     (out_xlsm_path, results_dir)
   """
   setup_logging()
   logging.info("=== INICIO EJECUCION ===")
   logging.info("DNI_TITULAR=%s | DNI_CONYUGE=%s", dni_titular, dni_conyuge or "")
   launcher = EdgeDebugLauncher()
   launcher.ensure_running()
   driver = SeleniumDriverFactory.create()
   try:
       # ====== APP_DIR (carpeta del exe o del proyecto) ======
       if getattr(sys, "frozen", False):
           app_dir = Path(sys.executable).resolve().parent
       else:
           app_dir = Path(__file__).resolve().parent
       # ====== Macro.xlsm debe estar junto al exe ======
       macro_path = app_dir / "Macro.xlsm"
       if not macro_path.exists():
           raise FileNotFoundError(f"No se encontró Macro.xlsm junto al ejecutable: {macro_path}")
       # ====== Carpeta results (con fallback) ======
       results_dir = _pick_results_dir(app_dir)
       # normalizar dni_conyuge
       dni_conyuge = (dni_conyuge or "").strip() or None
       # ====== Output ======
       out_xlsm = (results_dir / "Macro_out.xlsm").with_name(f"Macro_out_{dni_titular}.xlsm")
       # ====== Evidencias TITULAR ======
       # SBS
       captcha_img_path = results_dir / "captura.png"
       detallada_img_path = results_dir / "detallada.png"
       otros_img_path = results_dir / "otros_reportes.png"
       # SBS CONYUGE (si aplica)
       captcha_img_cony_path = results_dir / "sbs_captura_conyuge.png"
       detallada_img_cony_path = results_dir / "sbs_detallada_conyuge.png"
       otros_img_cony_path = results_dir / "sbs_otros_reportes_conyuge.png"
       # SUNAT (solo titular)
       sunat_img_path = results_dir / "sunat_panel.png"
       # RBM titular
       rbm_consumos_img_path = results_dir / "rbm_consumos.png"
       rbm_cem_img_path = results_dir / "rbm_cem.png"
       # RBM conyuge (si aplica)
       rbm_consumos_cony_path = results_dir / "rbm_consumos_conyuge.png"
       rbm_cem_cony_path = results_dir / "rbm_cem_conyuge.png"
       logging.info("APP_DIR=%s", app_dir)
       logging.info("RESULTS_DIR=%s", results_dir)
       logging.info("OUTPUT_XLSM=%s", out_xlsm)
       # ==========================================================
       # 1) SBS TITULAR (incluye logout dentro del flow)
       # ==========================================================
       logging.info("== FLUJO SBS (TITULAR) INICIO ==")
       _ = SbsFlow(driver, USUARIO, CLAVE).run(
           dni=dni_titular,
           captcha_img_path=captcha_img_path,
           detallada_img_path=detallada_img_path,
           otros_img_path=otros_img_path,
       )
       logging.info("== FLUJO SBS (TITULAR) FIN ==")
       # ==========================================================
       # 1.1) SBS CONYUGE (aislado en pestaña nueva) - opcional
       # ==========================================================
       if dni_conyuge:
           logging.info("== FLUJO SBS (CONYUGE) INICIO ==")
           original_handle_sbs = driver.current_window_handle
           try:
               driver.switch_to.new_window("tab")
               _ = SbsFlow(driver, USUARIO, CLAVE).run(
                   dni=dni_conyuge,
                   captcha_img_path=captcha_img_cony_path,
                   detallada_img_path=detallada_img_cony_path,
                   otros_img_path=otros_img_cony_path,
               )
           finally:
               try:
                   driver.close()
               except Exception:
                   pass
               try:
                   driver.switch_to.window(original_handle_sbs)
               except Exception:
                   pass
           logging.info("== FLUJO SBS (CONYUGE) FIN ==")
       # ==========================================================
       # 2) SUNAT TITULAR (NO cónyuge)
       # ==========================================================
       logging.info("== FLUJO SUNAT (TITULAR) INICIO ==")
       try:
           driver.delete_all_cookies()
       except Exception:
           pass
       SunatFlow(driver).run(dni=dni_titular, out_img_path=sunat_img_path)
       logging.info("== FLUJO SUNAT (TITULAR) FIN ==")
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
       # 4) RBM CONYUGE en pestaña nueva (NO SUNAT) - opcional
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
       # 5) Escribir todo en XLSM y guardar UNA SOLA VEZ
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
       with XlsmSessionWriter(macro_path) as writer:
           # ------------------ RBM -> Hoja Inicio (titular) ------------------
           writer.write_cell("Inicio", "C11", rbm_inicio_tit.get("segmento"))
           writer.write_cell("Inicio", "C12", rbm_inicio_tit.get("segmento_riesgo"))
           writer.write_cell("Inicio", "C13", rbm_inicio_tit.get("pdh"))  # Si/No
           writer.write_cell("Inicio", "C15", rbm_inicio_tit.get("score_rcc"))
           # ------------------ CEM titular -> Hoja Inicio ------------------
           for key, row in cem_row_map:
               item = rbm_cem_tit.get(key, {}) or {}
               writer.write_cell("Inicio", f"C{row}", item.get("cuota_bcp", 0))
               writer.write_cell("Inicio", f"D{row}", item.get("cuota_sbs", 0))
               writer.write_cell("Inicio", f"E{row}", item.get("saldo_sbs", 0))
           # ------------------ RBM + CEM cónyuge -> Hoja Inicio (si aplica) ------------------
           if dni_conyuge:
               writer.write_cell("Inicio", "D11", rbm_inicio_cony.get("segmento"))
               writer.write_cell("Inicio", "D12", rbm_inicio_cony.get("segmento_riesgo"))
               writer.write_cell("Inicio", "D15", rbm_inicio_cony.get("score_rcc"))
               for key, row in cem_row_map:
                   item = rbm_cem_cony.get(key, {}) or {}
                   writer.write_cell("Inicio", f"G{row}", item.get("cuota_bcp", 0))
                   writer.write_cell("Inicio", f"H{row}", item.get("cuota_sbs", 0))
                   writer.write_cell("Inicio", f"I{row}", item.get("saldo_sbs", 0))
           # ------------------ Imágenes ------------------
           # SBS (titular)
           writer.add_image_to_range("SBS", detallada_img_path, "C64", "Z110")
           writer.add_image_to_range("SBS", otros_img_path, "C5", "Z50")
           # SBS (cónyuge) - a la derecha para no pisar (si aplica)
           if dni_conyuge:
               writer.add_image_to_range("SBS", detallada_img_cony_path, "AI64", "AY110")
               writer.add_image_to_range("SBS", otros_img_cony_path, "AI5", "AY50")
           # SUNAT (titular)
           writer.add_image_to_range("SUNAT", sunat_img_path, "C5", "O51")
           # RBM (titular)
           writer.add_image_to_range("RBM", rbm_consumos_img_path, "C5", "Z50")
           writer.add_image_to_range("RBM", rbm_cem_img_path, "C64", "Z106")
           # RBM (cónyuge) - a la derecha para no pisar (si aplica)
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

@log_exceptions
def main():
   """
   Modo script (sin GUI): usa valores por defecto de config/settings.py
   """
   run_app(DNI_CONSULTA, DNI_CONYUGE_CONSULTA)

if __name__ == "__main__":
   main()

