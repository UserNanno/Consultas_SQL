Mira tengo otro detalle, cuando busca otros reportes en SBS se queda esperando que haya info ahi para desglosar esos botones pero hay dni que no tienen eso y les figura asi el html
<table class="Crw">
        <thead>
          <tr>
            <td class="Izq">
              <span class="F">Otros Reportes</span>
            </td>
          </tr>
        </thead>
         
        <tbody>
          <tr class="Def">
            <td class="Izq">
              
               
              No existe información en Otros Reportes
               
              
            </td>
          </tr>
        </tbody>
      </table>


pero cuando si tiene les figura asi:
<table class="Crw">
        <thead>
          <tr>
            <td class="Izq">
              <span class="F">Otros Reportes</span>
            </td>
          </tr>
        </thead>
         
        <tbody>
          <tr class="Def">
            <td class="Izq">
              
              <ul id="OtrosReportes" class="horizontal">
                
                 
                
                <li>
                  <a onclick="jsVerCT('-1299968084957350848'); return false;" href="#">
                    <img src="images/mas.gif" alt="Carteras Transferidas"></a>
                </li>
                 
                <li>
                  <a onclick="jsVerCT('-3829640554239577169'); return false;" href="#">Carteras Transferidas</a>
                </li>
                
                 
                
                
                
              </ul>
              
            </td>
          </tr>
        </tbody>
      </table>


Entonces cuando no encuentra, ese boton para desglosar, se queda esperando y demora. Que podriamos hacer? Buscar ambas ocpiones no? 

pages/sbs/riesgos_page.py
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
   def click_carteras_transferidas(self) -> bool:
       """
       No bloqueante: espera corta (5s).
       """
       self.wait.until(EC.presence_of_element_located(self.OTROS_LIST))
       self.wait.until(EC.element_to_be_clickable(self.LNK_CARTERAS_TRANSFERIDAS)).click()
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
               # fallback si element.screenshot se pone lento / stale
               self.driver.save_screenshot(str(out_path))
       except TimeoutException:
           # si no aparece rápido, no bloqueamos el flujo
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




services/sbs_flow.py
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


@log_exceptions
def run_app(dni_titular: str, dni_conyuge: Optional[str] = None) -> Tuple[Path, Path]:
    """
    Motor reutilizable para GUI o ejecución programática.
    - dni_titular: obligatorio
    - dni_conyuge: opcional (si viene None/"" no se ejecuta cónyuge)
    Retorna:
      (out_xlsm_path, results_dir)
    """
    # ====== APP_DIR (primero) ======
    app_dir = _get_app_dir()

    # ====== LOGGING (robusto, con fallback) ======
    setup_logging(app_dir)

    logging.info("=== INICIO EJECUCION ===")
    logging.info("DNI_TITULAR=%s | DNI_CONYUGE=%s", dni_titular, dni_conyuge or "")
    logging.info("APP_DIR=%s", app_dir)

    launcher = EdgeDebugLauncher()
    launcher.ensure_running()
    driver = SeleniumDriverFactory.create()

    try:
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

        logging.info("RESULTS_DIR=%s", results_dir)
        logging.info("OUTPUT_XLSM=%s", out_xlsm)

        # ==========================================================
        # 1) SBS TITULAR
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
        # 1.1) SBS CONYUGE (opcional)
        # ==========================================================
        if dni_conyuge:
            logging.info("== FLUJO SBS (CONYUGE) INICIO ==")
            _ = SbsFlow(driver, USUARIO, CLAVE).run(
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
        )
        rbm_inicio_tit = rbm_titular.get("inicio", {}) if isinstance(rbm_titular, dict) else {}
        rbm_cem_tit = rbm_titular.get("cem", {}) if isinstance(rbm_titular, dict) else {}
        logging.info("RBM titular inicio=%s", rbm_inicio_tit)
        logging.info("RBM titular cem=%s", rbm_cem_tit)
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
            writer.write_cell("Inicio", "C11", rbm_inicio_tit.get("segmento"))
            writer.write_cell("Inicio", "C12", rbm_inicio_tit.get("segmento_riesgo"))
            writer.write_cell("Inicio", "C13", rbm_inicio_tit.get("pdh"))
            writer.write_cell("Inicio", "C15", rbm_inicio_tit.get("score_rcc"))

            for key, row in cem_row_map:
                item = rbm_cem_tit.get(key, {}) or {}
                writer.write_cell("Inicio", f"C{row}", item.get("cuota_bcp", 0))
                writer.write_cell("Inicio", f"D{row}", item.get("cuota_sbs", 0))
                writer.write_cell("Inicio", f"E{row}", item.get("saldo_sbs", 0))

            if dni_conyuge:
                writer.write_cell("Inicio", "D11", rbm_inicio_cony.get("segmento"))
                writer.write_cell("Inicio", "D12", rbm_inicio_cony.get("segmento_riesgo"))
                writer.write_cell("Inicio", "D15", rbm_inicio_cony.get("score_rcc"))

                for key, row in cem_row_map:
                    item = rbm_cem_cony.get(key, {}) or {}
                    writer.write_cell("Inicio", f"G{row}", item.get("cuota_bcp", 0))
                    writer.write_cell("Inicio", f"H{row}", item.get("cuota_sbs", 0))
                    writer.write_cell("Inicio", f"I{row}", item.get("saldo_sbs", 0))

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
