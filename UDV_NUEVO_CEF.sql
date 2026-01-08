pages/sunat/sunat_page.py
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select
from pages.base_page import BasePage
from config.settings import URL_SUNAT

class SunatPage(BasePage):
   BTN_POR_DOCUMENTO = (By.ID, "btnPorDocumento")
   CMB_TIPO_DOC = (By.ID, "cmbTipoDoc")
   TXT_NUM_DOC = (By.ID, "txtNumeroDocumento")
   # ✅ Botón Buscar real
   BTN_BUSCAR = (By.ID, "btnAceptar")
   RESULT_ITEM = (By.CSS_SELECTOR, "a.aRucs.list-group-item, a.aRucs")
   PANEL_RESULTADO = (By.CSS_SELECTOR, "div.panel.panel-primary")
   def open(self):
       self.driver.get(URL_SUNAT)
       self.wait.until(EC.presence_of_element_located(self.BTN_POR_DOCUMENTO))
   def buscar_por_dni(self, dni: str):
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
       # 5) Click primer RUC encontrado
       first = self.wait.until(EC.element_to_be_clickable(self.RESULT_ITEM))
       first.click()
       # 6) Esperar panel final
       self.wait.until(EC.presence_of_element_located(self.PANEL_RESULTADO))
   def screenshot_panel_resultado(self, out_path):
       panel = self.wait.until(EC.presence_of_element_located(self.PANEL_RESULTADO))
       self.driver.execute_script("arguments[0].scrollIntoView({block:'start'});", panel)
       panel.screenshot(str(out_path))













services/sunat_service.py
from pathlib import Path
from pages.sunat.sunat_page import SunatPage

class SunatFlow:
   def __init__(self, driver):
       self.page = SunatPage(driver)
   def run(self, dni: str, out_img_path: Path):
       self.page.open()
       self.page.buscar_por_dni(dni)
       self.page.screenshot_panel_resultado(out_img_path)








main.py
from __future__ import annotations
from pathlib import Path
import os
import sys
import tempfile
import logging
from typing import Optional, Tuple

from config.settings import *  # URLs, etc. (SIN usuario/clave SBS)
from config.credentials_store import load_sbs_credentials
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
    app_dir = _get_app_dir()
    setup_logging(app_dir)

    logging.info("=== INICIO EJECUCION ===")
    logging.info("DNI_TITULAR=%s | DNI_CONYUGE=%s", dni_titular, dni_conyuge or "")
    logging.info("APP_DIR=%s", app_dir)

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

        out_xlsm = (results_dir / "Macro_out.xlsm").with_name(f"Macro_out_{dni_titular}.xlsm")

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
