pages/rbm/rbm_page.py
from __future__ import annotations

import base64
import time
from pathlib import Path
import logging

from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select

from pages.base_page import BasePage
from config.settings import URL_RBM


class RbmPage(BasePage):
    SELECT_TIPO_DOC = (By.ID, "CodTipoDocumento")
    INPUT_DOC = (By.ID, "CodDocumento")
    BTN_CONSULTAR = (By.ID, "btnConsultar")

    PANEL_BODY = (By.CSS_SELECTOR, "div.panel-body")

    TAB_CEM = (By.ID, "CEM-tab")
    TABPANE_CEM = (By.ID, "CEM")  # data-target="#CEM"

    def open(self):
        self.driver.get(URL_RBM)
        self.wait.until(EC.presence_of_element_located(self.SELECT_TIPO_DOC))

    # ----------------- helpers -----------------
    def wait_not_loading(self, timeout=40) -> bool:
        """
        RBM tiene overlay de carga:
          body.loading + .loadingmodal (capa blanca).
        Esperamos a que se quite la clase "loading" del body.
        """
        end = time.time() + timeout
        while time.time() < end:
            try:
                is_loading = self.driver.execute_script(
                    "return document.body && document.body.classList.contains('loading');"
                )
                if not is_loading:
                    return True
            except Exception:
                pass
            time.sleep(0.2)
        return False

    # ----------------- acciones -----------------
    def consultar_dni(self, dni: str):
        sel = Select(self.wait.until(EC.presence_of_element_located(self.SELECT_TIPO_DOC)))
        sel.select_by_value("1")  # DNI

        inp = self.wait.until(EC.element_to_be_clickable(self.INPUT_DOC))
        inp.click()
        inp.clear()
        inp.send_keys(dni)

        self.wait.until(EC.element_to_be_clickable(self.BTN_CONSULTAR)).click()
        self.wait.until(EC.visibility_of_element_located(self.PANEL_BODY))

        # importante para evitar captura con overlay
        self.wait_not_loading(timeout=40)

    def go_cem_tab(self):
        self.wait.until(EC.element_to_be_clickable(self.TAB_CEM)).click()

        # 1) tab seleccionado
        self.wait.until(
            lambda d: (d.find_element(*self.TAB_CEM).get_attribute("aria-selected") or "").lower() == "true"
        )

        # 2) panel activo/visible
        def cem_ready(d):
            pane = d.find_element(*self.TABPANE_CEM)
            cls = (pane.get_attribute("class") or "").lower()
            return ("active" in cls) and pane.is_displayed()

        self.wait.until(cem_ready)

        # 3) transición fade terminada (opacidad final)
        self.wait.until(
            lambda d: float(
                d.execute_script(
                    "return parseFloat(getComputedStyle(arguments[0]).opacity) || 1;",
                    d.find_element(*self.TABPANE_CEM),
                )
            )
            >= 0.99
        )

        self.wait.until(EC.visibility_of_element_located(self.PANEL_BODY))
        self.wait_not_loading(timeout=40)

    # ----------------- screenshots -----------------
    def screenshot_panel_body_cdp(self, out_path):
        """
        Captura robusta con CDP clip (ideal para Consumos donde se cortaba abajo).
        Evita problemas de overlay/topbar/sidebar.
        """
        self.wait_not_loading(timeout=40)

        panel = self.wait.until(EC.presence_of_element_located(self.PANEL_BODY))
        self.driver.execute_script("arguments[0].scrollIntoView({block:'start'});", panel)
        time.sleep(0.25)

        # ocultar elementos fixed que estorban
        self.driver.execute_script(
            """
            window.__rbm_prev_vis = window.__rbm_prev_vis || {};
            const sels = ['.topbar', '.left.side-menu'];
            sels.forEach(sel => {
              const el = document.querySelector(sel);
              if (!el) return;
              window.__rbm_prev_vis[sel] = el.style.visibility;
              el.style.visibility = 'hidden';
            });
            """
        )

        try:
            m = self.driver.execute_script(
                """
                const el = arguments[0];
                const r = el.getBoundingClientRect();
                const dpr = window.devicePixelRatio || 1;
                const sx = window.scrollX || window.pageXOffset || 0;
                const sy = window.scrollY || window.pageYOffset || 0;

                return {
                  x: Math.max(0, r.left + sx),
                  y: Math.max(0, r.top + sy),
                  w: Math.max(1, r.width),
                  h: Math.max(1, r.height),
                  dpr: dpr
                };
                """,
                panel,
            )

            clip = {
                "x": m["x"] * m["dpr"],
                "y": m["y"] * m["dpr"],
                "width": m["w"] * m["dpr"],
                "height": m["h"] * m["dpr"],
                "scale": 1,
            }

            data = self.driver.execute_cdp_cmd(
                "Page.captureScreenshot",
                {
                    "format": "png",
                    "fromSurface": True,
                    "captureBeyondViewport": True,
                    "clip": clip,
                },
            )["data"]

            out_path = Path(out_path)
            out_path.parent.mkdir(parents=True, exist_ok=True)
            out_path.write_bytes(base64.b64decode(data))

        finally:
            self.driver.execute_script(
                """
                const prev = window.__rbm_prev_vis || {};
                Object.keys(prev).forEach(sel => {
                  const el = document.querySelector(sel);
                  if (!el) return;
                  el.style.visibility = prev[sel] || '';
                });
                """
            )

    # --- nuevos selectores ---
    INPUT_SEGMENTO_RIESGO = (By.ID, "SegmentoRiesgo")
    # ----------------- extracción de datos -----------------
    def _text_norm(self, s: str) -> str:
        return " ".join((s or "").split()).strip()
    def extract_segmento(self) -> str:
        """
        Encuentra el valor del campo 'Segmento' en el panel inicial.
        Estrategia: buscar el div.editor-label con texto 'Segmento' y tomar el siguiente editor-field.
        """
        self.wait_not_loading(timeout=40)
        el = self.driver.find_element(
            By.XPATH,
            "//div[contains(@class,'editor-label')][normalize-space()='Segmento']"
            "/following-sibling::div[contains(@class,'editor-field')][1]"
        )
        return self._text_norm(el.text)
    def extract_segmento_riesgo(self) -> str:
        """
        Toma el value del input hidden #SegmentoRiesgo (fuente más confiable que el texto del td).
        """
        self.wait_not_loading(timeout=40)
        el = self.wait.until(EC.presence_of_element_located(self.INPUT_SEGMENTO_RIESGO))
        return self._text_norm(el.get_attribute("value"))
    def extract_situacion_laboral_badge(self) -> str:
        """
        Devuelve el texto del badge asociado a 'Situación Laboral'.
        """
        self.wait_not_loading(timeout=40)
        span = self.driver.find_element(
            By.XPATH,
            "//li[contains(@class,'list-group-item')][.//div[normalize-space()='Situación Laboral']]"
            "//span[contains(@class,'badge')]"
        )
        return self._text_norm(span.text)
    def extract_score_rcc(self) -> str:
        """
        Devuelve el valor del badge para 'Score RCC'.
        """
        self.wait_not_loading(timeout=40)
        span = self.driver.find_element(
            By.XPATH,
            "//li[contains(@class,'list-group-item')][contains(normalize-space(.),'Score RCC')]"
            "//span[contains(@class,'badge')]"
        )
        return self._text_norm(span.text)
    def extract_inicio_fields(self) -> dict:
        """
        Extrae todos los campos requeridos para la hoja 'Inicio':
        - segmento (ej ENALTA)
        - segmento_riesgo (ej A)
        - situacion_laboral_raw (ej 'No PdH – Dependiente' o 'PDH')
        - pdh (Si/No) => Si solo si situacion_laboral_raw == 'PDH'
        - score_rcc (ej 289)
        """
        self.wait_not_loading(timeout=40)
        segmento = self.extract_segmento()
        segmento_riesgo = self.extract_segmento_riesgo()
        sit_lab = self.extract_situacion_laboral_badge()
        pdh = "Si" if sit_lab == "PDH" else "No"
        score_rcc = self.extract_score_rcc()
        data = {
            "segmento": segmento,
            "segmento_riesgo": segmento_riesgo,
            "situacion_laboral_raw": sit_lab,
            "pdh": pdh,
            "score_rcc": score_rcc,
        }
        logging.info("RBM extract_inicio_fields: %s", data)
        return data
    
    def _num_from_value(self, raw: str) -> int:
        """
        Convierte textos como '-', '', '3,581', ' 212 ' a int.
        Regla: '-' o vacío => 0
        """
        s = (raw or "").strip()
        if not s or s == "-":
            return 0
        s = s.replace(",", "")  # 3,581 -> 3581
        try:
            return int(float(s))
        except Exception:
            return 0
    def _get_input_value_int(self, input_id: str) -> int:
        """
        Lee value de un input por id. Si no existe, devuelve 0.
        """
        try:
            el = self.driver.find_element(By.ID, input_id)
            return self._num_from_value(el.get_attribute("value"))
        except Exception:
            return 0
    def extract_cem_3cols(self) -> dict:
        """
        Extrae (solo) 3 columnas del CEM por producto:
        - cuota_bcp (BCP - Cuota)
        - cuota_sbs (SBS No BCP - Cuota)
        - saldo_sbs (SBS No BCP - Saldo)
        Usa inputs hidden por ID (más robusto que leer spans '-').
        Nota: 'Linea No Utilizada' usa otros IDs (ver mapeo).
        """
        self.wait_not_loading(timeout=40)
        data = {
            "hipotecario": {
                "cuota_bcp": self._get_input_value_int("CuotaHipotecarioBCP"),
                "cuota_sbs": self._get_input_value_int("CuotaHipotecarioNoBCP"),
                "saldo_sbs": self._get_input_value_int("SaldoHipotecarioNoBCP"),
            },
            "cef": {
                "cuota_bcp": self._get_input_value_int("CuotaCEFBCP"),
                "cuota_sbs": self._get_input_value_int("CuotaCEFNoBCP"),
                "saldo_sbs": self._get_input_value_int("SaldoCEFNoBCP"),
            },
            "vehicular": {
                "cuota_bcp": self._get_input_value_int("CuotaCVBCP"),
                "cuota_sbs": self._get_input_value_int("CuotaCVNoBCP"),
                "saldo_sbs": self._get_input_value_int("SaldoCVNoBCP"),
            },
            "pyme": {
                "cuota_bcp": self._get_input_value_int("CuotaPymeBCP"),
                "cuota_sbs": self._get_input_value_int("CuotaPymeNoBCP"),
                "saldo_sbs": self._get_input_value_int("SaldoPymeNoBCP"),
            },
            "comercial": {
                "cuota_bcp": self._get_input_value_int("CuotaComercialBCP"),
                "cuota_sbs": self._get_input_value_int("CuotaComercialNoBCP"),
                "saldo_sbs": self._get_input_value_int("SaldoComercialNoBCP"),
            },
            "deuda_indirecta": {
                # En tu HTML SBS(NoBCP) Cuota está como CuotaIndRCC y saldo como SaldoIndNoBCP
                "cuota_bcp": self._get_input_value_int("CuotaIndBCP"),
                "cuota_sbs": self._get_input_value_int("CuotaIndRCC"),
                "saldo_sbs": self._get_input_value_int("SaldoIndNoBCP"),
            },
            "tarjeta": {
                "cuota_bcp": self._get_input_value_int("CuotaTarjetaBCP"),
                "cuota_sbs": self._get_input_value_int("CuotaTarjetaNoBCP"),
                "saldo_sbs": self._get_input_value_int("SaldoTarjetaNoBCP"),
            },
            "linea_no_utilizada": {
                # Regla del HTML:
                # - BCP cuota: CuotaLineaDisponibleBCP (input text readonly, value="3")
                # - SBS cuota: CuotaLineaDisponibleNoBCP (hidden, value="17")
                # - SBS saldo: LineaDisponibleNoBCP (hidden, value="6308")  <-- saldo muestra la línea no utilizada
                "cuota_bcp": self._get_input_value_int("CuotaLineaDisponibleBCP"),
                "cuota_sbs": self._get_input_value_int("CuotaLineaDisponibleNoBCP"),
                "saldo_sbs": self._get_input_value_int("LineaDisponibleNoBCP"),
            },
        }
        logging.info("RBM extract_cem_3cols: %s", data)
        return data






















services/rbm_flow.py
from pathlib import Path
from pages.rbm.rbm_page import RbmPage
import logging

class RbmFlow:
    def __init__(self, driver):
        self.page = RbmPage(driver)

    def run(self, dni: str, consumos_img_path: Path, cem_img_path: Path):
        self.page.open()

        self.page.consultar_dni(dni)
        inicio_fields = self.page.extract_inicio_fields()
        self.page.screenshot_panel_body_cdp(consumos_img_path)

        self.page.go_cem_tab()
        cem_3cols = self.page.extract_cem_3cols()
        self.page.screenshot_panel_body_cdp(cem_img_path)
        out = {
           "inicio": inicio_fields,
           "cem": cem_3cols,
        }
        logging.info("RBM flow return: %s", out)
        return out


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
