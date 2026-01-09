infrastructure/edge_debug.py
import socket
import time
import subprocess
import os
from pathlib import Path

from config.settings import EDGE_EXE, DEBUG_PORT


class EdgeDebugLauncher:
    def __init__(self):
        self.process = None

    def _wait_port(self, host, port, timeout=15):
        start = time.time()
        while time.time() - start < timeout:
            try:
                with socket.create_connection((host, port), timeout=1):
                    return True
            except OSError:
                time.sleep(0.2)
        return False

    def ensure_running(self):
        profile_dir = Path(os.environ["LOCALAPPDATA"]) / "PrismaProject" / "edge_profile"
        profile_dir.mkdir(parents=True, exist_ok=True)

        # Lanza Edge (guardamos el process para poder cerrarlo al final)
        self.process = subprocess.Popen([
            EDGE_EXE,
            f"--remote-debugging-port={DEBUG_PORT}",
            f"--user-data-dir={profile_dir}",
            "--start-maximized",
            "--new-window",
        ], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

        if not self._wait_port("127.0.0.1", DEBUG_PORT, timeout=20):
            raise RuntimeError("Edge no abrió el puerto de debugging")

    def close(self):
        """
        Cierra el Edge que abrió este launcher.
        Importante: esto mata el proceso y sus hijos (/T).
        """
        if self.process is None:
            return

        try:
            subprocess.run(
                ["taskkill", "/PID", str(self.process.pid), "/T", "/F"],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                check=False
            )
        finally:
            self.process = None




infrastructure/selenium_driver.py
from selenium import webdriver
from selenium.webdriver.edge.options import Options
from selenium.webdriver.common.keys import Keys
from config.settings import DEBUG_PORT

class SeleniumDriverFactory:

    @staticmethod
    def create():
        options = Options()
        options.add_experimental_option("debuggerAddress", f"127.0.0.1:{DEBUG_PORT}")
        driver = webdriver.Edge(options=options)

        # lo que tenías antes (viewport fijo para evitar interferencias al screenshot)
        driver.set_window_size(1357, 924)
        driver.execute_cdp_cmd("Emulation.setDeviceMetricsOverride", {
            "width": 1357,
            "height": 924,
            "deviceScaleFactor": 1,
            "mobile": False
        })

        # fuerza zoom 100% (muy importante si el perfil quedó en 175%)
        SeleniumDriverFactory._reset_zoom_100(driver)

        return driver

    @staticmethod
    def _reset_zoom_100(driver):
        # Método 1 (más confiable): CTRL+0 sobre el <body>
        try:
            body = driver.find_element("tag name", "body")
            body.click()
            body.send_keys(Keys.CONTROL, "0")
        except Exception:
            # fallback: mandar la combinación al documento con ActionChains no siempre ayuda,
            # así que lo dejamos silencioso.
            pass

        # Método 2 (fallback CDP): setPageScaleFactor (si el navegador lo soporta)
        try:
            driver.execute_cdp_cmd("Emulation.setPageScaleFactor", {"pageScaleFactor": 1})
        except Exception:
            pass

        # Método 3 (último recurso): zoom CSS (afecta solo la página actual)
        try:
            driver.execute_script("document.body.style.zoom='100%';")
        except Exception:
            pass




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
