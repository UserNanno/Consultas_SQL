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


# =============================================================================
# POST-PROCESO EXCEL (COM) PARA DISPARAR TRIGGERS DE LISTAS DESPLEGABLES
# (sin tocar VBA). Requiere Excel instalado + pywin32.
# =============================================================================
def _apply_excel_triggers_inicio(
    xlsm_path: Path,
    producto_c4: Optional[str],
    segmento: Optional[str],
    segmento_riesgo: Optional[str],
    pdh: Optional[str],
    sheet_name: str = "Inicio",
):
    """
    Abre el XLSM con Excel real vía COM para que se ejecuten eventos/macros
    existentes del archivo (Worksheet_Change, etc.).

    Truco: clear + set de celdas (simula selección del desplegable).
    """
    xlsm_path = Path(xlsm_path)
    if not xlsm_path.exists():
        raise FileNotFoundError(f"No existe XLSM para post-proceso: {xlsm_path}")

    try:
        import win32com.client as win32  # type: ignore
    except Exception as e:
        logging.warning(
            "[XLSM] No se pudo importar win32com (pywin32). "
            "Se omite post-proceso de triggers. Detalle=%r",
            e,
        )
        return

    logging.info("[XLSM] Post-proceso Excel(COM) para triggers en %s", xlsm_path)

    xl = win32.DispatchEx("Excel.Application")
    xl.Visible = False
    xl.DisplayAlerts = False

    # CLAVE: permitir que se disparen eventos
    xl.EnableEvents = True

    wb = xl.Workbooks.Open(str(xlsm_path))
    try:
        ws = wb.Worksheets(sheet_name)

        def set_cell(addr: str, value):
            if value is None:
                return
            ws.Range(addr).Value = ""
            ws.Range(addr).Value = value

        # 🔥 Orden recomendado: primero producto, luego segmentación
        set_cell("C4", producto_c4)
        set_cell("C11", segmento)
        set_cell("C12", segmento_riesgo)
        set_cell("C13", pdh)

        # Por si hay fórmulas/condicionales dependientes
        xl.CalculateFull()

        wb.Save()
        logging.info("[XLSM] Triggers aplicados: %s!C4,C11:C13", sheet_name)

    finally:
        try:
            wb.Close(SaveChanges=True)
        except Exception:
            pass
        try:
            xl.Quit()
        except Exception:
            pass


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

    if not (numoportunidad or "").strip():
        raise ValueError("NUMOPORTUNIDAD es obligatorio.")
    if not (producto or "").strip():
        raise ValueError("PRODUCTO es obligatorio.")
    if not (desproducto or "").strip():
        raise ValueError("DESPRODUCTO es obligatorio.")

    matanalista = load_matanalista("").strip()
    if not matanalista:
        raise ValueError("No hay MATANALISTA configurado. Ve al botón 'MATANALISTA' y guárdalo.")
    logging.info("MATANALISTA runtime=%s", matanalista)

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

        # 1) SBS TITULAR
        logging.info("== FLUJO SBS (TITULAR) INICIO ==")
        _ = SbsFlow(driver, sbs_user, sbs_pass).run(
            dni=dni_titular,
            captcha_img_path=captcha_img_path,
            detallada_img_path=detallada_img_path,
            otros_img_path=otros_img_path,
        )
        logging.info("== FLUJO SBS (TITULAR) FIN ==")

        # 1.1) SBS CONYUGE
        if dni_conyuge:
            logging.info("== FLUJO SBS (CONYUGE) INICIO ==")
            _ = SbsFlow(driver, sbs_user, sbs_pass).run(
                dni=dni_conyuge,
                captcha_img_path=captcha_img_cony_path,
                detallada_img_path=detallada_img_cony_path,
                otros_img_path=otros_img_cony_path,
            )
            logging.info("== FLUJO SBS (CONYUGE) FIN ==")

        # 2) SUNAT TITULAR
        logging.info("== FLUJO SUNAT (TITULAR) INICIO ==")
        try:
            driver.delete_all_cookies()
        except Exception:
            pass
        SunatFlow(driver).run(dni=dni_titular, out_img_path=sunat_img_path)
        logging.info("== FLUJO SUNAT (TITULAR) FIN ==")

        # 3) RBM TITULAR
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

        # 4) RBM CONYUGE
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

        # 5) Escribir XLSM (openpyxl)
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
            if producto_excel_c4:
                writer.write_cell("Inicio", "C4", producto_excel_c4)

            writer.write_cell("Inicio", "C11", rbm_inicio_tit.get("segmento"))
            writer.write_cell("Inicio", "C12", rbm_inicio_tit.get("segmento_riesgo"))
            writer.write_cell("Inicio", "C13", rbm_inicio_tit.get("pdh"))
            writer.write_cell("Inicio", "C15", rbm_inicio_tit.get("score_rcc"))

            if rbm_scores_tit.get("inicio_c14") is not None:
                writer.write_cell("Inicio", "C14", rbm_scores_tit.get("inicio_c14"))
            if rbm_scores_tit.get("inicio_c83") is not None:
                writer.write_cell("Inicio", "C83", rbm_scores_tit.get("inicio_c83"))

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

        # 5.1) Post-proceso: disparar triggers (Producto + Segmentación)
        _apply_excel_triggers_inicio(
            out_xlsm,
            producto_excel_c4,
            rbm_inicio_tit.get("segmento"),
            rbm_inicio_tit.get("segmento_riesgo"),
            rbm_inicio_tit.get("pdh"),
            sheet_name="Inicio",
        )

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
