# services/rbm_flow.py
from pathlib import Path
import logging

from pages.rbm.rbm_page import RbmPage
from selenium.common.exceptions import TimeoutException, WebDriverException


class RbmFlow:
    def __init__(self, driver):
        self.page = RbmPage(driver)
        self.driver = driver

    def run(
        self,
        dni: str,
        consumos_img_path: Path,
        cem_img_path: Path,
        numoportunidad: str | None = None,
        producto: str | None = None,
        desproducto: str | None = None,
    ):
        try:
            logging.info(
                "[RBM] Contexto: DNI=%s | NUMOPORTUNIDAD=%s | PRODUCTO=%s | DESPRODUCTO=%s",
                dni,
                numoportunidad or "",
                producto or "",
                desproducto or "",
            )

            self.page.open()

            self.page.consultar_dni(dni)
            inicio_fields = self.page.extract_inicio_fields()

            # NUEVO: scores de Consumos según PRODUCTO/DESPRODUCTO
            scores = self.page.extract_scores_por_producto(producto or "", desproducto or "")

            self.page.screenshot_panel_body_cdp(consumos_img_path)

            self.page.go_cem_tab()
            cem_3cols = self.page.extract_cem_3cols()
            self.page.screenshot_panel_body_cdp(cem_img_path)

            out = {"ok": True, "inicio": inicio_fields, "cem": cem_3cols, "scores": scores}
            logging.info("RBM flow return: %s", out)
            return out

        except (TimeoutException, WebDriverException) as e:
            logging.warning("[RBM] No disponible (soft-fail). dni=%s | err=%s", dni, repr(e))
            logging.warning("[RBM] url=%s | title=%s", self.driver.current_url, self.driver.title)

            # Evidencia (siempre útil)
            try:
                err_path = Path(consumos_img_path).with_name("rbm_error.png")
                self.driver.save_screenshot(str(err_path))
                logging.info("[RBM] Screenshot error guardado: %s", err_path)
            except Exception:
                pass

            return {
                "ok": False,
                "error": "RBM_NO_DISPONIBLE",
                "error_detail": repr(e),
                "inicio": {},
                "cem": {},
                "scores": {},
            }
