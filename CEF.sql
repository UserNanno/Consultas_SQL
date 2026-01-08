from pathlib import Path
import logging

from pages.rbm.rbm_page import RbmPage
from selenium.common.exceptions import TimeoutException, WebDriverException


class RbmFlow:
    def __init__(self, driver):
        self.page = RbmPage(driver)
        self.driver = driver  # para screenshot en error

    def run(self, dni: str, consumos_img_path: Path, cem_img_path: Path):
        try:
            self.page.open()

            self.page.consultar_dni(dni)
            inicio_fields = self.page.extract_inicio_fields()
            self.page.screenshot_panel_body_cdp(consumos_img_path)

            self.page.go_cem_tab()
            cem_3cols = self.page.extract_cem_3cols()
            self.page.screenshot_panel_body_cdp(cem_img_path)

            out = {"ok": True, "inicio": inicio_fields, "cem": cem_3cols}
            logging.info("RBM flow return: %s", out)
            return out

        except (TimeoutException, WebDriverException) as e:
            # ✅ “Error esperado”: RBM no accesible / caído / sin permisos / sesión, etc.
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
            }











from selenium.common.exceptions import TimeoutException

def open(self):
    self.driver.get(URL_RBM)
    try:
        self.wait.until(EC.presence_of_element_located(self.SELECT_TIPO_DOC))
    except TimeoutException:
        logging.warning("[RBM] No cargó SELECT_TIPO_DOC | url=%s | title=%s",
                        self.driver.current_url, self.driver.title)
        raise








writer.write_cell("Inicio", "B10", "RBM")
writer.write_cell("Inicio", "C10", "OK" if rbm_titular.get("ok", True) else "NO DISPONIBLE")
