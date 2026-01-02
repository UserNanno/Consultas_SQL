from pathlib import Path

from config.settings import *
from infrastructure.edge_debug import EdgeDebugLauncher
from infrastructure.selenium_driver import SeleniumDriverFactory
from pages.login_page import LoginPage
from pages.copilot_page import CopilotPage
from pages.riesgos_page import RiesgosPage
from services.copilot_service import CopilotService
from services.excel_exporter import ExcelExporter
from utils.logging_utils import setup_logging
from utils.decorators import log_exceptions


@log_exceptions
def main():
    setup_logging()

    launcher = EdgeDebugLauncher()
    launcher.ensure_running()

    driver = SeleniumDriverFactory.create()

    try:
        driver.get(URL_LOGIN)

        login_page = LoginPage(driver)
        login_page.capture_image(IMG_PATH)

        # Resolver captcha por Copilot
        driver.switch_to.new_window("tab")
        copilot = CopilotService(CopilotPage(driver))
        captcha = copilot.resolve_captcha(IMG_PATH)

        # Volver al login y completar
        driver.switch_to.window(driver.window_handles[0])
        login_page.fill_form(USUARIO, CLAVE, captcha)

        # === POST-LOGIN ===
        riesgos = RiesgosPage(driver)
        riesgos.open_modulo_deuda()

        dni = DNI_CONSULTA
        riesgos.consultar_por_dni(dni)

        datos_deudor = riesgos.extract_datos_deudor()
        posicion = riesgos.extract_posicion_consolidada()

        # === Excel con DNI en el nombre ===
        excel_dir = Path(EXCEL_DIR)
        excel_dir.mkdir(parents=True, exist_ok=True)
        excel_path = excel_dir / f"deuda_{dni}.xlsx"

        ExcelExporter().export_deuda(datos_deudor, posicion, excel_path)
        print(f"Excel generado en: {excel_path}")

        # === Logout (2 pasos) ===
        riesgos.logout_modulo()
        riesgos.logout_portal()

        print("Sesión cerrada. Fin del flujo.")

    finally:
        # Cierra el webdriver (tabs controladas)
        try:
            driver.quit()
        except Exception:
            pass

        # Cierra Edge (el proceso que abriste con debugging)
        try:
            launcher.close()
        except Exception:
            pass


if __name__ == "__main__":
    main()
