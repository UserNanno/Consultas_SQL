
from config.settings import *

from infrastructure.edge_debug import EdgeDebugLauncher
from infrastructure.selenium_driver import SeleniumDriverFactory

from services.sbs_flow import SbsFlow
from services.sunat_flow import SunatFlow
from services.xlsm_session_writer import XlsmSessionWriter

from utils.logging_utils import setup_logging
from utils.decorators import log_exceptions


@log_exceptions
def main():
    setup_logging()

    launcher = EdgeDebugLauncher()
    launcher.ensure_running()

    driver = SeleniumDriverFactory.create()

    try:
        
        if not MACRO_XLSM_PATH.exists():
            raise FileNotFoundError(f"No se encontró Macro.xlsm junto al ejecutable: {MACRO_XLSM_PATH}")
        
        dni = DNI_CONSULTA

        out_xlsm = OUTPUT_XLSM_PATH.with_name(
            f"{OUTPUT_XLSM_PATH.stem}_{dni}{OUTPUT_XLSM_PATH.suffix}"
        )

        # 1) SBS (incluye logout dentro)
        sbs_data = SbsFlow(driver, USUARIO, CLAVE).run(
            dni=dni,
            captcha_img_path=IMG_PATH,
            detallada_img_path=DETALLADA_IMG_PATH,
            otros_img_path=OTROS_IMG_PATH,
        )

        # 2) SUNAT (después de terminar SBS)
        try:
            driver.delete_all_cookies()
        except Exception:
            pass

        SunatFlow(driver).run(dni=dni, out_img_path=SUNAT_IMG_PATH)

        # 3) Pegar TODO y guardar UNA SOLA VEZ (desde plantilla limpia)
        with XlsmSessionWriter(MACRO_XLSM_PATH) as writer:
            # SBS
            writer.add_image_to_range("SBS", DETALLADA_IMG_PATH, "C64", "Z110")
            writer.add_image_to_range("SBS", OTROS_IMG_PATH, "C5", "Z50")

            # SUNAT
            writer.add_image_to_range("SUNAT", SUNAT_IMG_PATH, "C5", "O51")

            writer.save(out_xlsm)

        print(f"XLSM final generado: {out_xlsm}")

    finally:
        try:
            driver.quit()
        except Exception:
            pass

        try:
            launcher.close()
        except Exception:
            pass


if __name__ == "__main__":
    main()




ASI QUEDA EL MAIN?
