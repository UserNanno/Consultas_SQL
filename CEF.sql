from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select

from pages.base_page import BasePage


class RiesgosPage(BasePage):
    """
    Página post-login donde existe el link al módulo (criesgos),
    y luego la pantalla del formulario + resultados (tablas).
    """

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

    # Logout
    LNK_SALIR = (
        By.CSS_SELECTOR,
        "a[href*='/criesgos/logout']"
    )

    def open_modulo_deuda(self):
        link = self.wait.until(EC.element_to_be_clickable(self.LINK_MODULO))
        link.click()

    def consultar_por_dni(self, dni: str):
        sel = self.wait.until(EC.presence_of_element_located(self.SELECT_TIPO_DOC))
        Select(sel).select_by_value("11")  # DNI

        inp = self.wait.until(EC.element_to_be_clickable(self.INPUT_DOC))
        inp.click()
        # a veces es un input con value tipo placeholder ("Número de Doc.") -> mejor limpiar con CTRL+A
        inp.send_keys("\u0001")  # CTRL+A
        inp.send_keys(dni)

        btn = self.wait.until(EC.element_to_be_clickable(self.BTN_CONSULTAR))
        btn.click()

        # Esperar a que aparezcan resultados (evita extraer vacío)
        self.wait.until(EC.presence_of_element_located(self.TBL_DATOS_DEUDOR))
        self.wait.until(EC.presence_of_element_located(self.TBL_POSICION))

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

    def logout(self):
        """
        Click al link Salir y espera volver al login (ajusta el selector si tu login usa otro id).
        """
        lnk = self.wait.until(EC.element_to_be_clickable(self.LNK_SALIR))
        lnk.click()

        # Si tu login tiene un elemento claro, espera por él.
        # Ajusta esto al id real de tu login si es distinto:
        self.wait.until(EC.presence_of_element_located((By.ID, "c_c_usuario")))




















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

    EdgeDebugLauncher().ensure_running()
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

        # === FLUJO POST-LOGIN ===
        riesgos = RiesgosPage(driver)
        riesgos.open_modulo_deuda()
        riesgos.consultar_por_dni(DNI_CONSULTA)

        datos_deudor = riesgos.extract_datos_deudor()
        posicion = riesgos.extract_posicion_consolidada()

        ExcelExporter().export_deuda(datos_deudor, posicion, EXCEL_PATH)
        print(f"Excel generado en: {EXCEL_PATH}")

        # Salir al final
        riesgos.logout()
        print("Sesión cerrada (Salir).")

        print("Flujo completo")

    finally:
        # opcional: si quieres cerrar el browser al final
        # driver.quit()
        pass


if __name__ == "__main__":
    main()
