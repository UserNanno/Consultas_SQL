pages/riesgos_page.py
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

    def open_modulo_deuda(self):
        # Click al link del módulo
        link = self.wait.until(EC.element_to_be_clickable(self.LINK_MODULO))
        link.click()

    def consultar_por_dni(self, dni: str):
        # Seleccionar tipo doc = DNI (value=11)
        sel = self.wait.until(EC.presence_of_element_located(self.SELECT_TIPO_DOC))
        Select(sel).select_by_value("11")

        # Escribir DNI
        inp = self.wait.until(EC.element_to_be_clickable(self.INPUT_DOC))
        inp.click()
        inp.clear()
        inp.send_keys(dni)

        # Click Consultar
        btn = self.wait.until(EC.element_to_be_clickable(self.BTN_CONSULTAR))
        btn.click()

    def extract_datos_deudor(self) -> dict:
        """
        Devuelve dict: {campo: valor} usando los <b class="Dz"> como labels
        y <span class="Dz"> como valor (cuando aplique).
        """
        tbl = self.wait.until(EC.presence_of_element_located(self.TBL_DATOS_DEUDOR))
        rows = tbl.find_elements(By.CSS_SELECTOR, "tbody tr")

        data = {}
        for r in rows:
            # Tomamos solo filas con datos (tr.Def típicamente)
            tds = r.find_elements(By.CSS_SELECTOR, "td")
            if len(tds) < 2:
                continue

            # Recorremos en pares label/valor
            i = 0
            while i < len(tds) - 1:
                label_el = None
                try:
                    label_el = tds[i].find_element(By.CSS_SELECTOR, "b.Dz")
                except Exception:
                    pass

                label = (label_el.text.strip() if label_el else tds[i].text.strip())
                value_text = tds[i + 1].text.strip()

                # Si no parece label válido, salta
                if label:
                    # Limpieza típica (espacios/linebreaks)
                    label = " ".join(label.split())
                    value_text = " ".join(value_text.split())
                    data[label] = value_text

                i += 2

        return data

    def extract_posicion_consolidada(self) -> list:
        """
        Devuelve lista de filas: [[concepto, saldo_mn, saldo_me, total], ...]
        """
        tbl = self.wait.until(EC.presence_of_element_located(self.TBL_POSICION))
        trs = tbl.find_elements(By.CSS_SELECTOR, "tbody tr")

        out = []
        for tr in trs:
            tds = tr.find_elements(By.CSS_SELECTOR, "td")
            if len(tds) != 4:
                continue

            row = [" ".join(td.text.split()) for td in tds]
            # Filtrar header tipo "SALDOS / Saldo MN / ..."
            if row[0].upper() == "SALDOS":
                continue

            out.append(row)

        return out









main.py
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

    # (Si ya corregiste el zoom al 100% antes, déjalo como lo tienes)
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

    # === NUEVO FLUJO POST-LOGIN ===
    riesgos = RiesgosPage(driver)
    riesgos.open_modulo_deuda()
    riesgos.consultar_por_dni(DNI_CONSULTA)

    datos_deudor = riesgos.extract_datos_deudor()
    posicion = riesgos.extract_posicion_consolidada()

    ExcelExporter().export_deuda(datos_deudor, posicion, EXCEL_PATH)
    print(f"Excel generado en: {EXCEL_PATH}")
    print("Flujo completo")


if __name__ == "__main__":
    main()

