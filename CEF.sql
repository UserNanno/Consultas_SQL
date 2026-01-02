from pathlib import Path
import tempfile

# ... lo tuyo (EDGE_EXE, DEBUG_PORT, URL_LOGIN, URL_COPILOT, USUARIO, CLAVE, IMG_PATH, etc.)

DNI_CONSULTA = "78801600"

TEMP_DIR = Path(tempfile.gettempdir()) / "PrismaProject"
TEMP_DIR.mkdir(parents=True, exist_ok=True)

EXCEL_PATH = TEMP_DIR / "reporte_deuda.xlsx"










from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from pages.base_page import BasePage

class HubPage(BasePage):
    # selector robusto: por el onclick que ya nos diste
    HUB_LINK = (By.CSS_SELECTOR, "a[onclick*=\"f_hub('/criesgos/criesgos/criesgos.jsp'\"]")

    def go_to_consulta(self):
        self.wait.until(EC.element_to_be_clickable(self.HUB_LINK)).click()
















from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select
from pages.base_page import BasePage

class ConsultaPage(BasePage):
    SELECT_TIPO_DOC = (By.ID, "as_tipo_doc")
    INPUT_DOC = (By.NAME, "as_doc_iden")
    BTN_CONSULTAR = (By.ID, "btnConsultar")

    # Tablas
    TABLAS_CRW = (By.CSS_SELECTOR, "table.Crw")

    # Logout
    LINK_SALIR = (By.CSS_SELECTOR, "a[href*='/criesgos/logout']")

    def consultar_dni(self, dni: str):
        # seleccionar DNI (value=11)
        sel = Select(self.wait.until(EC.presence_of_element_located(self.SELECT_TIPO_DOC)))
        sel.select_by_value("11")

        # input DNI
        inp = self.wait.until(EC.element_to_be_clickable(self.INPUT_DOC))
        inp.click()
        inp.clear()
        inp.send_keys(dni)

        # consultar
        self.wait.until(EC.element_to_be_clickable(self.BTN_CONSULTAR)).click()

        # esperar que carguen tablas resultado
        self.wait.until(lambda d: len(d.find_elements(*self.TABLAS_CRW)) >= 2)

    def extract_datos_deudor(self) -> dict:
        """
        Devuelve dict tipo:
        {
          "Documento": "DNI",
          "Número": "78801600",
          "Persona": "Natural",
          "Apellido Paterno": "CANECILLAS",
          ...
        }
        """
        tablas = self.driver.find_elements(*self.TABLAS_CRW)
        t1 = tablas[0]

        tds = t1.find_elements(By.CSS_SELECTOR, "tbody td")
        data = {}

        i = 0
        while i < len(tds) - 1:
            left = tds[i].text.strip()
            right = tds[i + 1].text.strip()

            # saltar celdas vacías/encabezados
            if left and right and left.lower() not in ("datos del deudor",):
                # limpiar posibles saltos de línea
                left = " ".join(left.split())
                right = " ".join(right.split())
                # Evitar meter cosas raras como "Distribución..." que no viene en formato label/value
                if len(left) <= 40:
                    data[left] = right

            i += 1

        # Si quieres quedarte SOLO con campos “bonitos”, puedes filtrar aquí.
        return data

    def extract_posicion_consolidada(self) -> list[dict]:
        """
        Devuelve lista:
        [
          {"Concepto":"Vigente","Saldo MN":"735","Saldo ME":"0","Total":"735"},
          ...
        ]
        """
        tablas = self.driver.find_elements(*self.TABLAS_CRW)
        t2 = tablas[1]

        rows = t2.find_elements(By.CSS_SELECTOR, "tbody tr")
        out = []

        for r in rows:
            cells = [c.text.strip() for c in r.find_elements(By.CSS_SELECTOR, "td")]
            cells = [" ".join(x.split()) for x in cells if x is not None]

            # fila header "SALDOS ..." => saltar
            if len(cells) == 4 and cells[0].upper() != "SALDOS":
                out.append({
                    "Concepto": cel
























 from openpyxl import Workbook
from openpyxl.styles import Font, Alignment
from openpyxl.utils import get_column_letter
from pathlib import Path

class ExcelService:
    def export(self, datos_deudor: dict, posicion: list[dict], excel_path: Path):
        wb = Workbook()

        # Sheet 1: Datos del Deudor
        ws1 = wb.active
        ws1.title = "DatosDeudor"
        ws1["A1"] = "Campo"
        ws1["B1"] = "Valor"
        ws1["A1"].font = ws1["B1"].font = Font(bold=True)
        ws1["A1"].alignment = ws1["B1"].alignment = Alignment(horizontal="center")

        row = 2
        for k, v in datos_deudor.items():
            ws1.cell(row=row, column=1, value=k)
            ws1.cell(row=row, column=2, value=v)
            row += 1

        ws1.column_dimensions["A"].width = 35
        ws1.column_dimensions["B"].width = 45

        # Sheet 2: Posición Consolidada
        ws2 = wb.create_sheet("PosicionConsolidada")
        headers = ["Concepto", "Saldo MN", "Saldo ME", "Total"]
        for col, h in enumerate(headers, start=1):
            c = ws2.cell(row=1, column=col, value=h)
            c.font = Font(bold=True)
            c.alignment = Alignment(horizontal="center")

        r = 2
        for item in posicion:
            ws2.cell(r, 1, item.get("Concepto"))
            ws2.cell(r, 2, item.get("Saldo MN"))
            ws2.cell(r, 3, item.get("Saldo ME"))
            ws2.cell(r, 4, item.get("Total"))
            r += 1

        for col in range(1, 5):
            ws2.column_dimensions[get_column_letter(col)].width = 22

        excel_path.parent.mkdir(parents=True, exist_ok=True)
        wb.save(str(excel_path))
















 from config.settings import *
from infrastructure.edge_debug import EdgeDebugLauncher
from infrastructure.selenium_driver import SeleniumDriverFactory

from pages.login_page import LoginPage
from pages.copilot_page import CopilotPage
from pages.hub_page import HubPage
from pages.consulta_page import ConsultaPage

from services.copilot_service import CopilotService
from services.excel_service import ExcelService

from utils.logging_utils import setup_logging
from utils.decorators import log_exceptions


@log_exceptions
def main():
    setup_logging()

    EdgeDebugLauncher().ensure_running()
    driver = SeleniumDriverFactory.create()

    driver.get(URL_LOGIN)

    # 1) Login con captcha (copilot)
    login_page = LoginPage(driver)
    login_page.capture_image(IMG_PATH)

    driver.switch_to.new_window("tab")
    copilot = CopilotService(CopilotPage(driver))
    captcha = copilot.resolve_captcha(IMG_PATH)

    # volver a tab principal y loguear
    driver.switch_to.window(driver.window_handles[0])
    login_page.fill_form(USUARIO, CLAVE, captcha)

    # 2) Click en el link del HUB
    hub = HubPage(driver)
    hub.go_to_consulta()

    # 3) Consultar DNI
    consulta = ConsultaPage(driver)
    consulta.consultar_dni(DNI_CONSULTA)

    # 4) Extraer tablas
    datos_deudor = consulta.extract_datos_deudor()
    posicion = consulta.extract_posicion_consolidada()

    # 5) Exportar Excel
    ExcelService().export(datos_deudor, posicion, EXCEL_PATH)
    print(f"Excel generado en: {EXCEL_PATH}")

    # 6) Logout
    consulta.logout()
    print("Sesión cerrada (Salir).")


if __name__ == "__main__":
    main()



















from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from pages.base_page import BasePage
import time

class LoginPage(BasePage):

    def capture_image(self, img_path):
        img = self.wait.until(EC.presence_of_element_located((By.ID, "CaptchaImgID")))
        img.screenshot(str(img_path))

    def fill_form(self, usuario, clave, captcha):
        captcha_inp = self.wait.until(EC.presence_of_element_located((By.ID, "c_c_captcha")))
        captcha_inp.clear()
        captcha_inp.send_keys(captcha)

        user_inp = self.wait.until(EC.presence_of_element_located((By.ID, "c_c_usuario")))
        user_inp.clear()
        user_inp.send_keys(usuario)

        self.wait.until(EC.presence_of_element_located((By.ID, "ulKeypad")))

        for d in clave:
            key = self.wait.until(
                EC.element_to_be_clickable(
                    (By.XPATH, f"//ul[@id='ulKeypad']//li[normalize-space()='{d}']")
                )
            )
            key.click()
            time.sleep(0.2)

        self.wait.until(EC.element_to_be_clickable((By.ID, "btnIngresar"))).click()
