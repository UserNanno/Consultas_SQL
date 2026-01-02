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
                    "Concepto": cells[0],
                    "Saldo MN": cells[1],
                    "Saldo ME": cells[2],
                    "Total": cells[3],
                })

        return out

    def logout(self):
        self.wait.until(EC.element_to_be_clickable(self.LINK_SALIR)).click()
