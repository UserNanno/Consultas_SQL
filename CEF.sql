# pages/riesgos_page.py
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select
from pages.base_page import BasePage


class RiesgosPage(BasePage):

    def click_link_situacion_deuda(self):
        """
        Click en el link:
        'Aquí Ud. podrá consultar la situación de deuda...'
        """
        link = self.wait.until(
            EC.element_to_be_clickable(
                (By.XPATH, "//a[contains(text(),'consultar la situación de deuda')]")
            )
        )
        link.click()

    def seleccionar_tipo_documento_dni(self):
        """
        Selecciona '1. LE/DNI' en el combo
        """
        select = self.wait.until(
            EC.presence_of_element_located((By.ID, "as_tipo_doc"))
        )
        Select(select).select_by_value("11")  # DNI

    def ingresar_dni(self, dni: str):
        """
        Escribe el DNI
        """
        input_dni = self.wait.until(
            EC.element_to_be_clickable((By.NAME, "as_doc_iden"))
        )
        input_dni.clear()
        input_dni.send_keys(dni)

    def click_consultar(self):
        """
        Click en botón Consultar
        """
        btn = self.wait.until(
            EC.element_to_be_clickable((By.ID, "btnConsultar"))
        )
        btn.click()












from pages.riesgos_page import RiesgosPage



# === DESPUÉS DEL LOGIN ===
riesgos_page = RiesgosPage(driver)

# 1) Click en el link de situación de deuda
riesgos_page.click_link_situacion_deuda()

# 2) Seleccionar DNI
riesgos_page.seleccionar_tipo_documento_dni()

# 3) Escribir DNI
riesgos_page.ingresar_dni("78801600")

# 4) Click en Consultar
riesgos_page.click_consultar()










login_page.fill_form(USUARIO, CLAVE, captcha)

riesgos_page = RiesgosPage(driver)
riesgos_page.click_link_situacion_deuda()
riesgos_page.seleccionar_tipo_documento_dni()
riesgos_page.ingresar_dni("78801600")
riesgos_page.click_consultar()

print("Flujo completo")
