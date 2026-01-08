from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

from pages.base_page import BasePage
from config.settings import URL_SBS_CERRAR_SESIONES


class CerrarSesionesPage(BasePage):
    # Inputs
    TXT_USUARIO = (By.ID, "Formulario:txtCodUsuario")
    TXT_CLAVE = (By.ID, "Formulario:txtClave")
    BTN_CONTINUAR = (By.ID, "Formulario:btnContinuar")

    # Keypad
    KEYPAD_DIV = (By.ID, "keypad-div")
    KEYPAD_CLEAR = (By.CSS_SELECTOR, "#keypad-div .keypad-key.keypad-clear")  # Limpiar

    # Confirmación
    H1_OK = (By.XPATH, "//h1[contains(.,'Ha cerrado la sesión activa satisfactoriamente')]")

    def open(self):
        self.driver.get(URL_SBS_CERRAR_SESIONES)
        self.wait.until(EC.presence_of_element_located(self.TXT_USUARIO))

    def _open_keypad(self):
        self.wait.until(EC.element_to_be_clickable(self.TXT_CLAVE)).click()
        self.wait.until(EC.visibility_of_element_located(self.KEYPAD_DIV))

    def _clear(self):
        try:
            self.wait.until(EC.element_to_be_clickable(self.KEYPAD_CLEAR)).click()
        except Exception:
            pass

    def _press_digit(self, d: str):
        btn = (
            By.XPATH,
            f"//div[@id='keypad-div']//button[contains(@class,'keypad-key') and normalize-space(text())='{d}']"
        )
        self.wait.until(EC.element_to_be_clickable(btn)).click()

    def cerrar_sesion(self, usuario: str, clave: str, timeout_ok: int = 15):
        # usuario
        inp_user = self.wait.until(EC.element_to_be_clickable(self.TXT_USUARIO))
        inp_user.click()
        inp_user.clear()
        inp_user.send_keys(usuario)

        # clave por keypad
        pwd = (clave or "").strip()
        if not pwd:
            raise ValueError("Clave SBS vacía.")
        if not pwd.isdigit():
            raise ValueError("La clave SBS para 'Cerrar Sesiones' debe ser numérica (keypad).")

        self._open_keypad()
        self._clear()

        for ch in pwd:
            self._press_digit(ch)

        # continuar
        self.wait.until(EC.element_to_be_clickable(self.BTN_CONTINUAR)).click()

        WebDriverWait(self.driver, timeout_ok).until(
            EC.presence_of_element_located(self.H1_OK)
        )
