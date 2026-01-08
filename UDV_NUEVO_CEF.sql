URL_SBS_CERRAR_SESIONES = "https://extranet.sbs.gob.pe/CambioClave/pages/cerrarSesiones.jsf"


sbs_cerrar_sesiones_page.py

from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

from pages.base_page import BasePage
from config.settings import URL_SBS_CERRAR_SESIONES


class SbsCerrarSesionesPage(BasePage):
    # Inputs
    TXT_USUARIO = (By.ID, "Formulario:txtCodUsuario")
    TXT_CLAVE = (By.ID, "Formulario:txtClave")
    BTN_CONTINUAR = (By.ID, "Formulario:btnContinuar")

    # Keypad
    KEYPAD_DIV = (By.ID, "keypad-div")
    KEYPAD_CLEAR = (By.CSS_SELECTOR, "#keypad-div .keypad-key.keypad-clear")  # "Limpiar"

    # Resultado OK
    H1_OK = (By.XPATH, "//h1[contains(.,'Ha cerrado la sesión activa satisfactoriamente')]")

    def open(self):
        self.driver.get(URL_SBS_CERRAR_SESIONES)
        self.wait.until(EC.presence_of_element_located(self.TXT_USUARIO))

    def _open_keypad(self):
        # Click en el input (es readonly y abre keypad)
        self.wait.until(EC.element_to_be_clickable(self.TXT_CLAVE)).click()
        self.wait.until(EC.visibility_of_element_located(self.KEYPAD_DIV))

    def _clear_keypad(self):
        # Click Limpiar si existe/visible (buena práctica)
        try:
            self.wait.until(EC.element_to_be_clickable(self.KEYPAD_CLEAR)).click()
        except Exception:
            # Si no está, no pasa nada
            pass

    def _press_digit(self, digit: str):
        # Botón por texto exacto dentro del keypad
        btn = (By.XPATH, f"//div[@id='keypad-div']//button[contains(@class,'keypad-key') and normalize-space(text())='{digit}']")
        self.wait.until(EC.element_to_be_clickable(btn)).click()

    def cerrar_sesion(self, user: str, password: str, timeout_ok: int = 15) -> None:
        """
        Ingresa usuario + clave via keypad y presiona Continuar.
        Espera la confirmación de cierre de sesión.
        """
        # Usuario
        usuario = self.wait.until(EC.element_to_be_clickable(self.TXT_USUARIO))
        usuario.click()
        usuario.clear()
        usuario.send_keys(user)

        # Keypad
        self._open_keypad()
        self._clear_keypad()

        # Password por dígitos
        pwd = (password or "").strip()
        if not pwd:
            raise ValueError("Password SBS vacío. No se puede cerrar sesión previa.")

        if not pwd.isdigit():
            raise ValueError("La clave SBS para 'Cerrar Sesiones' debe ser numérica (keypad).")

        for ch in pwd:
            self._press_digit(ch)

        # Continuar
        self.wait.until(EC.element_to_be_clickable(self.BTN_CONTINUAR)).click()

        # Confirmación
        WebDriverWait(self.driver, timeout_ok).until(
            EC.presence_of_element_located(self.H1_OK)
        )
