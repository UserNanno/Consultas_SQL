from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import TimeoutException

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

    # Resultado 1: cerró sesión
    H1_OK = (By.XPATH, "//h1[contains(.,'Ha cerrado la sesión activa satisfactoriamente')]")

    # Resultado 2: no había sesiones activas (esto también es OK)
    NO_SESIONES_MSG = (
        By.XPATH,
        "//span[contains(@class,'ui-messages-error-detail') and contains(.,'No existen sesiones activas')]"
    )

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

    def _wait_outcome(self, timeout: int = 12) -> str:
        """
        Espera cualquiera de los 2 outcomes y retorna:
          - "CERRADA" o "NO_ACTIVAS"
        """
        w = WebDriverWait(self.driver, timeout)

        # esperamos el primero que aparezca
        try:
            w.until(lambda d: (
                len(d.find_elements(*self.H1_OK)) > 0
                or len(d.find_elements(*self.NO_SESIONES_MSG)) > 0
            ))
        except TimeoutException:
            raise TimeoutException("No se detectó resultado de cierre de sesión (ni OK ni 'No existen sesiones activas').")

        if len(self.driver.find_elements(*self.H1_OK)) > 0:
            return "CERRADA"
        return "NO_ACTIVAS"

    def cerrar_sesion(self, usuario: str, clave: str) -> str:
        """
        Retorna:
          - "CERRADA"   -> cerró una sesión activa
          - "NO_ACTIVAS"-> no existían sesiones activas (esperado)
        """
        # usuario
        inp_user = self.wait.until(EC.element_to_be_clickable(self.TXT_USUARIO))
        inp_user.click()
        inp_user.clear()
        inp_user.send_keys(usuario)

        # clave por keypad (numérica)
        pwd = (clave or "").strip()
        if not pwd:
            raise ValueError("Clave SBS vacía.")
        if not pwd.isdigit():
            raise ValueError("La clave SBS para 'Cerrar Sesiones' debe ser numérica (keypad).")

        # keypad
        self._open_keypad()
        self._clear()
        for ch in pwd:
            self._press_digit(ch)

        # continuar
        self.wait.until(EC.element_to_be_clickable(self.BTN_CONTINUAR)).click()

        # outcome (ambos se consideran OK)
        return self._wait_outcome(timeout=12)
