# pages/sbs/login_page.py
import time
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import TimeoutException

from pages.base_page import BasePage


class CaptchaIncorrectoError(Exception):
    pass


class LoginPage(BasePage):

    # Texto del error (tu HTML)
    ERR_TXT_1 = "La aplicación ha generado un error al validar el ingreso del usuario"
    ERR_TXT_2 = "El código ingresado, no coincide con el código mostrado en la imagen"

    def _reset_zoom(self):
        try:
            body = self.driver.find_element(By.TAG_NAME, "body")
            body.click()
            body.send_keys(Keys.CONTROL, "0")
        except Exception:
            pass

    def capture_image(self, img_path):
        self._reset_zoom()
        img_el = self.wait.until(EC.presence_of_element_located((By.ID, "CaptchaImgID")))
        img_el.screenshot(str(img_path))

    def fill_form(self, usuario, clave, captcha):
        inp_test = self.wait.until(EC.presence_of_element_located((By.ID, "c_c_captcha")))
        inp_test.clear()
        inp_test.send_keys(captcha)

        inp_user = self.wait.until(EC.presence_of_element_located((By.ID, "c_c_usuario")))
        inp_user.clear()
        inp_user.send_keys(usuario)

        self.wait.until(EC.presence_of_element_located((By.ID, "ulKeypad")))

        try:
            self.driver.find_element(By.CSS_SELECTOR, "#ulKeypad li.WEB_zonaIngresobtnBorrar").click()
        except Exception:
            pass

        for d in clave:
            tecla = self.wait.until(
                EC.element_to_be_clickable((By.XPATH, f"//ul[@id='ulKeypad']//li[normalize-space()='{d}']"))
            )
            tecla.click()
            time.sleep(0.2)

        btn = self.wait.until(EC.element_to_be_clickable((By.ID, "btnIngresar")))
        btn.click()

    def _is_captcha_error_page(self) -> bool:
        try:
            body_txt = (self.driver.find_element(By.TAG_NAME, "body").text or "")
            return (self.ERR_TXT_1 in body_txt) or (self.ERR_TXT_2 in body_txt)
        except Exception:
            return False

    def wait_login_outcome(self, success_locator, timeout: int = 15) -> str:
        """
        Retorna:
          - "SUCCESS" si aparece algo post-login
          - "CAPTCHA_ERROR" si aparece la página de error de captcha
        """
        w = WebDriverWait(self.driver, timeout)

        def _pred(d):
            if self._is_captcha_error_page():
                return "CAPTCHA_ERROR"
            try:
                d.find_element(*success_locator)
                return "SUCCESS"
            except Exception:
                return False

        try:
            return w.until(_pred)
        except TimeoutException:
            # ni éxito ni error -> trátalo como unknown (puedes reintentar)
            return "UNKNOWN"
