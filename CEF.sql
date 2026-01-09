# pages/base_page.py
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException


class BasePage:
    def __init__(self, driver, timeout=30):
        self.driver = driver
        self.wait = WebDriverWait(driver, timeout)

    def accept_alert_if_present(self, timeout: float = 1.5) -> str | None:
        """
        Si hay un JS alert, lo acepta y devuelve el texto.
        Si no hay alert, devuelve None.
        """
        try:
            alert = WebDriverWait(self.driver, timeout).until(EC.alert_is_present())
        except TimeoutException:
            return None

        txt = None
        try:
            txt = (alert.text or "").strip()
        except Exception:
            txt = None

        try:
            alert.accept()
        except Exception:
            pass

        return txt or None
