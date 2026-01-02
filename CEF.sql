Ahhh te estas equivocando creo mira lo que tengo actualmente es:

settings.py
from config.settings import *
from infrastructure.edge_debug import EdgeDebugLauncher
from infrastructure.selenium_driver import SeleniumDriverFactory
from pages.login_page import LoginPage
from pages.copilot_page import CopilotPage
from services.copilot_service import CopilotService
from utils.logging_utils import setup_logging
from utils.decorators import log_exceptions

@log_exceptions
def main():
    setup_logging()

    EdgeDebugLauncher().ensure_running()
    driver = SeleniumDriverFactory.create()

    driver.get(URL_LOGIN)

    login_page = LoginPage(driver)
    login_page.capture_image(IMG_PATH)

    driver.switch_to.new_window("tab")
    copilot = CopilotService(CopilotPage(driver))
    captcha = copilot.resolve_captcha(IMG_PATH)

    driver.switch_to.window(driver.window_handles[0])
    login_page.fill_form(USUARIO, CLAVE, captcha)

    print("Flujo completo")

if __name__ == "__main__":
    main()





infrastructure/edge_debug.py
import socket, time, subprocess, os
from pathlib import Path
from config.settings import EDGE_EXE, DEBUG_PORT

class EdgeDebugLauncher:

    def _wait_port(self, host, port, timeout=15):
        start = time.time()
        while time.time() - start < timeout:
            try:
                with socket.create_connection((host, port), timeout=1):
                    return True
            except OSError:
                time.sleep(0.2)
        return False

    def ensure_running(self):
        profile_dir = Path(os.environ["LOCALAPPDATA"]) / "PrismaProject" / "edge_profile"
        profile_dir.mkdir(parents=True, exist_ok=True)

        subprocess.Popen([
            EDGE_EXE,
            f"--remote-debugging-port={DEBUG_PORT}",
            f"--user-data-dir={profile_dir}",
            "--start-maximized"
        ])

        if not self._wait_port("127.0.0.1", DEBUG_PORT):
            raise RuntimeError("Edge no abrió el puerto de debugging")




infrastructure/selenium_driver.py
from selenium import webdriver
from selenium.webdriver.edge.options import Options
from config.settings import DEBUG_PORT

class SeleniumDriverFactory:

    @staticmethod
    def create():
        options = Options()
        options.add_experimental_option(
            "debuggerAddress", f"127.0.0.1:{DEBUG_PORT}"
        )
        return webdriver.Edge(options=options)







pages/base_page.py
from selenium.webdriver.support.ui import WebDriverWait

class BasePage:

    def __init__(self, driver, timeout=30):
        self.driver = driver
        self.wait = WebDriverWait(driver, timeout)



pages/copilot_page.py
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains
from selenium.common.exceptions import TimeoutException
import time

from pages.base_page import BasePage
from config.settings import URL_COPILOT

class CopilotPage(BasePage):

    def ask_from_image(self, img_path):
        self.driver.get(URL_COPILOT)

        file_input = self.wait.until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "input[type='file']"))
        )
        file_input.send_keys(str(img_path))

        box = self.wait.until(
            EC.element_to_be_clickable((By.ID, "m365-chat-editor-target-element"))
        )

        prompt = (
            "Lee el texto de la imagen y transcribe el extracto de Edipo Rey. "
            "Responde solo con los 4 caracteres."
        )

        box.click()
        box.send_keys(Keys.CONTROL, "a")
        box.send_keys(prompt)

        try:
            ActionChains(self.driver).send_keys(Keys.ENTER).perform()
        except Exception:
            pass

        def last_text(driver):
            ps = driver.find_elements(By.CSS_SELECTOR, "p")
            texts = [p.text.strip() for p in ps if p.is_displayed()]
            return texts[-1] if texts else None

        return self.wait.until(lambda d: last_text(d))





pages/login_page.py
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from pages.base_page import BasePage
import time

class LoginPage(BasePage):

    def capture_image(self, img_path):
        img = self.wait.until(EC.presence_of_element_located((By.ID, "CaptchaImgID")))
        img.screenshot(str(img_path))

    def fill_form(self, usuario, clave, captcha):
        self.driver.find_element(By.ID, "c_c_captcha").send_keys(captcha)
        self.driver.find_element(By.ID, "c_c_usuario").send_keys(usuario)

        for d in clave:
            key = self.wait.until(
                EC.element_to_be_clickable(
                    (By.XPATH, f"//ul[@id='ulKeypad']//li[normalize-space()='{d}']")
                )
            )
            key.click()
            time.sleep(0.2)

        self.driver.find_element(By.ID, "btnIngresar").click()








services/copilot_service.py
class CopilotService:

    def __init__(self, copilot_page):
        self.copilot_page = copilot_page

    def resolve_captcha(self, img_path):
        return self.copilot_page.ask_from_image(img_path)




service/image_service.py
vacio







utils/decorators.py
import logging
import traceback
from functools import wraps

def log_exceptions(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        try:
            return fn(*args, **kwargs)
        except Exception:
            logging.error("EXCEPCION:\n%s", traceback.format_exc())
            raise
    return wrapper





utils/logging_utils.py
import logging
import tempfile
from pathlib import Path

LOG_PATH = Path(tempfile.gettempdir()) / "prisma_selenium.log"

def setup_logging():
    logging.basicConfig(
        filename=str(LOG_PATH),
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s"
    )






main.py
from config.settings import *
from infrastructure.edge_debug import EdgeDebugLauncher
from infrastructure.selenium_driver import SeleniumDriverFactory
from pages.login_page import LoginPage
from pages.copilot_page import CopilotPage
from services.copilot_service import CopilotService
from utils.logging_utils import setup_logging
from utils.decorators import log_exceptions

@log_exceptions
def main():
    setup_logging()

    EdgeDebugLauncher().ensure_running()
    driver = SeleniumDriverFactory.create()

    driver.get(URL_LOGIN)

    login_page = LoginPage(driver)
    login_page.capture_image(IMG_PATH)

    driver.switch_to.new_window("tab")
    copilot = CopilotService(CopilotPage(driver))
    captcha = copilot.resolve_captcha(IMG_PATH)

    driver.switch_to.window(driver.window_handles[0])
    login_page.fill_form(USUARIO, CLAVE, captcha)

    print("Flujo completo")

if __name__ == "__main__":
    main()

