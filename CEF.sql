# config/settings.py
from pathlib import Path
import tempfile

EDGE_EXE = r"C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe"
DEBUG_PORT = 9223

URL_LOGIN = "http://test.com"
URL_COPILOT = "https://m365.cloud.microsoft/chat/?auth=2"

USUARIO = "T10595"
CLAVE = "44445555"

TEMP_DIR = Path(tempfile.gettempdir()) / "PrismaProject"
IMG_PATH = TEMP_DIR / "captura.png"

















# infrastructure/edge_debug.py
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















# infrastructure/selenium_driver.py
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




















# pages/login_page.py
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

class LoginPage:

    def __init__(self, driver):
        self.driver = driver
        self.wait = WebDriverWait(driver, 30)

    def capture_image(self, img_path):
        img = self.wait.until(EC.presence_of_element_located((By.ID, "TestImgID")))
        img.screenshot(str(img_path))

    def fill_credentials(self, user, clave, captcha):
        self.driver.find_element(By.ID, "c_c_test").send_keys(captcha)
        self.driver.find_element(By.ID, "c_c_usuario").send_keys(user)

        for d in clave:
            key = self.wait.until(
                EC.element_to_be_clickable(
                    (By.XPATH, f"//ul[@id='ulKeypad']//li[normalize-space()='{d}']")
                )
            )
            key.click()

        self.driver.find_element(By.ID, "btnIngresar").click()














# services/copilot_service.py
from pages.copilot_page import CopilotPage

class CopilotService:

    def __init__(self, driver):
        self.page = CopilotPage(driver)

    def resolve_from_image(self, img_path):
        return self.page.ask_from_image(img_path)



















# main.py
from config.settings import *
from infrastructure.edge_debug import EdgeDebugLauncher
from infrastructure.selenium_driver import SeleniumDriverFactory
from pages.login_page import LoginPage
from services.copilot_service import CopilotService

def main():
    EdgeDebugLauncher().ensure_running()

    driver = SeleniumDriverFactory.create()
    driver.get(URL_LOGIN)

    login = LoginPage(driver)
    login.capture_image(IMG_PATH)

    driver.switch_to.new_window("tab")
    captcha = CopilotService(driver).resolve_from_image(IMG_PATH)

    driver.switch_to.window(driver.window_handles[0])
    login.fill_credentials(USUARIO, CLAVE, captcha)

    print("Flujo completo")

if __name__ == "__main__":
    main()
