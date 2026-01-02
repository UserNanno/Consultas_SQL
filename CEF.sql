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
            "--start-maximized",

            # ✅ ayuda a evitar escalados raros (DPI/zoom)
            "--force-device-scale-factor=1",
            "--high-dpi-support=1",
        ], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

        if not self._wait_port("127.0.0.1", DEBUG_PORT):
            raise RuntimeError("Edge no abrió el puerto de debugging")























from selenium import webdriver
from selenium.webdriver.edge.options import Options
from selenium.webdriver.common.keys import Keys
from config.settings import DEBUG_PORT

class SeleniumDriverFactory:

    @staticmethod
    def create():
        options = Options()
        options.add_experimental_option("debuggerAddress", f"127.0.0.1:{DEBUG_PORT}")
        driver = webdriver.Edge(options=options)

        # ✅ lo que tenías antes (viewport fijo para evitar interferencias al screenshot)
        driver.set_window_size(1357, 924)
        driver.execute_cdp_cmd("Emulation.setDeviceMetricsOverride", {
            "width": 1357,
            "height": 924,
            "deviceScaleFactor": 1,
            "mobile": False
        })

        # ✅ fuerza zoom 100% (muy importante si el perfil quedó en 175%)
        SeleniumDriverFactory._reset_zoom_100(driver)

        return driver

    @staticmethod
    def _reset_zoom_100(driver):
        # Método 1 (más confiable): CTRL+0 sobre el <body>
        try:
            body = driver.find_element("tag name", "body")
            body.click()
            body.send_keys(Keys.CONTROL, "0")
        except Exception:
            # fallback: mandar la combinación al documento con ActionChains no siempre ayuda,
            # así que lo dejamos silencioso.
            pass

        # Método 2 (fallback CDP): setPageScaleFactor (si el navegador lo soporta)
        try:
            driver.execute_cdp_cmd("Emulation.setPageScaleFactor", {"pageScaleFactor": 1})
        except Exception:
            pass

        # Método 3 (último recurso): zoom CSS (afecta solo la página actual)
        try:
            driver.execute_script("document.body.style.zoom='100%';")
        except Exception:
            pass























from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.keys import Keys
from pages.base_page import BasePage
import time

class LoginPage(BasePage):

    def _reset_zoom(self):
        try:
            body = self.driver.find_element(By.TAG_NAME, "body")
            body.click()
            body.send_keys(Keys.CONTROL, "0")
        except Exception:
            pass

    def capture_image(self, img_path):
        self._reset_zoom()  # ✅ clave antes del screenshot
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
