from selenium import webdriver
from selenium.webdriver.edge.options import Options
from config.settings import DEBUG_PORT

class SeleniumDriverFactory:
    DEFAULT_WIDTH = 1357
    DEFAULT_HEIGHT = 924

    @staticmethod
    def create(width: int = None, height: int = None):
        width = width or SeleniumDriverFactory.DEFAULT_WIDTH
        height = height or SeleniumDriverFactory.DEFAULT_HEIGHT

        options = Options()
        options.add_experimental_option("debuggerAddress", f"127.0.0.1:{DEBUG_PORT}")

        driver = webdriver.Edge(options=options)

        # 🔧 Esto es lo que tenías en el monolítico: evita que zoom/escala afecte el screenshot
        driver.set_window_size(width, height)
        driver.execute_cdp_cmd("Emulation.setDeviceMetricsOverride", {
            "width": width,
            "height": height,
            "deviceScaleFactor": 1,
            "mobile": False
        })

        return driver
