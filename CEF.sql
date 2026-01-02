from selenium import webdriver
from selenium.webdriver.edge.options import Options
from config.settings import DEBUG_PORT

class SeleniumDriverFactory:
    WIDTH = 1357
    HEIGHT = 924

    @staticmethod
    def create():
        options = Options()
        options.add_experimental_option(
            "debuggerAddress", f"127.0.0.1:{DEBUG_PORT}"
        )

        driver = webdriver.Edge(options=options)

        # 1️⃣ Tamaño de ventana
        driver.set_window_size(
            SeleniumDriverFactory.WIDTH,
            SeleniumDriverFactory.HEIGHT
        )

        # 2️⃣ Forzar métricas (evita escalado raro)
        driver.execute_cdp_cmd(
            "Emulation.setDeviceMetricsOverride",
            {
                "width": SeleniumDriverFactory.WIDTH,
                "height": SeleniumDriverFactory.HEIGHT,
                "deviceScaleFactor": 1,
                "mobile": False
            }
        )

        # 3️⃣ 🔥 FORZAR ZOOM 100% (ESTE ERA EL PROBLEMA)
        driver.execute_cdp_cmd(
            "Emulation.setPageScaleFactor",
            {"pageScaleFactor": 1}
        )

        return driver
