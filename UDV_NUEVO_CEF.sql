from pathlib import Path
from pages.rbm.rbm_page import RbmPage

class RbmFlow:
    def __init__(self, driver):
        self.page = RbmPage(driver)

    def run(self, dni: str, consumos_img_path: Path, cem_img_path: Path):
        self.page.open()

        self.page.consultar_dni(dni)
        self.page.screenshot_panel_body_stitched(consumos_img_path)

        self.page.go_cem_tab()
        self.page.screenshot_panel_body_stitched(cem_img_path)
