import math
import time
from io import BytesIO
from PIL import Image as PILImage
from selenium.webdriver.support import expected_conditions as EC

def screenshot_panel_body_stitched_crop(self, out_path):
    """
    Stitch robusto:
    - scrollea el panel internamente
    - toma screenshot de toda la ventana
    - recorta el rect del panel (boundingClientRect) con DPR
    - une verticalmente sin espacios blancos ni recortes laterales
    """
    panel = self.wait.until(EC.presence_of_element_located(self.PANEL_BODY))

    # Asegura panel visible y al inicio
    self.driver.execute_script("arguments[0].scrollIntoView({block:'start'});", panel)
    self.driver.execute_script("arguments[0].scrollTop = 0; arguments[0].scrollLeft = 0;", panel)

    # Medidas internas del panel
    scroll_height = int(self.driver.execute_script("return arguments[0].scrollHeight;", panel))
    client_height = int(self.driver.execute_script("return arguments[0].clientHeight;", panel))

    # Si no hay scroll, recorte simple desde full screenshot (más consistente que panel.screenshot)
    if scroll_height <= client_height + 2:
        img = self._crop_element_from_viewport_screenshot(panel)
        img.save(out_path)
        return

    n = int(math.ceil(scroll_height / client_height))

    tiles = []
    for i in range(n):
        top = i * client_height
        self.driver.execute_script("arguments[0].scrollTop = arguments[1];", panel, top)

        # Dejar estabilizar layout/render (clave para evitar “cortes” y blur)
        time.sleep(0.15)

        tile = self._crop_element_from_viewport_screenshot(panel)
        tiles.append(tile)

    # Unir verticalmente
    W = tiles[0].size[0]
    H = sum(t.size[1] for t in tiles)
    stitched = PILImage.new("RGB", (W, H), (255, 255, 255))

    y = 0
    for t in tiles:
        stitched.paste(t, (0, y))
        y += t.size[1]

    stitched.save(out_path)


def _crop_element_from_viewport_screenshot(self, element):
    """
    Toma screenshot del viewport y recorta el rect del elemento usando
    boundingClientRect + devicePixelRatio.
    """
    # rect en CSS px
    rect = self.driver.execute_script(
        """
        const r = arguments[0].getBoundingClientRect();
        return {left:r.left, top:r.top, width:r.width, height:r.height};
        """,
        element
    )
    dpr = float(self.driver.execute_script("return window.devicePixelRatio || 1;"))

    png = self.driver.get_screenshot_as_png()
    im = PILImage.open(BytesIO(png)).convert("RGB")

    left = int(rect["left"] * dpr)
    top = int(rect["top"] * dpr)
    right = int((rect["left"] + rect["width"]) * dpr)
    bottom = int((rect["top"] + rect["height"]) * dpr)

    # clamp por seguridad
    left = max(0, left)
    top = max(0, top)
    right = min(im.size[0], right)
    bottom = min(im.size[1], bottom)

    return im.crop((left, top, right, bottom))
