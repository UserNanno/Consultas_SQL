from PIL import Image as PILImage
import math
import time

def screenshot_panel_body_stitched(self, out_path):
    """
    Captura el panel-body completo aunque tenga scroll interno,
    haciendo varios screenshots por scrollTop y uniendo con PIL.
    """
    panel = self.wait.until(EC.presence_of_element_located(self.PANEL_BODY))
    self.driver.execute_script("arguments[0].scrollIntoView({block:'start'});", panel)

    # Medidas del panel
    scroll_height = int(self.driver.execute_script("return arguments[0].scrollHeight;", panel))
    client_height = int(self.driver.execute_script("return arguments[0].clientHeight;", panel))

    # Si no hay scroll interno, screenshot normal
    if scroll_height <= client_height + 2:
        panel.screenshot(str(out_path))
        return

    # Cantidad de “pantallas” internas a capturar
    n = int(math.ceil(scroll_height / client_height))

    tmp_paths = []
    for i in range(n):
        top = i * client_height
        self.driver.execute_script("arguments[0].scrollTop = arguments[1];", panel, top)

        # dejar que renderice (importante para evitar cortes/blank)
        time.sleep(0.15)

        tmp = out_path.with_name(f"{out_path.stem}_part{i}{out_path.suffix}")
        panel.screenshot(str(tmp))
        tmp_paths.append(tmp)

    # Unir imágenes verticalmente
    imgs = [PILImage.open(p) for p in tmp_paths]
    widths = [im.size[0] for im in imgs]
    heights = [im.size[1] for im in imgs]

    W = max(widths)
    H = sum(heights)

    stitched = PILImage.new("RGB", (W, H), (255, 255, 255))
    y = 0
    for im in imgs:
        stitched.paste(im, (0, y))
        y += im.size[1]

    stitched.save(out_path)

    # limpiar temporales
    for im in imgs:
        try:
            im.close()
        except Exception:
            pass
    for p in tmp_paths:
        try:
            p.unlink()
        except Exception:
            pass
