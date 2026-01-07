
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select

from pages.base_page import BasePage
from config.settings import URL_RBM
from PIL import Image as PILImage
import math
import time


class RbmPage(BasePage):
    SELECT_TIPO_DOC = (By.ID, "CodTipoDocumento")
    INPUT_DOC = (By.ID, "CodDocumento")
    BTN_CONSULTAR = (By.ID, "btnConsultar")

    PANEL_BODY = (By.CSS_SELECTOR, "div.panel-body")

    TAB_CEM = (By.ID, "CEM-tab")
    # el tab-pane objetivo por data-target="#CEM"
    TABPANE_CEM = (By.ID, "CEM")

    def open(self):
        self.driver.get(URL_RBM)
        self.wait.until(EC.presence_of_element_located(self.SELECT_TIPO_DOC))

    def consultar_dni(self, dni: str):
        sel = Select(self.wait.until(EC.presence_of_element_located(self.SELECT_TIPO_DOC)))
        sel.select_by_value("1")  # DNI

        inp = self.wait.until(EC.element_to_be_clickable(self.INPUT_DOC))
        inp.click()
        inp.clear()
        inp.send_keys(dni)

        self.wait.until(EC.element_to_be_clickable(self.BTN_CONSULTAR)).click()
        self.wait.until(EC.visibility_of_element_located(self.PANEL_BODY))

    def go_cem_tab(self):
        self.wait.until(EC.element_to_be_clickable(self.TAB_CEM)).click()

        # 1) Espera a que el tab esté seleccionado
        self.wait.until(lambda d: (d.find_element(*self.TAB_CEM).get_attribute("aria-selected") or "").lower() == "true")

        # 2) Espera a que el panel CEM esté activo/visible (bootstrap suele usar active/show)
        def cem_ready(d):
            pane = d.find_element(*self.TABPANE_CEM)
            cls = (pane.get_attribute("class") or "").lower()
            # algunos usan "active show", otros solo "active"
            return ("active" in cls) and (pane.is_displayed())

        self.wait.until(cem_ready)

        # 3) Espera a que la opacidad final sea 1 (por si hay transición fade)
        self.wait.until(lambda d: float(d.execute_script(
            "return parseFloat(getComputedStyle(arguments[0]).opacity) || 1;",
            d.find_element(*self.TABPANE_CEM)
        )) >= 0.99)

        self.wait.until(EC.visibility_of_element_located(self.PANEL_BODY))

    def screenshot_panel_body_full(self, out_path):
        """
        Captura TODO el contenido del panel-body evitando cortes arriba/abajo:
        - fuerza scrollTop=0
        - expande height a scrollHeight
        - overflow visible
        - espera repaint
        - restaura estilos
        """
        panel = self.wait.until(EC.presence_of_element_located(self.PANEL_BODY))

        # asegurar que el panel esté en el viewport
        self.driver.execute_script("arguments[0].scrollIntoView({block:'start'});", panel)

        # expandir y normalizar scroll interno
        self.driver.execute_script(
            """
            const el = arguments[0];

            // guardar estilos previos
            el.__oldOverflow = el.style.overflow;
            el.__oldHeight = el.style.height;
            el.__oldMaxHeight = el.style.maxHeight;

            // ir al inicio del scroll interno (evita corte arriba)
            el.scrollTop = 0;

            // expandir para que quepa todo el contenido
            const h = el.scrollHeight;
            el.style.overflow = 'visible';
            el.style.height = h + 'px';
            el.style.maxHeight = 'none';
            """,
            panel
        )

        # esperar 2 frames (repaint) para que el layout se estabilice
        self.driver.execute_script(
            """
            return new Promise(resolve => {
              requestAnimationFrame(() => requestAnimationFrame(resolve));
            });
            """
        )

        panel.screenshot(str(out_path))

        # restaurar
        self.driver.execute_script(
            """
            const el = arguments[0];
            el.style.overflow = el.__oldOverflow || '';
            el.style.height = el.__oldHeight || '';
            el.style.maxHeight = el.__oldMaxHeight || '';
            """,
            panel
        )

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
