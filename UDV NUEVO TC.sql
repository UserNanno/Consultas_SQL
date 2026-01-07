from __future__ import annotations

import base64
import time
from pathlib import Path

from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select

from pages.base_page import BasePage
from config.settings import URL_RBM


class RbmPage(BasePage):
    SELECT_TIPO_DOC = (By.ID, "CodTipoDocumento")
    INPUT_DOC = (By.ID, "CodDocumento")
    BTN_CONSULTAR = (By.ID, "btnConsultar")

    PANEL_BODY = (By.CSS_SELECTOR, "div.panel-body")

    TAB_CEM = (By.ID, "CEM-tab")
    TABPANE_CEM = (By.ID, "CEM")  # data-target="#CEM"

    def open(self):
        self.driver.get(URL_RBM)
        self.wait.until(EC.presence_of_element_located(self.SELECT_TIPO_DOC))

    # ----------------- helpers -----------------
    def wait_not_loading(self, timeout=40) -> bool:
        """
        RBM tiene overlay de carga:
          body.loading + .loadingmodal (capa blanca).
        Esperamos a que se quite la clase "loading" del body.
        """
        end = time.time() + timeout
        while time.time() < end:
            try:
                is_loading = self.driver.execute_script(
                    "return document.body && document.body.classList.contains('loading');"
                )
                if not is_loading:
                    return True
            except Exception:
                pass
            time.sleep(0.2)
        return False

    # ----------------- acciones -----------------
    def consultar_dni(self, dni: str):
        sel = Select(self.wait.until(EC.presence_of_element_located(self.SELECT_TIPO_DOC)))
        sel.select_by_value("1")  # DNI

        inp = self.wait.until(EC.element_to_be_clickable(self.INPUT_DOC))
        inp.click()
        inp.clear()
        inp.send_keys(dni)

        self.wait.until(EC.element_to_be_clickable(self.BTN_CONSULTAR)).click()
        self.wait.until(EC.visibility_of_element_located(self.PANEL_BODY))

        # importante para evitar captura con overlay
        self.wait_not_loading(timeout=40)

    def go_cem_tab(self):
        self.wait.until(EC.element_to_be_clickable(self.TAB_CEM)).click()

        # 1) tab seleccionado
        self.wait.until(
            lambda d: (d.find_element(*self.TAB_CEM).get_attribute("aria-selected") or "").lower() == "true"
        )

        # 2) panel activo/visible
        def cem_ready(d):
            pane = d.find_element(*self.TABPANE_CEM)
            cls = (pane.get_attribute("class") or "").lower()
            return ("active" in cls) and pane.is_displayed()

        self.wait.until(cem_ready)

        # 3) transición fade terminada (opacidad final)
        self.wait.until(
            lambda d: float(
                d.execute_script(
                    "return parseFloat(getComputedStyle(arguments[0]).opacity) || 1;",
                    d.find_element(*self.TABPANE_CEM),
                )
            )
            >= 0.99
        )

        self.wait.until(EC.visibility_of_element_located(self.PANEL_BODY))
        self.wait_not_loading(timeout=40)

    # ----------------- screenshots -----------------
    def screenshot_panel_body_full(self, out_path):
        """
        Captura el panel-body expandiendo height al scrollHeight.
        Útil cuando el contenido es estable (por ejemplo CEM después de esperar).
        """
        self.wait_not_loading(timeout=40)

        panel = self.wait.until(EC.presence_of_element_located(self.PANEL_BODY))

        self.driver.execute_script("arguments[0].scrollIntoView({block:'start'});", panel)

        self.driver.execute_script(
            """
            const el = arguments[0];
            el.__oldOverflow = el.style.overflow;
            el.__oldHeight = el.style.height;
            el.__oldMaxHeight = el.style.maxHeight;

            el.scrollTop = 0;
            el.scrollLeft = 0;

            const h = el.scrollHeight;
            el.style.overflow = 'visible';
            el.style.height = h + 'px';
            el.style.maxHeight = 'none';
            """,
            panel,
        )

        # repaint
        self.driver.execute_script(
            """
            return new Promise(resolve => {
              requestAnimationFrame(() => requestAnimationFrame(resolve));
            });
            """
        )

        Path(out_path).parent.mkdir(parents=True, exist_ok=True)
        panel.screenshot(str(out_path))

        # restore
        self.driver.execute_script(
            """
            const el = arguments[0];
            el.style.overflow = el.__oldOverflow || '';
            el.style.height = el.__oldHeight || '';
            el.style.maxHeight = el.__oldMaxHeight || '';
            """,
            panel,
        )

    def screenshot_panel_body_cdp(self, out_path):
        """
        Captura robusta con CDP clip (ideal para Consumos donde se cortaba abajo).
        Evita problemas de overlay/topbar/sidebar.
        """
        self.wait_not_loading(timeout=40)

        panel = self.wait.until(EC.presence_of_element_located(self.PANEL_BODY))
        self.driver.execute_script("arguments[0].scrollIntoView({block:'start'});", panel)
        time.sleep(0.25)

        # ocultar elementos fixed que estorban
        self.driver.execute_script(
            """
            window.__rbm_prev_vis = window.__rbm_prev_vis || {};
            const sels = ['.topbar', '.left.side-menu'];
            sels.forEach(sel => {
              const el = document.querySelector(sel);
              if (!el) return;
              window.__rbm_prev_vis[sel] = el.style.visibility;
              el.style.visibility = 'hidden';
            });
            """
        )

        try:
            m = self.driver.execute_script(
                """
                const el = arguments[0];
                const r = el.getBoundingClientRect();
                const dpr = window.devicePixelRatio || 1;
                const sx = window.scrollX || window.pageXOffset || 0;
                const sy = window.scrollY || window.pageYOffset || 0;

                return {
                  x: Math.max(0, r.left + sx),
                  y: Math.max(0, r.top + sy),
                  w: Math.max(1, r.width),
                  h: Math.max(1, r.height),
                  dpr: dpr
                };
                """,
                panel,
            )

            clip = {
                "x": m["x"] * m["dpr"],
                "y": m["y"] * m["dpr"],
                "width": m["w"] * m["dpr"],
                "height": m["h"] * m["dpr"],
                "scale": 1,
            }

            data = self.driver.execute_cdp_cmd(
                "Page.captureScreenshot",
                {
                    "format": "png",
                    "fromSurface": True,
                    "captureBeyondViewport": True,
                    "clip": clip,
                },
            )["data"]

            out_path = Path(out_path)
            out_path.parent.mkdir(parents=True, exist_ok=True)
            out_path.write_bytes(base64.b64decode(data))

        finally:
            self.driver.execute_script(
                """
                const prev = window.__rbm_prev_vis || {};
                Object.keys(prev).forEach(sel => {
                  const el = document.querySelector(sel);
                  if (!el) return;
                  el.style.visibility = prev[sel] || '';
                });
                """
            )










from pathlib import Path
from pages.rbm.rbm_page import RbmPage

class RbmFlow:
    def __init__(self, driver):
        self.page = RbmPage(driver)

    def run(self, dni: str, consumos_img_path: Path, cem_img_path: Path):
        self.page.open()

        self.page.consultar_dni(dni)
        # Consumos: robusto (evita corte)
        self.page.screenshot_panel_body_cdp(consumos_img_path)

        self.page.go_cem_tab()
        # CEM: tu full funciona bien
        self.page.screenshot_panel_body_full(cem_img_path)
