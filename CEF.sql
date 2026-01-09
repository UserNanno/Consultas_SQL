He agregado mas valores a DESPRODUCTO y deberiamos modificar la logica en rbm_page.py no? 

Esta es la nueva logica de los DESPRODUCTO cuando el PRODUCTO es CREDITO EFECTIVO
    
LIBRE DISPONIBILIDAD -> Venta CEF LD/RE
COMPRA DE DEUDA      -> Venta CEF CdD   
LD + CONSOLIDACION   -> Venta CEF LD/RE
LD + COMPRA DE DUDA Y/O CONSOLIDACION -> Venta CEF CdD

En pocas palabras, basta que haya compra de deuda en alguna variante y se toma compra de deuda

Que deberia cambiar en mi script actual de mis archivos? 

config/product_catalog.py
# PRODUCTO -> lista de DESPRODUCTO
PRODUCT_CATALOG = {
    "CREDITO EFECTIVO": [
        "LIBRE DISPONIBILIDAD",
        "COMPRA DE DEUDA",
        "LD + CONSOLIDACION",
        "LD + COMPRA DE DUDA Y/O CONSOLIDACION",
    ],
    "TARJETA DE CREDITO": [
        "TARJETA NUEVA",

    ],
}

def list_productos() -> list[str]:
    return sorted(PRODUCT_CATALOG.keys())

def list_desproductos(producto: str) -> list[str]:
    return PRODUCT_CATALOG.get(producto, [])



pages/rbm/rbm_page.py

from __future__ import annotations

import base64
import time
from pathlib import Path
import logging

from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select
from selenium.common.exceptions import TimeoutException

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
        try:
            self.wait.until(EC.presence_of_element_located(self.SELECT_TIPO_DOC))
        except TimeoutException:
            logging.warning(
                "[RBM] No cargó SELECT_TIPO_DOC | url=%s | title=%s",
                self.driver.current_url,
                self.driver.title,
            )
            raise

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
            lambda d: (d.find_element(*self.TAB_CEM).get_attribute("aria-selected") or "").lower()
            == "true"
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

    # --- selectores existentes ---
    INPUT_SEGMENTO_RIESGO = (By.ID, "SegmentoRiesgo")

    # ----------------- extracción de datos -----------------
    def _text_norm(self, s: str) -> str:
        return " ".join((s or "").split()).strip()

    def extract_segmento(self) -> str:
        """
        Encuentra el valor del campo 'Segmento' en el panel inicial.
        Estrategia: buscar el div.editor-label con texto 'Segmento' y tomar el siguiente editor-field.
        """
        self.wait_not_loading(timeout=40)
        el = self.driver.find_element(
            By.XPATH,
            "//div[contains(@class,'editor-label')][normalize-space()='Segmento']"
            "/following-sibling::div[contains(@class,'editor-field')][1]",
        )
        return self._text_norm(el.text)

    def extract_segmento_riesgo(self) -> str:
        """
        Toma el value del input hidden #SegmentoRiesgo (fuente más confiable que el texto del td).
        """
        self.wait_not_loading(timeout=40)
        el = self.wait.until(EC.presence_of_element_located(self.INPUT_SEGMENTO_RIESGO))
        return self._text_norm(el.get_attribute("value"))

    def extract_situacion_laboral_badge(self) -> str:
        """
        Devuelve el texto del badge asociado a 'Situación Laboral'.
        """
        self.wait_not_loading(timeout=40)
        span = self.driver.find_element(
            By.XPATH,
            "//li[contains(@class,'list-group-item')][.//div[normalize-space()='Situación Laboral']]"
            "//span[contains(@class,'badge')]",
        )
        return self._text_norm(span.text)

    def extract_score_rcc(self) -> str:
        """
        Devuelve el valor del badge para 'Score RCC'.
        """
        self.wait_not_loading(timeout=40)
        span = self.driver.find_element(
            By.XPATH,
            "//li[contains(@class,'list-group-item')][contains(normalize-space(.),'Score RCC')]"
            "//span[contains(@class,'badge')]",
        )
        return self._text_norm(span.text)

    def extract_inicio_fields(self) -> dict:
        """
        Extrae todos los campos requeridos para la hoja 'Inicio':
        - segmento (ej ENALTA)
        - segmento_riesgo (ej A)
        - situacion_laboral_raw (ej 'No PdH – Dependiente' o 'PDH')
        - pdh (Si/No) => Si solo si situacion_laboral_raw == 'PDH'
        - score_rcc (ej 289)
        """
        self.wait_not_loading(timeout=40)
        segmento = self.extract_segmento()
        segmento_riesgo = self.extract_segmento_riesgo()
        sit_lab = self.extract_situacion_laboral_badge()
        pdh = "Si" if sit_lab == "PDH" else "No"
        score_rcc = self.extract_score_rcc()
        data = {
            "segmento": segmento,
            "segmento_riesgo": segmento_riesgo,
            "situacion_laboral_raw": sit_lab,
            "pdh": pdh,
            "score_rcc": score_rcc,
        }
        logging.info("RBM extract_inicio_fields: %s", data)
        return data

    def _num_from_value(self, raw: str) -> int:
        """
        Convierte textos como '-', '', '3,581', ' 212 ' a int.
        Regla: '-' o vacío => 0
        """
        s = (raw or "").strip()
        if not s or s == "-":
            return 0
        s = s.replace(",", "")  # 3,581 -> 3581
        try:
            return int(float(s))
        except Exception:
            return 0

    def _get_input_value_int(self, input_id: str) -> int:
        """
        Lee value de un input por id. Si no existe, devuelve 0.
        """
        try:
            el = self.driver.find_element(By.ID, input_id)
            return self._num_from_value(el.get_attribute("value"))
        except Exception:
            return 0

    def extract_cem_3cols(self) -> dict:
        """
        Extrae (solo) 3 columnas del CEM por producto:
        - cuota_bcp (BCP - Cuota)
        - cuota_sbs (SBS No BCP - Cuota)
        - saldo_sbs (SBS No BCP - Saldo)
        Usa inputs hidden por ID (más robusto que leer spans '-').
        Nota: 'Linea No Utilizada' usa otros IDs (ver mapeo).
        """
        self.wait_not_loading(timeout=40)
        data = {
            "hipotecario": {
                "cuota_bcp": self._get_input_value_int("CuotaHipotecarioBCP"),
                "cuota_sbs": self._get_input_value_int("CuotaHipotecarioNoBCP"),
                "saldo_sbs": self._get_input_value_int("SaldoHipotecarioNoBCP"),
            },
            "cef": {
                "cuota_bcp": self._get_input_value_int("CuotaCEFBCP"),
                "cuota_sbs": self._get_input_value_int("CuotaCEFNoBCP"),
                "saldo_sbs": self._get_input_value_int("SaldoCEFNoBCP"),
            },
            "vehicular": {
                "cuota_bcp": self._get_input_value_int("CuotaCVBCP"),
                "cuota_sbs": self._get_input_value_int("CuotaCVNoBCP"),
                "saldo_sbs": self._get_input_value_int("SaldoCVNoBCP"),
            },
            "pyme": {
                "cuota_bcp": self._get_input_value_int("CuotaPymeBCP"),
                "cuota_sbs": self._get_input_value_int("CuotaPymeNoBCP"),
                "saldo_sbs": self._get_input_value_int("SaldoPymeNoBCP"),
            },
            "comercial": {
                "cuota_bcp": self._get_input_value_int("CuotaComercialBCP"),
                "cuota_sbs": self._get_input_value_int("CuotaComercialNoBCP"),
                "saldo_sbs": self._get_input_value_int("SaldoComercialNoBCP"),
            },
            "deuda_indirecta": {
                # En tu HTML SBS(NoBCP) Cuota está como CuotaIndRCC y saldo como SaldoIndNoBCP
                "cuota_bcp": self._get_input_value_int("CuotaIndBCP"),
                "cuota_sbs": self._get_input_value_int("CuotaIndRCC"),
                "saldo_sbs": self._get_input_value_int("SaldoIndNoBCP"),
            },
            "tarjeta": {
                "cuota_bcp": self._get_input_value_int("CuotaTarjetaBCP"),
                "cuota_sbs": self._get_input_value_int("CuotaTarjetaNoBCP"),
                "saldo_sbs": self._get_input_value_int("SaldoTarjetaNoBCP"),
            },
            "linea_no_utilizada": {
                # Regla del HTML:
                # - BCP cuota: CuotaLineaDisponibleBCP (input text readonly, value="3")
                # - SBS cuota: CuotaLineaDisponibleNoBCP (hidden, value="17")
                # - SBS saldo: LineaDisponibleNoBCP (hidden, value="6308")  <-- saldo muestra la línea no utilizada
                "cuota_bcp": self._get_input_value_int("CuotaLineaDisponibleBCP"),
                "cuota_sbs": self._get_input_value_int("CuotaLineaDisponibleNoBCP"),
                "saldo_sbs": self._get_input_value_int("LineaDisponibleNoBCP"),
            },
        }
        logging.info("RBM extract_cem_3cols: %s", data)
        return data

    # ===================== NUEVO: extracción de scores Consumos según PRODUCTO/DESPRODUCTO =====================

    def _xpath_literal(self, s: str) -> str:
        """Escapa string para usarlo como literal en XPath."""
        if "'" not in s:
            return f"'{s}'"
        if '"' not in s:
            return f'"{s}"'
        parts = s.split("'")
        return "concat(" + ", \"'\", ".join([f"'{p}'" for p in parts]) + ")"

    def extract_badge_by_label_contains(self, label_text: str) -> str:
        """
        Busca un list-group-item que contenga label_text y devuelve el número del badge-pill asociado.
        Ej: 'Venta CEF LD/RE' -> '252'
        """
        self.wait_not_loading(timeout=40)
        label_text = self._text_norm(label_text)

        xpath = (
            "//li[contains(@class,'list-group-item')]"
            f"[.//div[contains(normalize-space(.), {self._xpath_literal(label_text)})]]"
            "//span[contains(@class,'badge') and contains(@class,'badge-pill')]"
        )
        el = self.wait.until(EC.presence_of_element_located((By.XPATH, xpath)))
        return self._text_norm(el.text)

    def extract_scores_por_producto(self, producto: str, desproducto: str | None = None) -> dict:
        """
        Reglas:
        - Si producto == 'CREDITO EFECTIVO':
            - C14 depende de DESPRODUCTO:
                * 'LD/RE' -> 'Venta CEF LD/RE'
                * 'COMPRA DEUDA' -> 'Venta CEF CdD'
            - C83 siempre: 'Portafolio CEF (Score BHV)'
        - Si producto == 'TARJETA DE CREDITO':
            - C14: 'Venta TC Nueva'
            - C83: None
        """
        self.wait_not_loading(timeout=40)

        p = (producto or "").strip().upper()
        d = (desproducto or "").strip().upper()

        out = {"inicio_c14": None, "inicio_c83": None}

        if p == "CREDITO EFECTIVO":
            if d == "LD/RE":
                out["inicio_c14"] = self.extract_badge_by_label_contains("Venta CEF LD/RE")
            elif d == "COMPRA DEUDA":
                out["inicio_c14"] = self.extract_badge_by_label_contains("Venta CEF CdD")
            else:
                out["inicio_c14"] = None

            out["inicio_c83"] = self.extract_badge_by_label_contains("Portafolio CEF (Score BHV)")

        elif p == "TARJETA DE CREDITO":
            out["inicio_c14"] = self.extract_badge_by_label_contains("Venta TC Nueva")
            out["inicio_c83"] = None

        logging.info("RBM extract_scores_por_producto: %s", out)
        return out

