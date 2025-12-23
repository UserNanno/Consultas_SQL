from __future__ import annotations

import os
import re
import time
import random
from typing import Dict, Any, Optional

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys

from selenium.webdriver.chrome.options import Options as ChromeOptions
from selenium.webdriver.chrome.service import Service as ChromeService

from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, StaleElementReferenceException


# =========================
# URL
# =========================
URL_SUNEDU = "https://enlinea.sunedu.gob.pe/"

# ✅ Ajusta tu chromedriver.exe
CHROMEDRIVER_PATH = r"D:\Datos de Usuarios\T72496\Downloads\chromedriver-win64\chromedriver-win64\chromedriver.exe"

# ✅ Perfil separado
PROFILE_SUNEDU_DIR = r"D:\selenium_profiles\sunedu"


# =========================
# Utilidades “humanas”
# =========================
def human_pause(a: float = 0.6, b: float = 1.8) -> None:
    time.sleep(random.uniform(a, b))


def type_like_human(el, text: str, min_delay: float = 0.03, max_delay: float = 0.10) -> None:
    el.clear()
    for ch in text:
        el.send_keys(ch)
        time.sleep(random.uniform(min_delay, max_delay))


# =========================
# Chrome driver (con perfil)
# =========================
def build_chrome_driver(user_data_dir: str, show_window: bool = True) -> webdriver.Chrome:
    os.makedirs(user_data_dir, exist_ok=True)

    opts = ChromeOptions()
    opts.add_argument("--disable-gpu")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-extensions")
    opts.add_argument("--disable-notifications")
    opts.add_argument("--disable-popup-blocking")
    opts.add_argument("--window-size=1200,900")

    # perfil separado
    opts.add_argument(f"--user-data-dir={user_data_dir}")
    opts.add_argument("--profile-directory=Default")

    if not show_window:
        opts.add_argument("--window-position=-32000,-32000")

    opts.set_capability("pageLoadStrategy", "eager")

    service = ChromeService(executable_path=CHROMEDRIVER_PATH)
    driver = webdriver.Chrome(service=service, options=opts)
    driver.set_page_load_timeout(35)
    driver.set_script_timeout(35)
    return driver


# =========================
# SUNEDU helpers
# =========================
def _safe_click(driver: webdriver.Chrome, el) -> None:
    try:
        driver.execute_script("arguments[0].scrollIntoView({block:'center'});", el)
        human_pause(0.2, 0.6)
        el.click()
    except Exception:
        # fallback JS click
        try:
            driver.execute_script("arguments[0].click();", el)
        except Exception:
            raise


def _find_service_button(driver: webdriver.Chrome):
    """
    Botón del servicio (el que pegaste) por aria-label.
    Si cambia, ajustamos este selector.
    """
    # aria-label exacto o parcial
    css = 'button[aria-label="SERVICE.MAS_VISITADOS.VERIFICA_INSCRITO.ARIA_LABEL"]'
    els = driver.find_elements(By.CSS_SELECTOR, css)
    if els:
        return els[0]

    # fallback: buscar por contains del aria-label
    els = driver.find_elements(By.CSS_SELECTOR, 'button[aria-label*="VERIFICA_INSCRITO"]')
    if els:
        return els[0]

    return None


def _find_submit_button_in_modal(driver: webdriver.Chrome):
    """
    Intenta ubicar un botón razonable dentro del popup/modal.
    PrimeNG suele renderizar <button> con textos.
    """
    candidates = driver.find_elements(By.CSS_SELECTOR, "button")
    pattern = re.compile(r"(buscar|consultar|verificar|aceptar|continuar|enviar)", re.IGNORECASE)

    for b in candidates:
        try:
            txt = (b.text or "").strip()
            aria = (b.get_attribute("aria-label") or "").strip()
            if pattern.search(txt) or pattern.search(aria):
                return b
        except Exception:
            continue

    return None


def _modal_root(driver: webdriver.Chrome):
    """
    En apps Angular/PrimeNG el popup suele estar en overlays.
    Usamos heurística: si los inputs están visibles, tomamos el ancestro común.
    """
    dni_inp = driver.find_element(By.ID, "DNI")
    # intenta subir a un contenedor cercano
    root = None
    try:
        root = driver.execute_script(
            """
            const el = arguments[0];
            function up(n){ return n ? n.parentElement : null; }
            let cur = el;
            for (let i=0; i<8; i++){
              cur = up(cur);
              if (!cur) break;
              // heurística: contenedor "dialog" / "modal" / "overlay" por clase
              const cls = (cur.className || '').toString().toLowerCase();
              if (cls.includes('dialog') || cls.includes('modal') || cls.includes('overlay') || cls.includes('p-dialog')) return cur;
            }
            return el.closest('form') || el.closest('div') || el;
            """,
            dni_inp,
        )
    except Exception:
        pass
    return root


# =========================
# SUNEDU consulta
# =========================
def consultar_sunedu_verifica_inscrito(
    driver: webdriver.Chrome,
    wait: WebDriverWait,
    dni: str,
    apellidos_nombres: str,
) -> Dict[str, Any]:
    driver.get(URL_SUNEDU)

    # espera inicial "light"
    human_pause(1.0, 2.0)

    # botón del servicio
    btn = wait.until(lambda d: _find_service_button(d))
    _safe_click(driver, btn)

    # espera popup inputs
    wait.until(EC.presence_of_element_located((By.ID, "DNI")))
    wait.until(EC.presence_of_element_located((By.ID, "apellidosNombres")))
    human_pause(0.6, 1.2)

    # llenar
    inp_dni = driver.find_element(By.ID, "DNI")
    type_like_human(inp_dni, dni, 0.02, 0.08)
    human_pause(0.3, 0.8)

    inp_nom = driver.find_element(By.ID, "apellidosNombres")
    type_like_human(inp_nom, apellidos_nombres, 0.02, 0.07)
    human_pause(0.4, 1.0)

    # intentar enviar: ENTER + click a botón
    try:
        inp_nom.send_keys(Keys.ENTER)
    except Exception:
        pass

    human_pause(0.4, 0.9)

    submit_btn = _find_submit_button_in_modal(driver)
    if submit_btn is not None:
        _safe_click(driver, submit_btn)
    else:
        # fallback: otro ENTER
        try:
            inp_nom.send_keys(Keys.ENTER)
        except Exception:
            pass

    # espera a que aparezca algún resultado (heurística)
    # Si no sabemos IDs/clases, capturamos el texto del modal tras unos segundos.
    human_pause(1.2, 2.2)

    # reintentos cortos para que el DOM se estabilice
    txt = ""
    for _ in range(6):
        try:
            root = _modal_root(driver)
            if root is not None:
                txt = (root.text or "").strip()
                # si ya cambió a algo con más contenido, salimos
                if len(txt) > 30:
                    break
        except StaleElementReferenceException:
            pass
        time.sleep(0.5)

    return {
        "dni": dni,
        "apellidos_nombres": apellidos_nombres,
        "raw_modal_text": txt,
        "nota": "Si quieres que extraiga campos específicos, pégame el HTML del resultado (o un ejemplo del texto) y lo parseo.",
    }


# =========================
# Orquestación
# =========================
def consulta_sunedu(dni: str, apellidos_nombres: str, show_windows: bool = True) -> Dict[str, Any]:
    salida: Dict[str, Any] = {"SUNEDU": {}}

    chrome = build_chrome_driver(user_data_dir=PROFILE_SUNEDU_DIR, show_window=show_windows)
    wait = WebDriverWait(chrome, 30)
    try:
        data = consultar_sunedu_verifica_inscrito(chrome, wait, dni, apellidos_nombres)
        salida["SUNEDU"] = {"RESULTADOS": data}
        return salida
    except Exception as e:
        salida["SUNEDU"] = {"ERROR": f"{type(e).__name__}: {e}"}
        return salida
    finally:
        try:
            chrome.quit()
        except Exception:
            pass


if __name__ == "__main__":
    dni = "78801600"
    apn = "CANECILLAS CONTRERAS JUAN MARIANO"

    resultado = consulta_sunedu(dni, apn, show_windows=True)

    print("\nSUNEDU\n")
    for k, v in resultado.get("SUNEDU", {}).items():
        print(f"{k}: {v}")
