
from __future__ import annotations

import os
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

# ✅ Ajusta ruta a tu chromedriver.exe
CHROMEDRIVER_PATH = r"D:\Datos de Usuarios\T72496\Downloads\chromedriver-win64\chromedriver-win64\chromedriver.exe"

# ✅ Perfil separado (se crea solo)
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
    opts.add_argument("--blink-settings=imagesEnabled=false")
    opts.add_argument("--window-size=1200,800")

    # perfil separado
    opts.add_argument(f"--user-data-dir={user_data_dir}")
    opts.add_argument("--profile-directory=Default")

    if not show_window:
        opts.add_argument("--window-position=-32000,-32000")

    opts.set_capability("pageLoadStrategy", "eager")

    service = ChromeService(executable_path=CHROMEDRIVER_PATH)
    driver = webdriver.Chrome(service=service, options=opts)
    driver.set_page_load_timeout(30)
    driver.set_script_timeout(30)
    return driver


# =========================
# Helpers específicos SUNEDU
# =========================
def click_js(driver: webdriver.Chrome, el) -> None:
    driver.execute_script("arguments[0].click();", el)


def safe_find_first(driver: webdriver.Chrome, by: By, value: str):
    els = driver.find_elements(by, value)
    return els[0] if els else None


def wait_clickable(wait: WebDriverWait, locator: tuple) -> Any:
    return wait.until(EC.element_to_be_clickable(locator))


def wait_present(wait: WebDriverWait, locator: tuple) -> Any:
    return wait.until(EC.presence_of_element_located(locator))


def open_verifica_inscrito_modal(driver: webdriver.Chrome, wait: WebDriverWait) -> None:
    """
    Abre el modal emergente del botón "Verifica inscrito".
    Selector: botón con aria-label='SERVICE.MAS_VISITADOS.VERIFICA_INSCRITO.ARIA_LABEL'
    """
    # Asegurar carga básica
    wait_present(wait, (By.TAG_NAME, "body"))
    human_pause(0.8, 1.6)

    btn = wait_clickable(
        wait,
        (By.CSS_SELECTOR, "button[aria-label='SERVICE.MAS_VISITADOS.VERIFICA_INSCRITO.ARIA_LABEL']")
    )

    driver.execute_script("arguments[0].scrollIntoView({block:'center'});", btn)
    human_pause(0.2, 0.6)

    try:
        btn.click()
    except Exception:
        click_js(driver, btn)

    # Esperar que aparezcan los inputs del modal (con IDs conocidos)
    wait_present(wait, (By.ID, "DNI"))
    wait_present(wait, (By.ID, "apellidosNombres"))


def ensure_checkbox_checked(driver: webdriver.Chrome, wait: WebDriverWait) -> None:
    """
    Marca el checkbox del modal.

    Como solo viste:
        <input type="checkbox">
        <span class="cb-i"></span>

    Estrategia:
      1) Buscar un checkbox visible dentro del modal/página.
      2) Si no se puede click normal, click JS.
      3) Si el input está oculto y el click real es el <span.cb-i>, también lo intenta.
    """
    # Intento 1: input checkbox clickeable
    try:
        cb = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "input[type='checkbox']")))
        # si ya está marcado, no hacer nada
        try:
            if cb.is_selected():
                return
        except Exception:
            pass

        driver.execute_script("arguments[0].scrollIntoView({block:'center'});", cb)
        human_pause(0.2, 0.6)

        try:
            wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, "input[type='checkbox']")))
            cb.click()
        except Exception:
            click_js(driver, cb)

        human_pause(0.2, 0.6)

        # Verificación (si se puede)
        try:
            if cb.is_selected():
                return
        except Exception:
            # si no se puede leer, seguimos al plan B
            pass

    except TimeoutException:
        pass

    # Intento 2: click al "span.cb-i" (muchas UIs estilizan así)
    sp = safe_find_first(driver, By.CSS_SELECTOR, "span.cb-i")
    if sp:
        driver.execute_script("arguments[0].scrollIntoView({block:'center'});", sp)
        human_pause(0.2, 0.6)
        try:
            sp.click()
        except Exception:
            click_js(driver, sp)
        human_pause(0.2, 0.6)
        return

    raise RuntimeError("No pude encontrar/marcar el checkbox (input[type=checkbox] ni span.cb-i).")


def submit_modal(driver: webdriver.Chrome, wait: WebDriverWait) -> None:
    """
    Intenta ejecutar la búsqueda / verificación.
    Como no nos diste el botón exacto del modal, hacemos varias estrategias:
      - ENTER en el input de apellidosNombres
      - buscar un botón típico (Buscar/Consultar/Verificar) por texto
    """
    # ENTER
    try:
        apnom = driver.find_element(By.ID, "apellidosNombres")
        apnom.send_keys(Keys.ENTER)
        human_pause(0.4, 0.9)
    except Exception:
        pass

    # Botón por texto (fallback)
    # Nota: XPATH con contains(., ...) funciona con textos visibles. Si el botón no tiene texto, ignóralo.
    candidates_xpath = [
        "//button[contains(., 'Buscar')]",
        "//button[contains(., 'Consultar')]",
        "//button[contains(., 'Verificar')]",
        "//button[contains(., 'Verifica')]",
        "//button[contains(., 'Continuar')]",
        "//button[contains(., 'Aceptar')]",
    ]
    for xp in candidates_xpath:
        try:
            btns = driver.find_elements(By.XPATH, xp)
            if not btns:
                continue
            btn = btns[0]
            driver.execute_script("arguments[0].scrollIntoView({block:'center'});", btn)
            human_pause(0.2, 0.6)
            try:
                btn.click()
            except Exception:
                click_js(driver, btn)
            human_pause(0.5, 1.2)
            return
        except Exception:
            continue


def extract_result_text(driver: webdriver.Chrome) -> Dict[str, Any]:
    """
    Devuelve lo que se pueda de la pantalla como “resultado”.
    Como no tenemos el HTML exacto del panel final, capturamos:
      - algún contenedor típico de mensajes (toast/dialog) si existe
      - y un recorte del body (primeros caracteres) para debug
    """
    def first_text(css: str) -> str:
        el = safe_find_first(driver, By.CSS_SELECTOR, css)
        return el.text.strip() if el and el.text and el.text.strip() else ""

    # Ajusta/añade selectores si identificas el panel real luego
    possible = [
        "div.p-dialog-content",
        "div.p-dialog",
        "div[role='dialog']",
        "div.p-toast-detail",
        "div.p-toast-message",
        ".swal2-html-container",
        ".swal2-title",
        ".swal2-popup",
        "main",
    ]
    for css in possible:
        t = first_text(css)
        if t:
            return {"resultado_texto": t, "fuente": css}

    body = (driver.find_element(By.TAG_NAME, "body").text or "").strip()
    return {"resultado_texto": body[:1200], "fuente": "body[:1200]"}


# =========================
# Flujo principal SUNEDU
# =========================
def consultar_sunedu_verifica_inscrito(
    driver: webdriver.Chrome,
    wait: WebDriverWait,
    dni: str,
    apellidos_nombres: str,
) -> Dict[str, Any]:
    driver.get(URL_SUNEDU)

    open_verifica_inscrito_modal(driver, wait)

    # llenar inputs
    el_dni = wait_present(wait, (By.ID, "DNI"))
    type_like_human(el_dni, dni, 0.02, 0.07)
    human_pause(0.3, 0.8)

    el_apnom = wait_present(wait, (By.ID, "apellidosNombres"))
    type_like_human(el_apnom, apellidos_nombres, 0.02, 0.07)
    human_pause(0.3, 0.8)

    # checkbox (obligatorio)
    ensure_checkbox_checked(driver, wait)
    human_pause(0.3, 0.8)

    # enviar/consultar
    submit_modal(driver, wait)

    # esperar algo de “resultado” (ajusta si detectas un selector real)
    # aquí solo damos un margen y luego extraemos texto
    human_pause(1.2, 2.2)

    # intenta esperar a que cambie el contenido o aparezca un dialog/toast
    end = time.time() + 15
    last = ""
    while time.time() < end:
        cur = (driver.find_element(By.TAG_NAME, "body").text or "").strip()
        if cur and cur != last:
            # si aparece algo nuevo, rompemos (no perfecto, pero útil sin selector final)
            last = cur
        if safe_find_first(driver, By.CSS_SELECTOR, "div.p-dialog, div[role='dialog'], .swal2-popup, div.p-toast-message"):
            break
        time.sleep(0.4)

    out = {"dni": dni, "apellidos_nombres": apellidos_nombres}
    out.update(extract_result_text(driver))
    return out


# =========================
# Orquestación
# =========================
def consulta_sunedu(dni: str, apellidos_nombres: str, show_window: bool = True) -> Dict[str, Any]:
    salida: Dict[str, Any] = {"SUNEDU": {}}

    chrome = build_chrome_driver(user_data_dir=PROFILE_SUNEDU_DIR, show_window=show_window)
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
    DNI = "78801600"
    APELLIDOS_Y_NOMBRES = "CANECILLAS CONTRERAS JUAN MARIANO"

    resultado = consulta_sunedu(DNI, APELLIDOS_Y_NOMBRES, show_window=True)

    print("\nSUNEDU\n")
    for k, v in resultado.get("SUNEDU", {}).items():
        print(f"{k}: {v}\n")
