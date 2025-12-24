import os
import re
import time

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.common.action_chains import ActionChains


# =========================
# CONFIG
# =========================
CHROMEDRIVER_PATH = r"D:\Datos de Usuarios\T72496\Downloads\chromedriver-win64\chromedriver-win64\chromedriver.exe"

URL_LOGIN = "https://test.com/app/login.jsp"
URL_COPILOT = "https://m365.cloud.microsoft/chat/?auth=2"

USUARIO = "TU_USUARIO"
CLAVE = "123456"  # solo números

CAPTURA_PATH = r"D:\Datos de Usuarios\T72496\Desktop\MODELOS_RPTs\WebAutomatic\captura.png"

PROMPT = "En una sola palabra dime el texto de la imagen"


# =========================
# HELPERS
# =========================
def wait_send_enabled(driver, wait):
    """Espera botón Enviar (Copilot) habilitado."""
    def _cond(d):
        try:
            btn = d.find_element(
                By.CSS_SELECTOR,
                "button[type='submit'][aria-label='Enviar'], button[type='submit'][title='Enviar']"
            )
            disabled_attr = btn.get_attribute("disabled")
            aria_disabled = (btn.get_attribute("aria-disabled") or "").lower()
            if disabled_attr is None and aria_disabled != "true":
                return btn
        except Exception:
            return None
        return None
    return wait.until(_cond)


def set_contenteditable_text(driver, element, text):
    """Escribe en contenteditable vía JS (más confiable)."""
    driver.execute_script(
        """
        const el = arguments[0];
        const txt = arguments[1];
        el.focus();
        el.innerText = txt;
        el.dispatchEvent(new InputEvent('input', { bubbles: true }));
        el.dispatchEvent(new Event('change', { bubbles: true }));
        """,
        element, text
    )


def click_send_with_retries(driver, wait, attempts=3):
    last_err = None
    for _ in range(attempts):
        try:
            btn = wait_send_enabled(driver, wait)
            try:
                btn.click()
            except Exception:
                driver.execute_script("arguments[0].click();", btn)
            return True
        except Exception as e:
            last_err = e
            time.sleep(0.6)
    print("No se pudo clicar Enviar:", repr(last_err))
    return False


def normalize_copilot_answer(text: str) -> str:
    """Deja la respuesta lo más limpia posible (una palabra, sin cosas raras)."""
    if not text:
        return ""
    t = text.strip()
    # Quitar caracteres invisibles comunes (ZWSP/ZWNBSP)
    t = t.replace("\u200b", "").replace("\ufeff", "").replace("\u200c", "").replace("\u200d", "")
    # Quedarnos con la primera "palabra" si devuelve frase
    # (letras/números/underscore con acentos también)
    m = re.findall(r"[0-9A-Za-zÁÉÍÓÚÜÑáéíóúüñ_]+", t)
    return m[0] if m else t


def get_last_visible_p_text(driver):
    ps = driver.find_elements(By.CSS_SELECTOR, "p")
    texts = [p.text.strip() for p in ps if p.is_displayed() and p.text.strip()]
    return texts[-1] if texts else None


# =========================
# MAIN
# =========================
def main():
    # 1) Conectarse a Chrome DEBUG ya abierto
    options = Options()
    options.add_experimental_option("debuggerAddress", "127.0.0.1:9222")

    driver = webdriver.Chrome(service=Service(CHROMEDRIVER_PATH), options=options)
    wait = WebDriverWait(driver, 40)

    # 2) TAB 1: Formulario
    driver.get(URL_LOGIN)
    form_handle = driver.current_window_handle

    # 2.1) Capturar imagen del elemento imgID
    #     (mejor que sleep fijo: esperamos que exista y sea visible)
    img_el = wait.until(EC.visibility_of_element_located((By.ID, "imgID")))
    os.makedirs(os.path.dirname(CAPTURA_PATH), exist_ok=True)
    img_el.screenshot(CAPTURA_PATH)
    print("Captura guardada:", CAPTURA_PATH)

    # 3) Abrir TAB 2: Copilot (EN OTRA PESTAÑA)
    driver.execute_script("window.open('about:blank','_blank');")
    handles = driver.window_handles
    copilot_handle = [h for h in handles if h != form_handle][-1]
    driver.switch_to.window(copilot_handle)

    driver.get(URL_COPILOT)

    # 3.1) Subir imagen en Copilot
    file_input = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "input[type='file']")))
    file_input.send_keys(CAPTURA_PATH)

    # 3.2) Esperar botón enviar habilitado (cuando adjunta, a veces tarda)
    try:
        wait_send_enabled(driver, wait)
    except TimeoutException:
        pass

    # 3.3) Escribir prompt
    box = wait.until(EC.element_to_be_clickable((By.ID, "m365-chat-editor-target-element")))
    box.click()
    # intento normal
    box.send_keys(Keys.CONTROL, "a")
    box.send_keys(PROMPT)
    # refuerzo JS
    set_contenteditable_text(driver, box, PROMPT)

    # 3.4) Enviar (enter + fallback botón)
    sent = False
    try:
        ActionChains(driver).move_to_element(box).click(box).send_keys(Keys.ENTER).perform()
        sent = True
    except Exception:
        sent = False

    if not sent:
        click_send_with_retries(driver, wait, attempts=3)
    else:
        # si por algo no envió, reforzamos
        time.sleep(0.8)
        try:
            btn = driver.find_element(
                By.CSS_SELECTOR,
                "button[type='submit'][aria-label='Enviar'], button[type='submit'][title='Enviar']"
            )
            aria_disabled = (btn.get_attribute("aria-disabled") or "").lower()
            if aria_disabled != "true" and btn.get_attribute("disabled") is None:
                click_send_with_retries(driver, wait, attempts=3)
        except Exception:
            pass

    # 3.5) Leer respuesta
    # Esperamos a que aparezca un <p> con texto (puede tardar)
    try:
        raw = WebDriverWait(driver, 60).until(lambda d: get_last_visible_p_text(d))
    except TimeoutException:
        raw = ""

    answer = normalize_copilot_answer(raw)
    if not answer:
        raise RuntimeError("No se pudo obtener respuesta de Copilot (texto vacío).")

    print("Respuesta Copilot (limpia):", answer)

    # 4) Cerrar tab Copilot y volver al formulario
    driver.close()
    driver.switch_to.window(form_handle)

    # 5) Escribir respuesta en c_c_test ANTES de continuar login
    inp_test = wait.until(EC.presence_of_element_located((By.ID, "c_c_test")))
    inp_test.clear()
    inp_test.send_keys(answer)

    # 6) LOGIN (usuario + keypad + aceptar)
    inp_user = wait.until(EC.presence_of_element_located((By.ID, "c_c_usuario")))
    inp_user.clear()
    inp_user.send_keys(USUARIO)

    wait.until(EC.presence_of_element_located((By.ID, "ulKeypad")))

    # Limpiar clave (si el botón existe)
    try:
        driver.find_element(By.CSS_SELECTOR, "#ulKeypad li.WEB_zonaIngresobtnBorrar").click()
    except Exception:
        pass

    for d in CLAVE:
        tecla = wait.until(
            EC.element_to_be_clickable(
                (By.XPATH, f"//ul[@id='ulKeypad']//li[normalize-space()='{d}']")
            )
        )
        tecla.click()
        time.sleep(0.2)

    btn = wait.until(EC.element_to_be_clickable((By.ID, "btnIngresar")))
    btn.click()

    print("Listo: pegó el texto y ejecutó el login.")

    # No cierres si estás en debug y quieres ver el resultado:
    input("ENTER para terminar (NO cierra Chrome debug, solo el script)...")

    # NO driver.quit() para no tumbar tu sesión debug


if __name__ == "__main__":
    main()
