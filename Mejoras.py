import os
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

IMG_PATH = r"D:\Datos de Usuarios\T72496\Desktop\MODELOS_RPTs\WebAutomatic\captura.png"

USUARIO = "TU_USUARIO"
CLAVE = "123456"  # solo números


# =========================
# COPILOT HELPERS
# =========================
def wait_send_enabled(driver, wait):
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
    print("No se pudo clicar Enviar tras reintentos:", repr(last_err))
    return False


def copilot_ask_from_image(driver, img_path):
    wait = WebDriverWait(driver, 60)

    driver.get(URL_COPILOT)

    # subir imagen
    file_input = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "input[type='file']")))
    file_input.send_keys(img_path)

    # esperar a que "Enviar" quede habilitado (a veces tarda por el adjunto)
    try:
        wait_send_enabled(driver, wait)
    except TimeoutException:
        # seguimos igual, pero probablemente todavía esté subiendo
        pass

    # escribir prompt
    box = wait.until(EC.element_to_be_clickable((By.ID, "m365-chat-editor-target-element")))
    box.click()

    prompt = "En una sola palabra dime el texto de la imagen"
    # intenta send_keys
    box.send_keys(Keys.CONTROL, "a")
    box.send_keys(prompt)
    # refuerzo JS
    set_contenteditable_text(driver, box, prompt)

    # enviar: Enter + fallback a botón
    sent = False
    try:
        ActionChains(driver).move_to_element(box).click(box).send_keys(Keys.ENTER).perform()
        sent = True
    except Exception:
        sent = False

    if not sent:
        click_send_with_retries(driver, wait, attempts=3)
    else:
        # si Enter "no pegó", a veces el botón sigue habilitado => click para asegurar
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

    # leer respuesta (genérico: último <p> visible con texto)
    def last_p_with_text(drv):
        ps = drv.find_elements(By.CSS_SELECTOR, "p")
        texts = [p.text.strip() for p in ps if p.is_displayed() and p.text.strip()]
        return texts[-1] if texts else None

    result = wait.until(lambda d: last_p_with_text(d))
    return result


# =========================
# MAIN
# =========================
def main():
    # Conectar al Chrome en DEBUG ya abierto en 9222
    opts = Options()
    opts.add_experimental_option("debuggerAddress", "127.0.0.1:9222")

    driver = webdriver.Chrome(service=Service(CHROMEDRIVER_PATH), options=opts)
    wait = WebDriverWait(driver, 30)

    # 1) Pestaña FORM (principal)
    driver.get(URL_LOGIN)
    form_handle = driver.current_window_handle

    # 2) Capturar imagen del formulario (imgID)
    img_el = wait.until(EC.presence_of_element_located((By.ID, "imgID")))
    os.makedirs(os.path.dirname(IMG_PATH), exist_ok=True)
    img_el.screenshot(IMG_PATH)
    print("Captura guardada:", IMG_PATH)

    # 3) Abrir pestaña nueva para Copilot (NO usar window.open)
    driver.switch_to.new_window('tab')
    copilot_handle = driver.current_window_handle

    # 4) Preguntar a Copilot con la imagen y obtener respuesta
    copilot_text = copilot_ask_from_image(driver, IMG_PATH)
    print("Copilot devolvió:", copilot_text)

    # 5) Cerrar pestaña Copilot y volver al formulario
    driver.close()
    driver.switch_to.window(form_handle)

    # 6) Pegar texto en el input c_c_test ANTES de ingresar
    inp_test = wait.until(EC.presence_of_element_located((By.ID, "c_c_test")))
    inp_test.clear()
    inp_test.send_keys(copilot_text)

    # 7) Login (tu lógica)
    inp_user = wait.until(EC.presence_of_element_located((By.ID, "c_c_usuario")))
    inp_user.clear()
    inp_user.send_keys(USUARIO)

    wait.until(EC.presence_of_element_located((By.ID, "ulKeypad")))

    # limpiar clave (si existe)
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

    print("✅ Flujo completo ejecutado.")
    input("ENTER para terminar (no cierro Chrome debug)...")

    # si quieres cerrar SOLO la sesión Selenium (no el chrome debug), puedes omitir quit()
    # driver.quit()


if __name__ == "__main__":
    main()
