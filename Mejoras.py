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

URL_FORM = "https://test.com/app/login.jsp"
URL_COPILOT = "https://m365.cloud.microsoft/chat/?auth=2"

IMG_ELEMENT_ID = "imgID"  # elemento a capturar
IMG_PATH = r"D:\Datos de Usuarios\T72496\Desktop\MODELOS_RPTs\WebAutomatic\captura.png"

USUARIO = "TU_USUARIO"
CLAVE = "123456"  # solo números

PROMPT = "En una sola palabra dime el texto de la imagen"

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
    raise RuntimeError(f"No se pudo clicar Enviar tras reintentos: {repr(last_err)}")

def copilot_ask_image_and_get_text(driver, img_path, prompt):
    wait = WebDriverWait(driver, 60)

    driver.get(URL_COPILOT)

    # Espera input file y sube imagen
    file_input = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "input[type='file']")))
    file_input.send_keys(img_path)

    # Espera que Enviar se habilite (importante con adjuntos)
    try:
        wait_send_enabled(driver, wait)
    except TimeoutException:
        # Igual seguimos; a veces tarda pero luego habilita
        pass

    # Caja contenteditable
    box = wait.until(EC.element_to_be_clickable((By.ID, "m365-chat-editor-target-element")))
    box.click()

    # Guardar “estado anterior” para detectar respuesta nueva
    def snapshot_ps():
        ps = driver.find_elements(By.CSS_SELECTOR, "p")
        return [p.text.strip() for p in ps if p.is_displayed() and p.text and p.text.strip()]

    before = set(snapshot_ps())

    # Escribir prompt (send_keys + refuerzo JS)
    box.send_keys(Keys.CONTROL, "a")
    box.send_keys(prompt)
    set_contenteditable_text(driver, box, prompt)

    # Intento Enter; si no, click Enviar
    sent = False
    try:
        ActionChains(driver).move_to_element(box).click(box).send_keys(Keys.ENTER).perform()
        sent = True
    except Exception:
        sent = False

    # En Copilot, muchas veces Enter no manda con adjunto -> reforzar con botón
    if not sent:
        click_send_with_retries(driver, wait, attempts=3)
    else:
        time.sleep(0.8)
        # si sigue habilitado, probablemente no envió
        try:
            btn = driver.find_element(
                By.CSS_SELECTOR,
                "button[type='submit'][aria-label='Enviar'], button[type='submit'][title='Enviar']"
            )
            aria_disabled = (btn.get_attribute("aria-disabled") or "").lower()
            if btn.get_attribute("disabled") is None and aria_disabled != "true":
                click_send_with_retries(driver, wait, attempts=3)
        except Exception:
            pass

    # Esperar nueva respuesta: un <p> nuevo distinto a lo que ya había
    def new_text_only(d):
        now = [t for t in snapshot_ps() if t not in before]
        if not now:
            return None
        # filtra el prompt (para no devolverte tu propio texto)
        now = [t for t in now if t.strip() != prompt.strip()]
        return now[-1] if now else None

    result = wait.until(new_text_only)
    return result.strip()

# =========================
# FORM HELPERS
# =========================
def capture_element_screenshot(driver, element_id, out_path):
    wait = WebDriverWait(driver, 30)
    el = wait.until(EC.presence_of_element_located((By.ID, element_id)))
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    el.screenshot(out_path)

def do_login_with_keypad(driver, usuario, clave):
    wait = WebDriverWait(driver, 30)

    # Usuario
    inp_user = wait.until(EC.presence_of_element_located((By.ID, "c_c_usuario")))
    inp_user.clear()
    inp_user.send_keys(usuario)

    # Esperar keypad
    wait.until(EC.presence_of_element_located((By.ID, "ulKeypad")))

    # Limpiar clave (si existe)
    try:
        driver.find_element(By.CSS_SELECTOR, "#ulKeypad li.WEB_zonaIngresobtnBorrar").click()
    except Exception:
        pass

    # Ingresar clave con clicks
    for d in clave:
        tecla = wait.until(
            EC.element_to_be_clickable(
                (By.XPATH, f"//ul[@id='ulKeypad']//li[normalize-space()='{d}']")
            )
        )
        tecla.click()
        time.sleep(0.2)

    # ACEPTAR
    btn = wait.until(EC.element_to_be_clickable((By.ID, "btnIngresar")))
    btn.click()

# =========================
# MAIN
# =========================
def main():
    # 1) Adjuntarse a Chrome Debug
    options = Options()
    options.add_experimental_option("debuggerAddress", "127.0.0.1:9222")
    driver = webdriver.Chrome(service=Service(CHROMEDRIVER_PATH), options=options)
    wait = WebDriverWait(driver, 30)

    # 2) Abrir formulario en pestaña actual (TAB 1)
    driver.get(URL_FORM)
    form_handle = driver.current_window_handle

    # 3) Capturar imagen (desde el formulario)
    capture_element_screenshot(driver, IMG_ELEMENT_ID, IMG_PATH)
    print(f"Captura guardada: {IMG_PATH}")

    # 4) Abrir Copilot en NUEVA pestaña (TAB 2) de forma explícita
    driver.switch_to.window(form_handle)
    driver.execute_script("window.open('about:blank','_blank');")

    # Esperar 2 pestañas (evita el IndexError)
    wait.until(lambda d: len(d.window_handles) >= 2)

    handles = driver.window_handles
    copilot_handle = [h for h in handles if h != form_handle][-1]
    driver.switch_to.window(copilot_handle)

    # 5) Consultar Copilot y obtener respuesta
    copilot_text = copilot_ask_image_and_get_text(driver, IMG_PATH, PROMPT)
    print("Copilot devolvió:", copilot_text)

    # 6) Cerrar pestaña Copilot y volver al formulario
    driver.close()
    driver.switch_to.window(form_handle)

    # 7) Pegar en c_c_test ANTES de login (según tu flujo)
    inp_test = wait.until(EC.presence_of_element_located((By.ID, "c_c_test")))
    inp_test.clear()
    inp_test.send_keys(copilot_text)

    # 8) Ejecutar login
    do_login_with_keypad(driver, USUARIO, CLAVE)

    print("Listo. Revisa el navegador.")
    input("ENTER para terminar...")

    # NO cierres Chrome debug si lo quieres dejar abierto:
    # driver.quit()

if __name__ == "__main__":
    main()
