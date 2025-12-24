import os
import time
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
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
# HELPERS COPILOT
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
    raise last_err

def read_last_visible_p_text(driver):
    ps = driver.find_elements(By.CSS_SELECTOR, "p")
    texts = [p.text.strip() for p in ps if p.is_displayed() and p.text.strip()]
    return texts[-1] if texts else None

def get_copilot_answer(driver, wait, img_path, prompt):
    driver.get(URL_COPILOT)

    # Subir imagen
    file_input = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "input[type='file']")))
    file_input.send_keys(img_path)

    # Esperar a que enviar se habilite (adjunto puede tardar)
    try:
        wait_send_enabled(driver, wait)
    except TimeoutException:
        pass

    # Escribir prompt
    box = wait.until(EC.element_to_be_clickable((By.ID, "m365-chat-editor-target-element")))
    box.click()

    # Limpia y escribe
    box.send_keys(Keys.CONTROL, "a")
    box.send_keys(prompt)
    set_contenteditable_text(driver, box, prompt)

    # Intento Enter
    sent = False
    try:
        ActionChains(driver).move_to_element(box).click(box).send_keys(Keys.ENTER).perform()
        sent = True
    except Exception:
        sent = False

    # Si Enter no envía, click en Enviar (3 intentos)
    if not sent:
        click_send_with_retries(driver, wait, attempts=3)
    else:
        # refuerzo: si sigue habilitado, quizá no envió
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

    # Esperar respuesta “nueva”: estrategia simple (puedes refinar si el DOM cambia mucho)
    end_time = time.time() + 40
    last = None
    while time.time() < end_time:
        txt = read_last_visible_p_text(driver)
        if txt and txt != last and txt.strip() != prompt.strip():
            # OJO: a veces lo último puede ser el eco del prompt; filtramos eso.
            # Si tu Copilot devuelve exactamente el prompt (como te pasó), esto lo evita.
            # Si igual quieres permitirlo, quita la condición de != prompt.
            return txt.strip()
        last = txt
        time.sleep(0.4)

    raise TimeoutException("No pude leer respuesta de Copilot a tiempo.")

# =========================
# HELPERS LOGIN TEST.COM
# =========================
def capture_img_element(driver, wait, save_path):
    driver.get(URL_LOGIN)
    # esperar imgID
    el = wait.until(EC.presence_of_element_located((By.ID, "imgID")))
    os.makedirs(os.path.dirname(save_path), exist_ok=True)
    el.screenshot(save_path)

def login_test_site(driver, wait, usuario, clave, test_text):
    driver.get(URL_LOGIN)

    # Usuario
    inp_user = wait.until(EC.presence_of_element_located((By.ID, "c_c_usuario")))
    inp_user.clear()
    inp_user.send_keys(usuario)

    # Escribir el test ANTES de enter/aceptar
    inp_test = wait.until(EC.presence_of_element_located((By.ID, "c_c_test")))
    inp_test.clear()
    inp_test.send_keys(test_text)

    # Esperar keypad
    wait.until(EC.presence_of_element_located((By.ID, "ulKeypad")))

    # (Opcional) limpiar clave si hay botón borrar
    try:
        driver.find_element(By.CSS_SELECTOR, "#ulKeypad li.WEB_zonaIngresobtnBorrar").click()
    except Exception:
        pass

    # Click por cada dígito
    for d in clave:
        tecla = wait.until(
            EC.element_to_be_clickable((By.XPATH, f"//ul[@id='ulKeypad']//li[normalize-space()='{d}']"))
        )
        tecla.click()
        time.sleep(0.15)

    # ACEPTAR
    btn = wait.until(EC.element_to_be_clickable((By.ID, "btnIngresar")))
    btn.click()

# =========================
# MAIN
# =========================
def main():
    # 1) Conectar a Chrome debug (debe estar abierto con --remote-debugging-port=9222)
    options = Options()
    options.add_experimental_option("debuggerAddress", "127.0.0.1:9222")

    try:
        driver = webdriver.Chrome(service=Service(CHROMEDRIVER_PATH), options=options)
    except Exception as e:
        raise SystemExit(
            "No pude conectar al Chrome en debug.\n"
            "Asegúrate de abrirlo así (CMD):\n"
            r'"C:\Program Files\Google\Chrome\Application\chrome.exe" ^' "\n"
            r'  --remote-debugging-port=9222 ^' "\n"
            r'  --user-data-dir="C:\ChromeDebugProfile"' "\n\n"
            f"Detalle: {repr(e)}"
        )

    wait = WebDriverWait(driver, 30)

    # 2) Capturar imagen del sitio
    capture_img_element(driver, wait, CAPTURA_PATH)

    # 3) Preguntar a Copilot por el texto
    copilot_text = get_copilot_answer(driver, WebDriverWait(driver, 40), CAPTURA_PATH, PROMPT)

    # 4) Usar ese texto en el login del sitio
    login_test_site(driver, wait, USUARIO, CLAVE, copilot_text)

    # 5) Imprimir ÚNICAMENTE lo que devolvió Copilot
    print(copilot_text)

    # En debug normalmente NO cerramos
    # driver.quit()

if __name__ == "__main__":
    main()
