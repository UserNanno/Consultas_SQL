Ya me funcionó todo

Ahora quiero unir todos mis pasos

  0. Primero debe entrar en modo debug a chrome con 

  "C:\Program Files\Google\Chrome\Application\chrome.exe" ^
  --remote-debugging-port=9222 ^
  --user-data-dir="C:\ChromeDebugProfile"

  1. entra a la pagina y coloca el usuario y contraseña


from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time

# =========================
# CONFIG
# =========================
CHROMEDRIVER_PATH = r"D:\Datos de Usuarios\T72496\Downloads\chromedriver-win64\chromedriver-win64\chromedriver.exe"
URL_LOGIN = "https://test.com/app/login.jsp"

USUARIO = "TU_USUARIO"
CLAVE = "123456"  # solo números

# =========================
# DRIVER
# =========================
opts = Options()
opts.add_argument("--window-size=1200,800")

service = Service(CHROMEDRIVER_PATH)
driver = webdriver.Chrome(service=service, options=opts)
wait = WebDriverWait(driver, 20)

# =========================
# LOGIN
# =========================
driver.get(URL_LOGIN)

# Usuario
inp_user = wait.until(EC.presence_of_element_located((By.ID, "c_c_usuario")))
inp_user.clear()
inp_user.send_keys(USUARIO)

# Esperar keypad
wait.until(EC.presence_of_element_located((By.ID, "ulKeypad")))

# Limpiar clave (si existe)
try:
    driver.find_element(By.CSS_SELECTOR, "#ulKeypad li.WEB_zonaIngresobtnBorrar").click()
except Exception:
    pass

# Ingresar clave con clicks
for d in CLAVE:
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

print("Login ejecutado. Revisa el navegador.")
input("ENTER para cerrar...")

driver.quit()



Antes de dar enter debe escribir acá

<input id="c_c_captcha" type="text" name="c_c_captcha" class="WEB_zonaIngresoCaptchaInfo watermark" maxlength="5" title="Ingrese texto captcha">


El texto que me arroje el siguiente script

import time
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException, StaleElementReferenceException
from selenium.webdriver.common.action_chains import ActionChains

CHROMEDRIVER_PATH = r"D:\Datos de Usuarios\T72496\Downloads\chromedriver-win64\chromedriver-win64\chromedriver.exe"
IMG_PATH = r"D:\Datos de Usuarios\T72496\Desktop\MODELOS_RPTs\WebAutomatic\captura.png"

URL = "https://m365.cloud.microsoft/chat/?auth=2"

def wait_send_enabled(driver, wait):
    """
    Espera hasta que el botón Enviar exista y esté habilitado.
    Copilot a veces usa disabled o aria-disabled.
    """
    def _cond(d):
        try:
            btn = d.find_element(By.CSS_SELECTOR, "button[type='submit'][aria-label='Enviar'], button[type='submit'][title='Enviar']")
            disabled_attr = btn.get_attribute("disabled")
            aria_disabled = btn.get_attribute("aria-disabled")
            # enabled si NO tiene disabled y aria-disabled no es "true"
            if disabled_attr is None and (aria_disabled is None or aria_disabled.lower() != "true"):
                return btn
        except Exception:
            return None
        return None

    return wait.until(_cond)

def set_contenteditable_text(driver, element, text):
    """
    Asegura el texto en un contenteditable usando JS (más confiable que send_keys en ciertas webs).
    """
    driver.execute_script(
        """
        const el = arguments[0];
        const txt = arguments[1];
        el.focus();
        // Limpia y escribe como texto
        el.innerText = txt;

        // Dispara eventos para que la app "se entere"
        el.dispatchEvent(new InputEvent('input', { bubbles: true }));
        el.dispatchEvent(new Event('change', { bubbles: true }));
        """,
        element, text
    )

def click_send_with_retries(driver, wait, attempts=3):
    last_err = None
    for i in range(attempts):
        try:
            btn = wait_send_enabled(driver, wait)
            # click "real" + fallback JS por si hay overlay
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

def main():
    options = Options()
    options.add_experimental_option("debuggerAddress", "127.0.0.1:9222")

    driver = webdriver.Chrome(service=Service(CHROMEDRIVER_PATH), options=options)
    wait = WebDriverWait(driver, 40)

    driver.get(URL)
    print("URL actual:", driver.current_url)

    # 1) Subir imagen (input real)
    file_input = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "input[type='file']")))
    file_input.send_keys(IMG_PATH)

    # 2) Espera a que el botón Enviar esté habilitado (clave cuando hay adjunto)
    try:
        wait_send_enabled(driver, wait)
    except TimeoutException:
        print("Ojo: el botón Enviar no se habilitó a tiempo. Igual intento continuar...")

    # 3) Escribir prompt en el contenteditable
    box = wait.until(EC.element_to_be_clickable((By.ID, "m365-chat-editor-target-element")))
    box.click()

    prompt = "En una sola palabra dime el texto de la imagen"
    # primero intenta send_keys normal
    box.send_keys(Keys.CONTROL, "a")
    box.send_keys(prompt)

    # refuerzo: set por JS (por si Copilot no registró el send_keys)
    set_contenteditable_text(driver, box, prompt)

    # 4) Intentar Enter; si no, click Enviar 3 veces
    sent = False
    try:
        ActionChains(driver).move_to_element(box).click(box).send_keys(Keys.ENTER).perform()
        sent = True
    except Exception:
        sent = False

    # a veces Enter no envía si hay adjunto; usa botón
    if not sent:
        click_send_with_retries(driver, wait, attempts=3)
    else:
        # aunque "haya enviado", a veces no; reforzamos con click si sigue habilitado
        time.sleep(0.8)
        try:
            btn = driver.find_element(By.CSS_SELECTOR, "button[type='submit'][aria-label='Enviar'], button[type='submit'][title='Enviar']")
            aria_disabled = (btn.get_attribute("aria-disabled") or "").lower()
            if aria_disabled != "true" and btn.get_attribute("disabled") is None:
                # si sigue habilitado, probablemente NO envió -> click
                click_send_with_retries(driver, wait, attempts=3)
        except Exception:
            pass

    # 5) Esperar “algo” de respuesta (esto es muy genérico; depende del DOM real)
    time.sleep(2)

    def last_p_with_text(drv):
        ps = drv.find_elements(By.CSS_SELECTOR, "p")
        texts = [p.text.strip() for p in ps if p.is_displayed() and p.text.strip()]
        return texts[-1] if texts else None

    try:
        result = wait.until(lambda d: last_p_with_text(d))
        print("Respuesta:", result)
    except TimeoutException:
        print("No pude leer respuesta a tiempo (puede que el DOM cambie o esté cargando).")

    # En modo debug, no cierres
    # driver.quit()

if __name__ == "__main__":
    main()

La respuesta debe ser unicamente lo que devuelva copilot

Ahora antes de buscar en copilot o ejecutar lo anterior debe buscar primero la imagen a consultar

Par aello se debe ejecutar esto que captura la imgen que quiero buscar en copilot
  
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
import time

CHROMEDRIVER_PATH = r"D:\Datos de Usuarios\T72496\Downloads\chromedriver-win64\chromedriver-win64\chromedriver.exe"

options = Options()
options.add_argument("--start-maximized")

service = Service(CHROMEDRIVER_PATH)
driver = webdriver.Chrome(service=service, options=options)

driver.get("https://test.com/app/login.jsp")

# Espera a que cargue el elemento
time.sleep(2)  # simple y efectivo
element = driver.find_element(By.ID, "imgID")

# Captura SOLO el elemento
element.screenshot("captura.png")

print("Captura guardada como captura.png")

driver.quit()



Se entiende?

