@echo off
echo Iniciando Edge en modo debug...

REM Abrir Edge con puerto 9222 y perfil separado
start "" msedge.exe ^
  --remote-debugging-port=9222 ^
  --user-data-dir="%~dp0edge_profile"

REM Esperar a que Edge levante
timeout /t 5 /nobreak >nul

echo Ejecutando script...
python main.py

pause



msedge.exe --remote-debugging-port=9222 --user-data-dir="%TEMP%\edge-debug"



import time
from pathlib import Path

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.edge.options import Options as EdgeOptions
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.common.action_chains import ActionChains

URL_LOGIN = "https://extranet.sbs.gob.pe/app/login.jsp"
URL_COPILOT = "https://m365.cloud.microsoft/chat/?auth=2"

USUARIO = "T10595"
CLAVE = "44445555"  # solo números

BASE_DIR = Path(__file__).resolve().parent
IMG_PATH = BASE_DIR / "output" / "captura.png"


# COPILOT HELPERS
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


def copilot_ask_from_image(driver, img_path: Path):
    wait = WebDriverWait(driver, 60)
    driver.get(URL_COPILOT)

    file_input = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "input[type='file']")))
    file_input.send_keys(str(img_path))

    try:
        wait_send_enabled(driver, wait)
    except TimeoutException:
        pass

    box = wait.until(EC.element_to_be_clickable((By.ID, "m365-chat-editor-target-element")))
    box.click()

    prompt = (
        "Lee el texto de la imagen y transcribe exactamente los 4 caracteres visibles. "
        "Ignora cualquier línea, raya, marca o distorsión superpuesta. "
        "Responde únicamente con esos 4 caracteres, sin añadir nada más"
    )

    box.send_keys(Keys.CONTROL, "a")
    box.send_keys(prompt)
    set_contenteditable_text(driver, box, prompt)

    sent = False
    try:
        ActionChains(driver).move_to_element(box).click(box).send_keys(Keys.ENTER).perform()
        sent = True
    except Exception:
        sent = False

    if not sent:
        click_send_with_retries(driver, wait, attempts=3)
    else:
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

    def last_p_with_text(drv):
        ps = drv.find_elements(By.CSS_SELECTOR, "p")
        texts = [p.text.strip() for p in ps if p.is_displayed() and p.text.strip()]
        return texts[-1] if texts else None

    result = wait.until(lambda d: last_p_with_text(d))
    return result


def main():
    # Conectar Edge en DEBUG ya abierto en 9222
    opts = EdgeOptions()
    opts.add_experimental_option("debuggerAddress", "127.0.0.1:9222")

    # IMPORTANTÍSIMO: SIN Service(executable_path). Así Selenium Manager gestiona el driver.
    driver = webdriver.Edge(options=opts)

    wait = WebDriverWait(driver, 30)

    driver.get(URL_LOGIN)
    form_handle = driver.current_window_handle

    img_el = wait.until(EC.presence_of_element_located((By.ID, "testImgID")))
    IMG_PATH.parent.mkdir(parents=True, exist_ok=True)
    img_el.screenshot(str(IMG_PATH))
    print("Captura guardada:", IMG_PATH)

    driver.switch_to.new_window('tab')
    copilot_text = copilot_ask_from_image(driver, IMG_PATH)
    print("Copilot devolvió:", copilot_text)

    driver.close()
    driver.switch_to.window(form_handle)

    inp_test = wait.until(EC.presence_of_element_located((By.ID, "c_c_test")))
    inp_test.clear()
    inp_test.send_keys(copilot_text)

    inp_user = wait.until(EC.presence_of_element_located((By.ID, "c_c_usuario")))
    inp_user.clear()
    inp_user.send_keys(USUARIO)

    wait.until(EC.presence_of_element_located((By.ID, "ulKeypad")))

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

    print("Flujo completo")


if __name__ == "__main__":
    main()


