"C:\Program Files\Google\Chrome\Application\chrome.exe" ^
  --remote-debugging-port=9222 ^
  --user-data-dir="C:\ChromeDebugProfile"




import time
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options

CHROMEDRIVER_PATH = r"D:\Datos de Usuarios\T72496\Downloads\chromedriver-win64\chromedriver-win64\chromedriver.exe"
IMG_PATH = r"D:\Datos de Usuarios\T72496\Desktop\MODELOS_RPTs\WebAutomatic\captura.png"

def main():
    options = Options()
    options.add_experimental_option("debuggerAddress", "127.0.0.1:9222")

    driver = webdriver.Chrome(
        service=Service(CHROMEDRIVER_PATH),
        options=options
    )

    wait = WebDriverWait(driver, 30)

    driver.get("https://m365.cloud.microsoft/chat/?auth=2")

    print("URL actual:", driver.current_url)

    # Subir imagen (buscar el input real)
    file_input = wait.until(
        EC.presence_of_element_located((By.CSS_SELECTOR, "input[type='file']"))
    )
    file_input.send_keys(IMG_PATH)

    box = wait.until(
        EC.element_to_be_clickable((By.ID, "m365-chat-editor-target-element"))
    )
    box.click()
    box.send_keys("En una sola palabra dime el texto de la imagen")
    box.send_keys(Keys.ENTER)

    time.sleep(2)

    def last_p_with_text(drv):
        ps = drv.find_elements(By.CSS_SELECTOR, "p")
        texts = [p.text.strip() for p in ps if p.is_displayed() and p.text.strip()]
        return texts[-1] if texts else None

    result = wait.until(lambda d: last_p_with_text(d))
    print("Respuesta:", result)

    # NO cierres Chrome en debug
    # driver.quit()

if __name__ == "__main__":
    main()




A esta version que abre con debug quizá podamos dar un tiempo de espera a que complete de escribir y adjuntar la imagen
  y luego que intente dar click en este boton unas 3 veces

  <button type="submit" aria-label="Enviar" class="fui-Button r1alrhcs fai-SendButton fai-ChatInput__send fai-ExpandableChatInput__send ___1l3wey0 ffp7eso f1p3nwhy f11589ue f1q5o8ev f1pdflbu f1phragk fjxutwb f1s2uweq fr80ssc f1ukrpxl fecsdlb f1m1wcaq ft1hn21 fuxngvv fwiml72 f1h0usnq fs4ktlq f16h9ulv fx2bmrt f1fg1p5m f1dfjoow f1j98vj9 f1tme0vf f4xjyn1 f18onu3q f9ddjv3 fwbmr0d f1mk8lai f44lkw9 fod5ikn fl43uef faaz57k f1062rbf f22iagw feqmc2u fbhnoac f122n59 f1lm9dni f1mn5ei1 f5n6bpk fxeu3t6 fac75ms f1wkqx0x fpjfiuo f10vq4ri f15flfb3 fk73vx1 f1ho2jej faaplsl fnf7g6g f10t3ba1 f1ec5yf7 fq8omct" tabindex="0" title="Enviar"><span class="fai-SendButton__sendIcon ___udkpex0 f1euv43f f122n59 ftuwxu6 f4d9j23 f1pp30po frvgh55 fq4mcun"><svg class="fui-Icon-filled ___yt8pzc0 fjseox fez10in f1dd5bof" fill="currentColor" aria-hidden="true" width="24" height="24" viewBox="0 0 24 24" xmlns="http://www.w3.org/2000/svg"><path d="M13.7 4.28a1 1 0 1 0-1.4 1.43L17.67 11H4a1 1 0 1 0 0 2h13.66l-5.36 5.28a1 1 0 0 0 1.4 1.43l6.93-6.82c.5-.5.5-1.3 0-1.78L13.7 4.28Z" fill="currentColor"></path></svg><svg class="fui-Icon-regular ___9ctc0p0 f1w7gpdv fez10in f1dd5bof" fill="currentColor" aria-hidden="true" width="24" height="24" viewBox="0 0 24 24" xmlns="http://www.w3.org/2000/svg"><path d="M13.27 4.2a.75.75 0 0 0-1.04 1.1l6.25 5.95H3.75a.75.75 0 0 0 0 1.5h14.73l-6.25 5.95a.75.75 0 0 0 1.04 1.1l7.42-7.08a1 1 0 0 0 0-1.44L13.27 4.2Z" fill="currentColor"></path></svg></span></button>






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
