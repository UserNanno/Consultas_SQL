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

URL = "https://m365.cloud.microsoft/chat/?auth=2"

def get_visible_p_texts(driver):
    ps = driver.find_elements(By.CSS_SELECTOR, "p")
    return [p.text.strip() for p in ps if p.is_displayed() and p.text.strip()]

def main():
    options = Options()
    options.add_argument(r'--user-data-dir=C:\Users\T72496\AppData\Local\Google\Chrome\User Data')
    options.add_argument(r'--profile-directory=Default')

    driver = webdriver.Chrome(service=Service(CHROMEDRIVER_PATH), options=options)
    wait = WebDriverWait(driver, 40)

    driver.get(URL)
    print("URL actual:", driver.current_url)

    # Captura "estado" antes de enviar para luego detectar cambios
    before_texts = get_visible_p_texts(driver)

    # 1) Subir imagen
    # OJO: a veces el elemento con id upload-file-button es un <button> que abre el diálogo,
    # y el input real es <input type=file>. Si te falla, te dejo un fallback.
    try:
        file_input = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "input[type='file']")))
        file_input.send_keys(IMG_PATH)
    except:
        # si solo existe botón, intenta click y luego buscar el input file
        btn = wait.until(EC.element_to_be_clickable((By.ID, "upload-file-button")))
        btn.click()
        file_input = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "input[type='file']")))
        file_input.send_keys(IMG_PATH)

    # 2) Escribir prompt
    box = wait.until(EC.element_to_be_clickable((By.ID, "m365-chat-editor-target-element")))
    box.click()
    box.send_keys(Keys.CONTROL, "a")
    box.send_keys("En una sola palabra dime el texto de la imagen")

    # 3) Esperar a que "Enviar" esté listo y enviar
    # Intentamos localizar un botón de enviar típico (puede variar).
    # Fallback: CTRL+ENTER.
    send_btn = None
    possible_selectors = [
        "button[aria-label='Send']",
        "button[title='Send']",
        "button[data-testid*='send']",
        "button[id*='send']",
    ]
    for sel in possible_selectors:
        try:
            send_btn = driver.find_element(By.CSS_SELECTOR, sel)
            break
        except:
            pass

    # Espera a que el botón esté habilitado (si existe). Si no existe, usa CTRL+ENTER.
    sent = False
    if send_btn:
        def enabled_send(d):
            try:
                b = d.find_element(By.CSS_SELECTOR, sel)  # último selector que encontró
                return b.is_displayed() and b.is_enabled()
            except:
                return False

        try:
            wait.until(enabled_send)
            # click normal; si lo intercepta algo, click por JS
            try:
                send_btn.click()
            except:
                driver.execute_script("arguments[0].click();", send_btn)
            sent = True
        except:
            pass

    if not sent:
        # En varios chats modernos: CTRL+ENTER envía más confiable que ENTER
        box.click()
        box.send_keys(Keys.CONTROL, Keys.ENTER)

    # 4) Esperar respuesta REAL (texto nuevo diferente al prompt)
    prompt = "En una sola palabra dime el texto de la imagen"

    def new_answer(d):
        texts = get_visible_p_texts(d)
        # buscamos cualquier texto nuevo que no esté en before_texts y que no sea el prompt
        news = [t for t in texts if t not in before_texts and t != prompt]
        return news[-1] if news else None

    result = wait.until(new_answer)
    print("Respuesta:", result)

    driver.quit()

if __name__ == "__main__":
    main()


