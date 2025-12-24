import re
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

URL = "https://m365.cloud.microsoft/chat/?auth=2"

CHROMEDRIVER = r"D:\RUTA\A\chromedriver.exe"
IMAGE_PATH  = r"D:\Datos de Usuarios\T72496\Desktop\MODELOS_RPTs\WebAutomatic\captura.png"

PROMPT = "En una sola palabra dime el texto de la imagen."

def main():
    options = webdriver.ChromeOptions()
    options.add_argument("--start-maximized")

    service = Service(CHROMEDRIVER)
    driver = webdriver.Chrome(service=service, options=options)
    wait = WebDriverWait(driver, 30)

    driver.get(URL)

    # Si necesitas login/MFA manual, pausa aquí:
    input("Cuando termines de iniciar sesión en Copilot, presiona ENTER...")

    # 1) Subir archivo (input hidden)
    file_input = wait.until(EC.presence_of_element_located((By.ID, "upload-file-button")))
    file_input.send_keys(IMAGE_PATH)

    # 2) Escribir prompt en el textbox contenteditable
    editor = wait.until(EC.element_to_be_clickable((By.ID, "m365-chat-editor-target-element")))
    editor.click()
    editor.send_keys(PROMPT)

    # 3) Click en Enviar
    send_btn = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, "button[type='submit'][aria-label='Enviar']")))
    send_btn.click()

    # 4) Esperar respuesta: último <p> que parezca la salida (ej. CEFC)
    def last_p_text(drv):
        ps = drv.find_elements(By.XPATH, "//p[normalize-space(.)!='']")
        if not ps:
            return None
        txt = ps[-1].text.strip()
        return txt if txt else None

    answer = wait.until(lambda d: last_p_text(d))
    print("Respuesta cruda:", answer)

    # Opcional: quedarte solo con una “palabra” tipo CEFC
    m = re.search(r"\b[A-Za-z0-9_]{2,10}\b", answer)
    print("Respuesta filtrada:", m.group(0) if m else answer)

    driver.quit()

if __name__ == "__main__":
    main()
