import time
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options

CHROMEDRIVER_PATH = r"D:\ruta\chromedriver.exe"
IMG_PATH = r"D:\Datos de Usuarios\T72496\Desktop\MODELOS_RPTs\WebAutomatic\captura.png"

def main():
    options = Options()
    # Reusar sesión (elige una opción):
    options.add_argument(r'--user-data-dir=C:\Users\TU_USUARIO\AppData\Local\Google\Chrome\User Data')
    options.add_argument(r'--profile-directory=Default')

    driver = webdriver.Chrome(service=Service(CHROMEDRIVER_PATH), options=options)
    wait = WebDriverWait(driver, 30)

    driver.get("https://m365.cloud.microsoft/chat/?auth=2")

    # 1) Subir imagen
    file_input = wait.until(EC.presence_of_element_located((By.ID, "upload-file-button")))
    file_input.send_keys(IMG_PATH)

    # 2) Escribir prompt en el contenteditable
    box = wait.until(EC.element_to_be_clickable((By.ID, "m365-chat-editor-target-element")))
    box.click()
    box.send_keys("En una sola palabra dime el texto de la imagen")

    # 3) Enviar (Enter suele funcionar; si no, clic al botón enviar)
    box.send_keys(Keys.ENTER)

    # 4) Esperar respuesta: busca un <p> con 4 letras mayúsculas (ajusta si hace falta)
    #   OJO: Copilot puede meter varios <p>. Aquí esperamos uno “nuevo”.
    time.sleep(1)

    # Si tu div de respuesta cambia de clases (muy probable), evita esas clases largas.
    # Mejor: esperar el último <p> visible que tenga texto.
    def last_p_with_text(drv):
        ps = drv.find_elements(By.CSS_SELECTOR, "p")
        texts = [p.text.strip() for p in ps if p.is_displayed() and p.text.strip()]
        return texts[-1] if texts else None

    result = wait.until(lambda d: last_p_with_text(d))
    print("Respuesta:", result)

    driver.quit()

if __name__ == "__main__":
    main()
