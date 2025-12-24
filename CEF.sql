from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time

COPILOT_URL = "https://m365.cloud.microsoft/chat/?auth=2"
IMAGE_PATH = r"D:\Datos de Usuarios\T72496\Desktop\MODELOS_RPTs\WebAutomatic\captura.png"
PROMPT = "En una sola palabra dime el texto de la imagen."

def main():
    # Chrome (recomendado). Si usas Edge, cambia webdriver.Chrome por webdriver.Edge
    options = webdriver.ChromeOptions()

    # Para mantener sesión (evita loguearte cada vez):
    options.add_argument(r"--user-data-dir=D:\selenium_profiles\copilot")
    options.add_argument("--profile-directory=Default")

    driver = webdriver.Chrome(options=options)
    wait = WebDriverWait(driver, 30)

    driver.get(COPILOT_URL)

    # 1) Espera a que exista el input de upload (aunque esté hidden)
    file_input = wait.until(EC.presence_of_element_located((By.ID, "upload-file-button")))

    # A veces está display:none; igual suele aceptar send_keys, pero si falla, lo “destapas”:
    driver.execute_script("arguments[0].style.display='block'; arguments[0].style.visibility='visible';", file_input)
    file_input.send_keys(IMAGE_PATH)

    # 2) Escribe prompt en el editor contenteditable
    editor = wait.until(EC.presence_of_element_located((By.ID, "m365-chat-editor-target-element")))
    editor.click()
    editor.send_keys(PROMPT)

    # 3) Enviar (ENTER suele funcionar; si no, clic al botón Enviar)
    editor.send_keys(Keys.ENTER)

    # 4) Esperar respuesta: buscamos el <p> más reciente dentro del contenedor que tú diste
    #    IMPORTANTE: ese div tiene clases muy largas (inestables). Mejor: esperar cualquier <p> nuevo.
    #    Estrategia: esperar a que aparezca al menos un <p> adicional después de enviar.
    time.sleep(1)  # pequeño margen

    # Espera a que aparezca algún <p> que contenga texto (y toma el último)
    wait.until(lambda d: len(d.find_elements(By.CSS_SELECTOR, "div p")) > 0)
    time.sleep(2)  # deja que termine de “escribir” la respuesta

    ps = driver.find_elements(By.CSS_SELECTOR, "div p")
    last = next((p.text for p in reversed(ps) if p.text.strip()), "")

    print("Respuesta Copilot:", last)

    # driver.quit()

if __name__ == "__main__":
    main()
