
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
    # Reusar sesión (elige una opción):
    options.add_argument(r'--user-data-dir=C:\Users\T72496\AppData\Local\Google\Chrome\User Data')
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




Tengo este error:

(venv) D:\Datos de Usuarios\T72496\Desktop\MODELOS_RPTs\WebAutomatic>py reconocimiento.py
Traceback (most recent call last):
  File "D:\Datos de Usuarios\T72496\Desktop\MODELOS_RPTs\WebAutomatic\reconocimiento.py", line 54, in <module>
    main()
    ~~~~^^
  File "D:\Datos de Usuarios\T72496\Desktop\MODELOS_RPTs\WebAutomatic\reconocimiento.py", line 20, in main
    driver = webdriver.Chrome(service=Service(CHROMEDRIVER_PATH), options=options)
  File "D:\Datos de Usuarios\T72496\Desktop\MODELOS_RPTs\venv\Lib\site-packages\selenium\webdriver\chrome\webdriver.py", line 46, in __init__
    super().__init__(
    ~~~~~~~~~~~~~~~~^
        browser_name=DesiredCapabilities.CHROME["browserName"],
        ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    ...<3 lines>...
        keep_alive=keep_alive,
        ^^^^^^^^^^^^^^^^^^^^^^
    )
    ^
  File "D:\Datos de Usuarios\T72496\Desktop\MODELOS_RPTs\venv\Lib\site-packages\selenium\webdriver\chromium\webdriver.py", line 67, in __init__
    super().__init__(command_executor=executor, options=options)
    ~~~~~~~~~~~~~~~~^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "D:\Datos de Usuarios\T72496\Desktop\MODELOS_RPTs\venv\Lib\site-packages\selenium\webdriver\remote\webdriver.py", line 257, in __init__
    self.start_session(capabilities)
    ~~~~~~~~~~~~~~~~~~^^^^^^^^^^^^^^
  File "D:\Datos de Usuarios\T72496\Desktop\MODELOS_RPTs\venv\Lib\site-packages\selenium\webdriver\remote\webdriver.py", line 352, in start_session
    response = self.execute(Command.NEW_SESSION, caps)["value"]
               ~~~~~~~~~~~~^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "D:\Datos de Usuarios\T72496\Desktop\MODELOS_RPTs\venv\Lib\site-packages\selenium\webdriver\remote\webdriver.py", line 432, in execute
    self.error_handler.check_response(response)
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~^^^^^^^^^^
  File "D:\Datos de Usuarios\T72496\Desktop\MODELOS_RPTs\venv\Lib\site-packages\selenium\webdriver\remote\errorhandler.py", line 232, in check_response
    raise exception_class(message, screen, stacktrace)
selenium.common.exceptions.SessionNotCreatedException: Message: session not created: Chrome failed to start: crashed.
  (session not created: DevToolsActivePort file doesn't exist)
  (The process started from chrome location C:\Program Files\Google\Chrome\Application\chrome.exe is no longer running, so ChromeDriver is assuming that Chrome has crashed.); For documentation on this error, please visit: https://www.selenium.dev/documentation/webdriver/troubleshooting/errors#sessionnotcreatedexception
Stacktrace:
Symbols not available. Dumping unresolved backtrace:
        0x7ff7fe4488e5
        0x7ff7fe448940
        0x7ff7fe22165d
        0x7ff7fe2602a0
        0x7ff7fe25b7dd
        0x7ff7fe2b0186
        0x7ff7fe2afa06
        0x7ff7fe26ac29
        0x7ff7fe26ba93
        0x7ff7fe760640
        0x7ff7fe75af80
        0x7ff7fe7796e6
        0x7ff7fe465de4
        0x7ff7fe46ed8c
        0x7ff7fe452004
        0x7ff7fe4521b5
        0x7ff7fe437ee2
        0x7ffd919a259d
        0x7ffd93d4af78
