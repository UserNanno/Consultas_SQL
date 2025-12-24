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
