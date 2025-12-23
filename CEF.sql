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

driver.get("https://tu-pagina.com")

# Espera a que cargue el elemento
time.sleep(2)  # simple y efectivo
element = driver.find_element(By.ID, "test")

# Captura SOLO el elemento
element.screenshot("captura.png")

print("Captura guardada como captura.png")

driver.quit()
