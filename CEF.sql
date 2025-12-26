from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
import time

options = Options()
options.add_argument("--start-maximized")

print("Iniciando Chrome con Selenium Manager...")

driver = webdriver.Chrome(options=options)

print("Chrome iniciado correctamente")

driver.get("https://www.google.com")

print("Página cargada")

time.sleep(5)

driver.quit()
print("Cerrado OK")
