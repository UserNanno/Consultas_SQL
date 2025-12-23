from selenium import webdriver
from selenium.webdriver.common.by import By

driver = webdriver.Edge()
driver.get("https://tu-pagina.com")

element = driver.find_element(By.ID, "test")
element.screenshot("captura.png")

driver.quit()
