from selenium import webdriver

driver = webdriver.Edge()
driver.get("https://example.com")
print(driver.title)
driver.quit()
