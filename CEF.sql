pip install playwright
playwright install



from playwright.sync_api import sync_playwright

with sync_playwright() as p:
    browser = p.chromium.launch(headless=False)  # True si no quieres ver el navegador
    page = browser.new_page()

    page.goto("https://tu-pagina.com")

    # Espera a que el elemento exista
    page.wait_for_timeout(5000)  # 2 segundos
    page.wait_for_selector("#test")

    # Captura SOLO ese elemento
    page.locator("#test").screenshot(path="captura.png")

    browser.close()

print("Captura guardada como captura.png")
