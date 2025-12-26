from selenium.webdriver.edge.options import Options as EdgeOptions

def main():
    opts = EdgeOptions()
    opts.add_experimental_option("debuggerAddress", "127.0.0.1:9223")

    driver = webdriver.Edge(options=opts)

    # ✅ Forzar viewport consistente
    driver.set_window_size(1366, 768)
    driver.execute_cdp_cmd("Emulation.setDeviceMetricsOverride", {
        "width": 1366,
        "height": 768,
        "deviceScaleFactor": 1,
        "mobile": False
    })




img_el = wait.until(EC.visibility_of_element_located((By.ID, "testaImgID")))
driver.execute_script("arguments[0].scrollIntoView({block:'center'});", img_el)
time.sleep(0.2)
img_el.screenshot(str(IMG_PATH))
