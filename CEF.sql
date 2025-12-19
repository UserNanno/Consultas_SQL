from selenium.common.exceptions import StaleElementReferenceException, TimeoutException


def parse_panel(panel) -> Dict[str, Any]:
    data: Dict[str, Any] = {}

    items = panel.find_elements(By.CSS_SELECTOR, "div.list-group > div.list-group-item")
    for item in items:
        cols = item.find_elements(By.CSS_SELECTOR, ".row > div")
        if not cols:
            continue

        i = 0
        while i < len(cols) - 1:
            col_label = cols[i]
            col_value = cols[i + 1]

            label_el = col_label.find_elements(By.CSS_SELECTOR, "h4.list-group-item-heading")
            if not label_el:
                i += 1
                continue

            label = _clean_label(label_el[0].text)
            value = _extract_value(col_value).strip() or "-"

            if label:
                if label in data:
                    if isinstance(data[label], list):
                        data[label].append(value)
                    else:
                        data[label] = [data[label], value]
                else:
                    data[label] = value

            i += 2

    footer = panel.find_elements(By.CSS_SELECTOR, "div.panel-footer small")
    if footer:
        data["fecha_consulta"] = footer[0].text.strip()

    return data





def parse_panel_with_retry(driver: webdriver.Chrome, tries: int = 3) -> Dict[str, Any]:
    last_exc: Exception | None = None
    for _ in range(tries):
        try:
            panel = driver.find_element(By.CSS_SELECTOR, "div.panel.panel-primary")
            # leer y parsear inmediatamente
            return parse_panel(panel)
        except StaleElementReferenceException as e:
            last_exc = e
            time.sleep(0.2)  # mini backoff y reintento
    # si sigue fallando
    raise last_exc if last_exc else RuntimeError("No se pudo parsear el panel (stale).")









def consultar_ruc_en_sesion(driver: webdriver.Chrome, wait: WebDriverWait, ruc: str) -> Dict[str, Any]:
    if not re.fullmatch(r"\d{11}", ruc):
        raise ValueError(f"RUC inválido: {ruc}")

    # referencia al panel anterior (si existiera)
    old_panels = driver.find_elements(By.CSS_SELECTOR, "div.panel.panel-primary")
    old_panel = old_panels[0] if old_panels else None

    inp = wait.until(EC.presence_of_element_located((By.ID, "txtRuc")))
    inp.clear()
    inp.send_keys(ruc)

    driver.find_element(By.ID, "btnAceptar").click()

    # si había panel anterior, esperar a que quede stale (SUNAT re-renderiza)
    if old_panel is not None:
        try:
            wait.until(EC.staleness_of(old_panel))
        except TimeoutException:
            # no siempre ocurre, seguimos igual
            pass

    # esperar panel nuevo
    wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "div.panel.panel-primary")))

    data = {"ruc_consultado": ruc}
    data.update(parse_panel_with_retry(driver, tries=4))
    return data
