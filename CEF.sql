
(venv) D:\Datos de Usuarios\T72496\Desktop\MODELOS_RPTs\WebAutomatic>py main.py
Traceback (most recent call last):
  File "D:\Datos de Usuarios\T72496\Desktop\MODELOS_RPTs\venv\Lib\site-packages\selenium\webdriver\common\driver_finder.py", line 64, in _binary_paths
    raise ValueError(f"The path is not a valid file: {path}")
ValueError: The path is not a valid file: D:\Datos de Usuarios\T72496\Downloads\chromedriver-win64\chromedriver-win64

The above exception was the direct cause of the following exception:

Traceback (most recent call last):
  File "D:\Datos de Usuarios\T72496\Desktop\MODELOS_RPTs\WebAutomatic\main.py", line 176, in <module>
    resultados = consultar_muchos(rucs, headless=False)
  File "D:\Datos de Usuarios\T72496\Desktop\MODELOS_RPTs\WebAutomatic\main.py", line 150, in consultar_muchos
    driver = build_driver(
        headless=headless,
        user_data_dir=None,       # opcional
        profile_dir="Default",
    )
  File "D:\Datos de Usuarios\T72496\Desktop\MODELOS_RPTs\WebAutomatic\main.py", line 82, in build_driver
    driver = webdriver.Chrome(
        service=service,
        options=opts,
    )
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
  File "D:\Datos de Usuarios\T72496\Desktop\MODELOS_RPTs\venv\Lib\site-packages\selenium\webdriver\chromium\webdriver.py", line 51, in __init__
    if finder.get_browser_path():
       ~~~~~~~~~~~~~~~~~~~~~~~^^
  File "D:\Datos de Usuarios\T72496\Desktop\MODELOS_RPTs\venv\Lib\site-packages\selenium\webdriver\common\driver_finder.py", line 47, in get_browser_path
    return self._binary_paths()["browser_path"]
           ~~~~~~~~~~~~~~~~~~^^
  File "D:\Datos de Usuarios\T72496\Desktop\MODELOS_RPTs\venv\Lib\site-packages\selenium\webdriver\common\driver_finder.py", line 78, in _binary_paths
    raise NoSuchDriverException(msg) from err
selenium.common.exceptions.NoSuchDriverException: Message: Unable to obtain driver for chrome; For documentation on this error, please visit: https://www.selenium.dev/documentation/webdriver/troubleshooting/errors/driver_location


(venv) D:\Datos de Usuarios\T72496\Desktop\MODELOS_RPTs\WebAutomatic>










from __future__ import annotations

import re
import time
from typing import Dict, Any, List, Iterable, Optional

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


URL = "https://e-consultaruc.sunat.gob.pe/cl-ti-itmrconsruc/FrameCriterioBusquedaWeb.jsp"

# 🔴 CAMBIA ESTA RUTA
CHROMEDRIVER_PATH = r"D:\Datos de Usuarios\T72496\Downloads\chromedriver-win64\chromedriver-win64"


def _clean_label(s: str) -> str:
    s = (s or "").strip()
    return re.sub(r":\s*$", "", s)


def _extract_value(cell) -> str:
    if cell.find_elements(By.CSS_SELECTOR, "table"):
        rows: List[str] = []
        for td in cell.find_elements(By.CSS_SELECTOR, "table tr td"):
            t = td.text.strip()
            if t:
                rows.append(t)
        return "\n".join(rows).strip() if rows else "-"

    ps = cell.find_elements(By.CSS_SELECTOR, "p")
    if ps:
        t = " ".join(p.text.strip() for p in ps if p.text.strip())
        return t if t else "-"

    h4s = cell.find_elements(By.CSS_SELECTOR, "h4")
    if h4s:
        t = " ".join(h.text.strip() for h in h4s if h.text.strip())
        return t if t else "-"

    t = cell.text.strip()
    return t if t else "-"


def build_driver(
    headless: bool = False,
    user_data_dir: Optional[str] = None,
    profile_dir: Optional[str] = None,
) -> webdriver.Chrome:

    opts = Options()

    # Optimización
    opts.add_argument("--disable-gpu")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-extensions")
    opts.add_argument("--disable-notifications")
    opts.add_argument("--disable-popup-blocking")
    opts.add_argument("--blink-settings=imagesEnabled=false")
    opts.add_argument("--window-size=1200,800")

    # Reusar perfil (opcional)
    if user_data_dir:
        opts.add_argument(f"--user-data-dir={user_data_dir}")
    if profile_dir:
        opts.add_argument(f"--profile-directory={profile_dir}")

    if headless:
        opts.add_argument("--headless=new")

    # Selenium 4: capability aquí
    opts.set_capability("pageLoadStrategy", "eager")

    # Servicio EXPLÍCITO (NO Selenium Manager)
    service = Service(executable_path=CHROMEDRIVER_PATH)

    driver = webdriver.Chrome(
        service=service,
        options=opts,
    )

    driver.set_page_load_timeout(20)
    driver.set_script_timeout(20)

    return driver


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


def consultar_ruc_en_sesion(driver: webdriver.Chrome, wait: WebDriverWait, ruc: str) -> Dict[str, Any]:
    if not re.fullmatch(r"\d{11}", ruc):
        raise ValueError(f"RUC inválido: {ruc}")

    inp = wait.until(EC.presence_of_element_located((By.ID, "txtRuc")))
    inp.clear()
    inp.send_keys(ruc)

    driver.find_element(By.ID, "btnAceptar").click()

    panel = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "div.panel.panel-primary")))

    data = {"ruc_consultado": ruc}
    data.update(parse_panel(panel))
    return data


def consultar_muchos(rucs: Iterable[str], headless: bool = False) -> List[Dict[str, Any]]:
    driver = build_driver(
        headless=headless,
        user_data_dir=None,       # opcional
        profile_dir="Default",
    )
    wait = WebDriverWait(driver, 15)

    try:
        driver.get(URL)

        resultados: List[Dict[str, Any]] = []
        for ruc in rucs:
            try:
                resultados.append(consultar_ruc_en_sesion(driver, wait, ruc))
            except Exception as e:
                resultados.append({"ruc_consultado": ruc, "error": str(e)})

            time.sleep(0.4)  # pausa corta

        return resultados
    finally:
        driver.quit()


if __name__ == "__main__":
    rucs = ["10788016005"]
    resultados = consultar_muchos(rucs, headless=False)

    for r in resultados:
        print("----")
        for k, v in r.items():
            print(f"{k}: {v}")
