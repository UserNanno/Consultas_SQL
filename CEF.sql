from __future__ import annotations

import re
from typing import Dict, Any, List

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


URL = "https://e-consultaruc.sunat.gob.pe/cl-ti-itmrconsruc/FrameCriterioBusquedaWeb.jsp"


def _clean_label(s: str) -> str:
    s = (s or "").strip()
    s = re.sub(r":\s*$", "", s)  # quita ":" final
    return s


def _extract_value(cell) -> str:
    """
    En la columna de valor puede haber:
    - <h4>...</h4> (caso Número de RUC)
    - <p>...</p>
    - <table> con filas <tr><td>...</td></tr>
    """
    # 1) Tabla (lista)
    if cell.find_elements(By.CSS_SELECTOR, "table"):
        rows: List[str] = []
        for td in cell.find_elements(By.CSS_SELECTOR, "table tr td"):
            t = td.text.strip()
            if t:
                rows.append(t)
        return "\n".join(rows).strip() if rows else "-"

    # 2) p
    ps = cell.find_elements(By.CSS_SELECTOR, "p")
    if ps:
        t = " ".join(p.text.strip() for p in ps if p.text.strip())
        return t if t else "-"

    # 3) h4
    h4s = cell.find_elements(By.CSS_SELECTOR, "h4")
    if h4s:
        t = " ".join(h.text.strip() for h in h4s if h.text.strip())
        return t if t else "-"

    # 4) fallback
    t = cell.text.strip()
    return t if t else "-"


def consultar_ruc(ruc: str, headless: bool = False, timeout: int = 25) -> Dict[str, Any]:
    if not re.fullmatch(r"\d{11}", ruc):
        raise ValueError("El RUC debe tener 11 dígitos.")

    opts = Options()
    if headless:
        # Si hay CAPTCHA, NO uses headless (mejor visible para resolverlo).
        opts.add_argument("--headless=new")
    opts.add_argument("--window-size=1280,900")

    driver = webdriver.Chrome(options=opts)
    wait = WebDriverWait(driver, timeout)

    try:
        driver.get(URL)

        # Ingresar RUC
        input_ruc = wait.until(EC.presence_of_element_located((By.ID, "txtRuc")))
        input_ruc.clear()
        input_ruc.send_keys(ruc)

        # Click Buscar
        driver.find_element(By.ID, "btnAceptar").click()

        # Esperar panel resultado
        panel = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "div.panel.panel-primary")))

        data: Dict[str, Any] = {"ruc_consultado": ruc}

        # Recorremos cada bloque list-group-item
        items = panel.find_elements(By.CSS_SELECTOR, "div.list-group > div.list-group-item")
        for item in items:
            cols = item.find_elements(By.CSS_SELECTOR, ".row > div")
            if not cols:
                continue

            # Parse por parejas: (label_col, value_col)
            i = 0
            while i < len(cols) - 1:
                col_label = cols[i]
                col_value = cols[i + 1]

                label_el = col_label.find_elements(By.CSS_SELECTOR, "h4.list-group-item-heading")
                if not label_el:
                    i += 1
                    continue

                label = _clean_label(label_el[0].text)
                if not label:
                    i += 2
                    continue

                value = _extract_value(col_value).strip()
                if not value:
                    value = "-"

                # Guardar (si se repite label, acumular)
                if label in data:
                    if isinstance(data[label], list):
                        data[label].append(value)
                    else:
                        data[label] = [data[label], value]
                else:
                    data[label] = value

                i += 2

            # Si hay un caso raro donde queda una última columna suelta (impar), la ignoramos.

        # Footer (fecha consulta)
        footer = panel.find_elements(By.CSS_SELECTOR, "div.panel-footer small")
        if footer:
            data["fecha_consulta"] = footer[0].text.strip()

        return data

    finally:
        driver.quit()


if __name__ == "__main__":
    resultado = consultar_ruc("10788016005", headless=False)
    for k, v in resultado.items():
        print(f"{k}: {v}")
