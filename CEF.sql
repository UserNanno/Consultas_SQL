from __future__ import annotations

import re
import time
from typing import Dict, Any, List, Iterable, Optional, Tuple

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (
    TimeoutException,
    StaleElementReferenceException,
    WebDriverException,
)

# =========================
# URLs
# =========================
URL_SUNAT = "https://e-consultaruc.sunat.gob.pe/cl-ti-itmrconsruc/FrameCriterioBusquedaWeb.jsp"
URL_SBS = "https://servicios.sbs.gob.pe/ReporteSituacionPrevisional/Afil_Consulta.aspx"

# ✅ Ajusta a tu ruta real
CHROMEDRIVER_PATH = r"D:\Datos de Usuarios\T72496\Downloads\chromedriver-win64\chromedriver-win64\chromedriver.exe"


# =========================
# Helpers SUNAT parse
# =========================
def _clean_label(s: str) -> str:
    s = (s or "").strip()
    return re.sub(r":\s*$", "", s)


def _extract_value(cell) -> str:
    # tablas
    if cell.find_elements(By.CSS_SELECTOR, "table"):
        rows: List[str] = []
        for td in cell.find_elements(By.CSS_SELECTOR, "table tr td"):
            t = td.text.strip()
            if t:
                rows.append(t)
        return "\n".join(rows).strip() if rows else "-"

    # p
    ps = cell.find_elements(By.CSS_SELECTOR, "p")
    if ps:
        t = " ".join(p.text.strip() for p in ps if p.text.strip())
        return t if t else "-"

    # h4
    h4s = cell.find_elements(By.CSS_SELECTOR, "h4")
    if h4s:
        t = " ".join(h.text.strip() for h in h4s if h.text.strip())
        return t if t else "-"

    t = cell.text.strip()
    return t if t else "-"


def build_driver(
    headless: bool = False,
    hide_window: bool = True,
    user_data_dir: Optional[str] = None,
    profile_dir: Optional[str] = None,
) -> webdriver.Chrome:
    opts = Options()
    opts.add_argument("--disable-gpu")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-extensions")
    opts.add_argument("--disable-notifications")
    opts.add_argument("--disable-popup-blocking")
    opts.add_argument("--blink-settings=imagesEnabled=false")
    opts.add_argument("--window-size=1200,800")

    # ✅ ocultar sin headless (más compatible con SUNAT/SBS)
    if hide_window and not headless:
        opts.add_argument("--window-position=-32000,-32000")

    if user_data_dir:
        opts.add_argument(f"--user-data-dir={user_data_dir}")
    if profile_dir:
        opts.add_argument(f"--profile-directory={profile_dir}")

    if headless:
        # OJO: SUNAT/SBS pueden bloquear headless.
        opts.add_argument("--headless=new")

    # Selenium 4 capability
    opts.set_capability("pageLoadStrategy", "eager")

    service = Service(executable_path=CHROMEDRIVER_PATH)
    driver = webdriver.Chrome(service=service, options=opts)
    driver.set_page_load_timeout(25)
    driver.set_script_timeout(25)
    return driver


def parse_panel_sunat(panel) -> Dict[str, Any]:
    data: Dict[str, Any] = {}

    items = panel.find_elements(By.CSS_SELECTOR, "div.list-group > div.list-group-item")
    for item in items:
        cols = item.find_elements(By.CSS_SELECTOR, ".row > div")
        if not cols:
            continue

        # parse por pares: label col + value col
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


def parse_panel_sunat_with_retry(driver: webdriver.Chrome, tries: int = 4) -> Dict[str, Any]:
    last_exc: Exception | None = None
    for _ in range(tries):
        try:
            panel = driver.find_element(By.CSS_SELECTOR, "div.panel.panel-primary")
            return parse_panel_sunat(panel)
        except StaleElementReferenceException as e:
            last_exc = e
            time.sleep(0.2)
    raise last_exc if last_exc else RuntimeError("No se pudo parsear SUNAT (stale).")


def extraer_dni_y_nombres(tipo_documento_text: str) -> Dict[str, str]:
    """
    Espera algo como:
    "DNI  78801600  - CANECILLAS CONTRERAS, JUAN MARIANO"
    """
    s = (tipo_documento_text or "").strip()

    # DNI (8 dígitos)
    m = re.search(r"\bDNI\s*([0-9]{8})\b", s)
    dni = m.group(1) if m else ""

    # Parte nombre: después del "-"
    # Ej: " ... - APELLIDOS, NOMBRES"
    partes = s.split("-", 1)
    nombre_raw = partes[1].strip() if len(partes) == 2 else ""

    # Separar "APELLIDOS, NOMBRES"
    apellidos = ""
    nombres = ""
    if "," in nombre_raw:
        apellidos, nombres = [x.strip() for x in nombre_raw.split(",", 1)]
    else:
        # fallback si no viene coma
        nombres = nombre_raw.strip()

    # Apellidos: paterno/materno
    ap_tokens = [t for t in apellidos.split() if t]
    apellido_paterno = ap_tokens[0] if len(ap_tokens) >= 1 else ""
    apellido_materno = ap_tokens[1] if len(ap_tokens) >= 2 else ""

    # Nombres: primer/segundo (si hay más, los dejo en segundo)
    nom_tokens = [t for t in nombres.split() if t]
    primer_nombre = nom_tokens[0] if len(nom_tokens) >= 1 else ""
    segundo_nombre = " ".join(nom_tokens[1:]) if len(nom_tokens) >= 2 else ""

    return {
        "dni": dni,
        "apellido_paterno": apellido_paterno,
        "apellido_materno": apellido_materno,
        "primer_nombre": primer_nombre,
        "segundo_nombre": segundo_nombre,
    }


def consultar_sunat(driver: webdriver.Chrome, wait: WebDriverWait, ruc: str) -> Dict[str, Any]:
    if not re.fullmatch(r"\d{11}", ruc):
        raise ValueError("El RUC debe tener 11 dígitos.")

    driver.get(URL_SUNAT)

    # panel anterior si existiera
    old_panels = driver.find_elements(By.CSS_SELECTOR, "div.panel.panel-primary")
    old_panel = old_panels[0] if old_panels else None

    inp = wait.until(EC.presence_of_element_located((By.ID, "txtRuc")))
    inp.clear()
    inp.send_keys(ruc)

    driver.find_element(By.ID, "btnAceptar").click()

    if old_panel is not None:
        try:
            wait.until(EC.staleness_of(old_panel))
        except TimeoutException:
            pass

    wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "div.panel.panel-primary")))

    data = {"ruc_consultado": ruc}
    data.update(parse_panel_sunat_with_retry(driver))

    # Extraer DNI y nombres desde "Tipo de Documento"
    tipo_doc = data.get("Tipo de Documento", "")
    if isinstance(tipo_doc, list):
        tipo_doc = tipo_doc[0] if tipo_doc else ""
    if isinstance(tipo_doc, str) and tipo_doc:
        data["datos_persona"] = extraer_dni_y_nombres(tipo_doc)
    else:
        data["datos_persona"] = {
            "dni": "",
            "apellido_paterno": "",
            "apellido_materno": "",
            "primer_nombre": "",
            "segundo_nombre": "",
        }

    return data


# =========================
# SBS / AFP
# =========================
def consultar_sbs_afp(
    driver: webdriver.Chrome,
    wait: WebDriverWait,
    dni: str,
    apellido_paterno: str,
    apellido_materno: str,
    primer_nombre: str,
    segundo_nombre: str = "",
) -> Dict[str, Any]:
    driver.get(URL_SBS)

    # Llenar inputs
    wait.until(EC.presence_of_element_located((By.ID, "ctl00_ContentPlaceHolder1_txtNumeroDoc"))).clear()
    driver.find_element(By.ID, "ctl00_ContentPlaceHolder1_txtNumeroDoc").send_keys(dni)

    driver.find_element(By.ID, "ctl00_ContentPlaceHolder1_txtAp_pat").clear()
    driver.find_element(By.ID, "ctl00_ContentPlaceHolder1_txtAp_pat").send_keys(apellido_paterno)

    driver.find_element(By.ID, "ctl00_ContentPlaceHolder1_txtAp_mat").clear()
    driver.find_element(By.ID, "ctl00_ContentPlaceHolder1_txtAp_mat").send_keys(apellido_materno)

    driver.find_element(By.ID, "ctl00_ContentPlaceHolder1_txtPri_nom").clear()
    driver.find_element(By.ID, "ctl00_ContentPlaceHolder1_txtPri_nom").send_keys(primer_nombre)

    driver.find_element(By.ID, "ctl00_ContentPlaceHolder1_txtSeg_nom").clear()
    if segundo_nombre:
        driver.find_element(By.ID, "ctl00_ContentPlaceHolder1_txtSeg_nom").send_keys(segundo_nombre)

    # Click buscar (submit)
    driver.find_element(By.ID, "ctl00_ContentPlaceHolder1_btnBuscar").click()

    # Esperar panel de resultados
    wait.until(EC.presence_of_element_located((By.ID, "ctl00_ContentPlaceHolder1_pnlConfirmar")))

    # Extraer campos por ID (más estable que parsear HTML completo)
    def safe_text(by, sel) -> str:
        try:
            return driver.find_element(by, sel).text.strip()
        except Exception:
            return ""

    res = {
        "dni": dni,
        "info_al": safe_text(By.ID, "ctl00_ContentPlaceHolder1_lblFechaReg"),
        "afiliado_desde": safe_text(By.ID, "ctl00_ContentPlaceHolder1_lblFec_ing"),
        "afp_actual": safe_text(By.ID, "ctl00_ContentPlaceHolder1_lblAfp_act"),
        "codigo_spp": safe_text(By.ID, "ctl00_ContentPlaceHolder1_lblCod_afi"),
        "situacion": safe_text(By.ID, "ctl00_ContentPlaceHolder1_lblSituacion"),
        "devengue_ultimo_aporte": safe_text(By.ID, "ctl00_ContentPlaceHolder1_lblFec_dev"),
        "detalle_situacion": safe_text(By.ID, "ctl00_ContentPlaceHolder1_TxtBoxSit_Det"),
        "detalle_aportes_obligatorios": safe_text(By.ID, "ctl00_ContentPlaceHolder1_TxtBoxApor_Obl"),
    }

    return res


# =========================
# Orquestación: SUNAT + SBS
# =========================
def consulta_completa(ruc: str, headless: bool = False, hide_window: bool = True) -> Dict[str, Any]:
    driver = build_driver(headless=headless, hide_window=hide_window, profile_dir="Default")
    wait = WebDriverWait(driver, 18)

    try:
        salida: Dict[str, Any] = {
            "SUNAT": {},
            "AFP": {},
        }

        # 1) SUNAT
        try:
            sunat = consultar_sunat(driver, wait, ruc)
            salida["SUNAT"] = {"RESULTADOS": sunat}
        except Exception as e:
            salida["SUNAT"] = {"ERROR": f"{type(e).__name__}: {e}"}
            return salida  # sin datos no podemos ir a SBS

        # 2) SBS/AFP usando datos de SUNAT
        persona = sunat.get("datos_persona", {}) or {}
        dni = persona.get("dni", "")
        ap_pat = persona.get("apellido_paterno", "")
        ap_mat = persona.get("apellido_materno", "")
        pri_nom = persona.get("primer_nombre", "")
        seg_nom = persona.get("segundo_nombre", "")

        if not dni or not ap_pat or not pri_nom:
            salida["AFP"] = {"ERROR": "No se pudo extraer DNI/nombres desde SUNAT (Tipo de Documento)."}
            return salida

        try:
            afp = consultar_sbs_afp(driver, wait, dni, ap_pat, ap_mat, pri_nom, seg_nom)
            salida["AFP"] = {"RESULTADOS": afp}
        except Exception as e:
            salida["AFP"] = {"ERROR": f"{type(e).__name__}: {e}"}

        return salida

    finally:
        driver.quit()


if __name__ == "__main__":
    ruc = "10788016005"
    resultado = consulta_completa(ruc, headless=False, hide_window=True)

    # impresión amigable
    print("\nSUNAT\n")
    for k, v in resultado.get("SUNAT", {}).items():
        print(f"{k}: {v}")

    print("\nAFP\n")
    for k, v in resultado.get("AFP", {}).items():
        print(f"{k}: {v}")
