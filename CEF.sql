from __future__ import annotations

import os
import re
import time
import random
from typing import Dict, Any, List, Optional

from selenium import webdriver
from selenium.webdriver.common.by import By

from selenium.webdriver.chrome.options import Options as ChromeOptions
from selenium.webdriver.chrome.service import Service as ChromeService

from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, StaleElementReferenceException


# =========================
# URLs
# =========================
URL_SUNAT = "https://e-consultaruc.sunat.gob.pe/cl-ti-itmrconsruc/FrameCriterioBusquedaWeb.jsp"
URL_SBS = "https://servicios.sbs.gob.pe/ReporteSituacionPrevisional/Afil_Consulta.aspx"

# ✅ Ajusta ruta a tu chromedriver.exe
CHROMEDRIVER_PATH = r"D:\Datos de Usuarios\T72496\Downloads\chromedriver-win64\chromedriver-win64\chromedriver.exe"

# ✅ Perfiles separados (carpetas que se crearán si no existen)
#    IMPORTANTE: no uses tu perfil real de Chrome. Usa perfiles “selenium”.
PROFILE_SUNAT_DIR = r"D:\selenium_profiles\sunat"
PROFILE_SBS_DIR = r"D:\selenium_profiles\sbs"


# =========================
# Utilidades “humanas”
# =========================
def human_pause(a: float = 0.6, b: float = 1.8) -> None:
    time.sleep(random.uniform(a, b))


def type_like_human(el, text: str, min_delay: float = 0.03, max_delay: float = 0.10) -> None:
    el.clear()
    for ch in text:
        el.send_keys(ch)
        time.sleep(random.uniform(min_delay, max_delay))


# =========================
# Chrome driver (con perfil)
# =========================
def build_chrome_driver(user_data_dir: str, show_window: bool = True) -> webdriver.Chrome:
    os.makedirs(user_data_dir, exist_ok=True)

    opts = ChromeOptions()
    opts.add_argument("--disable-gpu")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-extensions")
    opts.add_argument("--disable-notifications")
    opts.add_argument("--disable-popup-blocking")
    opts.add_argument("--blink-settings=imagesEnabled=false")
    opts.add_argument("--window-size=1200,800")

    # Perfil separado
    opts.add_argument(f"--user-data-dir={user_data_dir}")
    # opcional: mantener un nombre de perfil dentro del user-data-dir
    opts.add_argument("--profile-directory=Default")

    if not show_window:
        opts.add_argument("--window-position=-32000,-32000")

    # Selenium 4
    opts.set_capability("pageLoadStrategy", "eager")

    service = ChromeService(executable_path=CHROMEDRIVER_PATH)
    driver = webdriver.Chrome(service=service, options=opts)
    driver.set_page_load_timeout(25)
    driver.set_script_timeout(25)
    return driver


# =========================
# SUNAT parseo
# =========================
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


def parse_panel_sunat(panel) -> Dict[str, Any]:
    data: Dict[str, Any] = {}
    items = panel.find_elements(By.CSS_SELECTOR, "div.list-group > div.list-group-item")

    for item in items:
        cols = item.find_elements(By.CSS_SELECTOR, ".row > div")
        if not cols:
            continue

        # label/valor por pares
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
    last_exc: Optional[Exception] = None
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
    Ej: "DNI 78801600 - CANECILLAS CONTRERAS, JUAN MARIANO"
    """
    s = (tipo_documento_text or "").strip()

    m = re.search(r"\bDNI\s*([0-9]{8})\b", s)
    dni = m.group(1) if m else ""

    partes = s.split("-", 1)
    nombre_raw = partes[1].strip() if len(partes) == 2 else ""

    apellidos = ""
    nombres = ""
    if "," in nombre_raw:
        apellidos, nombres = [x.strip() for x in nombre_raw.split(",", 1)]
    else:
        nombres = nombre_raw.strip()

    ap_tokens = [t for t in apellidos.split() if t]
    apellido_paterno = ap_tokens[0] if len(ap_tokens) >= 1 else ""
    apellido_materno = ap_tokens[1] if len(ap_tokens) >= 2 else ""

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
    human_pause(0.8, 1.6)

    old_panels = driver.find_elements(By.CSS_SELECTOR, "div.panel.panel-primary")
    old_panel = old_panels[0] if old_panels else None

    inp = wait.until(EC.presence_of_element_located((By.ID, "txtRuc")))
    type_like_human(inp, ruc, 0.02, 0.07)
    human_pause(0.4, 1.0)

    driver.find_element(By.ID, "btnAceptar").click()

    if old_panel is not None:
        try:
            wait.until(EC.staleness_of(old_panel))
        except TimeoutException:
            pass

    wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "div.panel.panel-primary")))

    data = {"ruc_consultado": ruc}
    data.update(parse_panel_sunat_with_retry(driver))

    tipo_doc = data.get("Tipo de Documento", "")
    if isinstance(tipo_doc, list):
        tipo_doc = tipo_doc[0] if tipo_doc else ""
    data["datos_persona"] = extraer_dni_y_nombres(tipo_doc) if tipo_doc else {
        "dni": "",
        "apellido_paterno": "",
        "apellido_materno": "",
        "primer_nombre": "",
        "segundo_nombre": "",
    }
    return data


# =========================
# SBS (ASISTIDO)
# =========================
def consultar_sbs_afp_asistido(
    driver: webdriver.Chrome,
    wait: WebDriverWait,
    dni: str,
    apellido_paterno: str,
    apellido_materno: str,
    primer_nombre: str,
    segundo_nombre: str = "",
    timeout_resultado: int = 180,
) -> Dict[str, Any]:
    driver.get(URL_SBS)

    wait.until(EC.presence_of_element_located((By.ID, "ctl00_ContentPlaceHolder1_txtNumeroDoc")))
    human_pause(1.0, 2.0)

    type_like_human(driver.find_element(By.ID, "ctl00_ContentPlaceHolder1_txtNumeroDoc"), dni)
    human_pause(0.4, 1.0)

    type_like_human(driver.find_element(By.ID, "ctl00_ContentPlaceHolder1_txtAp_pat"), apellido_paterno)
    human_pause(0.4, 1.0)

    type_like_human(driver.find_element(By.ID, "ctl00_ContentPlaceHolder1_txtAp_mat"), apellido_materno)
    human_pause(0.4, 1.0)

    type_like_human(driver.find_element(By.ID, "ctl00_ContentPlaceHolder1_txtPri_nom"), primer_nombre)
    human_pause(0.4, 1.0)

    el_seg = driver.find_element(By.ID, "ctl00_ContentPlaceHolder1_txtSeg_nom")
    el_seg.clear()
    if segundo_nombre:
        type_like_human(el_seg, segundo_nombre)
        human_pause(0.4, 1.0)

    btn = driver.find_element(By.ID, "ctl00_ContentPlaceHolder1_btnBuscar")
    driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", btn)
    human_pause(0.6, 1.2)

    print("\n[SBS - CHROME] Ya se llenaron los datos.")
    print("[SBS - CHROME] Ahora haz CLICK en 'Buscar' (reCAPTCHA es invisible).")
    print("[SBS - CHROME] Cuando veas el resultado (o un mensaje), presiona ENTER aquí.\n")
    input()

    end_time = time.time() + timeout_resultado
    while time.time() < end_time:
        if driver.find_elements(By.ID, "ctl00_ContentPlaceHolder1_pnlConfirmar"):
            break
        if "sospechosa" in (driver.page_source or "").lower():
            return {"dni": dni, "error": "SBS bloqueó la consulta ('consulta sospechosa' / reCAPTCHA no validó)."}
        time.sleep(0.5)

    if not driver.find_elements(By.ID, "ctl00_ContentPlaceHolder1_pnlConfirmar"):
        return {"dni": dni, "error": "No apareció el panel de resultados (pnlConfirmar) en el tiempo esperado."}

    def safe_text(sel_id: str) -> str:
        try:
            return driver.find_element(By.ID, sel_id).text.strip()
        except Exception:
            return ""

    return {
        "dni": dni,
        "info_al": safe_text("ctl00_ContentPlaceHolder1_lblFechaReg"),
        "afiliado_desde": safe_text("ctl00_ContentPlaceHolder1_lblFec_ing"),
        "afp_actual": safe_text("ctl00_ContentPlaceHolder1_lblAfp_act"),
        "codigo_spp": safe_text("ctl00_ContentPlaceHolder1_lblCod_afi"),
        "situacion": safe_text("ctl00_ContentPlaceHolder1_lblSituacion"),
        "devengue_ultimo_aporte": safe_text("ctl00_ContentPlaceHolder1_lblFec_dev"),
        "detalle_situacion": safe_text("ctl00_ContentPlaceHolder1_TxtBoxSit_Det"),
        "detalle_aportes_obligatorios": safe_text("ctl00_ContentPlaceHolder1_TxtBoxApor_Obl"),
    }


# =========================
# Orquestación: Chrome(SUNAT) -> Chrome(SBS) (2 perfiles)
# =========================
def consulta_completa_dos_chromes(ruc: str, show_windows: bool = True) -> Dict[str, Any]:
    salida: Dict[str, Any] = {"SUNAT": {}, "AFP": {}}

    # 1) SUNAT en Chrome (perfil SUNAT)
    chrome1 = build_chrome_driver(user_data_dir=PROFILE_SUNAT_DIR, show_window=show_windows)
    wait1 = WebDriverWait(chrome1, 30)
    try:
        sunat = consultar_sunat(chrome1, wait1, ruc)
        salida["SUNAT"] = {"RESULTADOS": sunat}
    except Exception as e:
        salida["SUNAT"] = {"ERROR": f"{type(e).__name__}: {e}"}
        chrome1.quit()
        return salida
    finally:
        chrome1.quit()

    persona = (salida["SUNAT"].get("RESULTADOS") or {}).get("datos_persona", {}) or {}
    dni = persona.get("dni", "")
    ap_pat = persona.get("apellido_paterno", "")
    ap_mat = persona.get("apellido_materno", "")
    pri_nom = persona.get("primer_nombre", "")
    seg_nom = persona.get("segundo_nombre", "")

    if not dni or not ap_pat or not pri_nom:
        salida["AFP"] = {"ERROR": "No se pudo extraer DNI/nombres desde SUNAT (Tipo de Documento)."}
        return salida

    # Pausa entre sitios
    human_pause(2.0, 4.0)

    # 2) SBS en Chrome (perfil SBS independiente)
    chrome2 = build_chrome_driver(user_data_dir=PROFILE_SBS_DIR, show_window=show_windows)
    wait2 = WebDriverWait(chrome2, 30)
    try:
        afp = consultar_sbs_afp_asistido(chrome2, wait2, dni, ap_pat, ap_mat, pri_nom, seg_nom)
        if "error" in afp:
            salida["AFP"] = {"ERROR": afp["error"], "DEBUG": afp}
        else:
            salida["AFP"] = {"RESULTADOS": afp}
    finally:
        chrome2.quit()

    return salida


if __name__ == "__main__":
    ruc = "10788016005"
    resultado = consulta_completa_dos_chromes(ruc, show_windows=True)

    print("\nSUNAT\n")
    for k, v in resultado.get("SUNAT", {}).items():
        print(f"{k}: {v}")

    print("\nAFP\n")
    for k, v in resultado.get("AFP", {}).items():
        print(f"{k}: {v}")
