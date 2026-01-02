from pathlib import Path

# URLs
URL_SUNAT = "https://e-consultaruc.sunat.gob.pe/cl-ti-itmrconsruc/FrameCriterioBusquedaWeb.jsp"
URL_AFP = "https://servicios.sbs.gob.pe/ReporteSituacionPrevisional/Afil_Consulta.aspx"

# Driver
CHROMEDRIVER_PATH = r"D:\Datos de Usuarios\T72496\Downloads\chromedriver-win64\chromedriver-win64\chromedriver.exe"

# Perfiles (se crean solos)
PROFILE_SUNAT_DIR = r"D:\selenium_profiles\sunat"
PROFILE_AFP_DIR   = r"D:\selenium_profiles\afp"











from dataclasses import dataclass

@dataclass(frozen=True)
class Persona:
    dni: str
    apellido_paterno: str
    apellido_materno: str
    primer_nombre: str
    segundo_nombre: str = ""








from dataclasses import dataclass
from typing import Any, Dict, Optional

@dataclass
class SunatResult:
    ruc_consultado: str
    raw: Dict[str, Any]
    persona: Optional[dict]  # dict con dni/nombres extraídos

@dataclass
class AfpResult:
    raw: Dict[str, Any]
    status: str  # "ok" | "suspicious" | "timeout"







import random
import time

def human_pause(a: float = 0.6, b: float = 1.8) -> None:
    time.sleep(random.uniform(a, b))

def type_like_human(el, text: str, min_delay: float = 0.03, max_delay: float = 0.10) -> None:
    el.clear()
    for ch in text:
        el.send_keys(ch)
        time.sleep(random.uniform(min_delay, max_delay))




















import os
from selenium import webdriver
from selenium.webdriver.chrome.options import Options as ChromeOptions
from selenium.webdriver.chrome.service import Service as ChromeService

class ChromeDriverFactory:
    def __init__(self, chromedriver_path: str) -> None:
        self.chromedriver_path = chromedriver_path

    def create(self, user_data_dir: str, show_window: bool = True) -> webdriver.Chrome:
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

        opts.add_argument(f"--user-data-dir={user_data_dir}")
        opts.add_argument("--profile-directory=Default")

        if not show_window:
            opts.add_argument("--window-position=-32000,-32000")

        opts.set_capability("pageLoadStrategy", "eager")

        service = ChromeService(executable_path=self.chromedriver_path)
        driver = webdriver.Chrome(service=service, options=opts)
        driver.set_page_load_timeout(25)
        driver.set_script_timeout(25)
        return driver



















import re
import time
from typing import Any, Dict, List, Optional

from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, StaleElementReferenceException

from core.human import human_pause, type_like_human

class SunatService:
    def __init__(self, url_sunat: str) -> None:
        self.url_sunat = url_sunat

    def consultar_por_ruc(self, driver, wait: WebDriverWait, ruc: str) -> Dict[str, Any]:
        if not re.fullmatch(r"\d{11}", ruc):
            raise ValueError("El RUC debe tener 11 dígitos.")

        driver.get(self.url_sunat)
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
        data.update(self._parse_panel_with_retry(driver))

        tipo_doc = data.get("Tipo de Documento", "")
        if isinstance(tipo_doc, list):
            tipo_doc = tipo_doc[0] if tipo_doc else ""

        data["datos_persona"] = self._extraer_dni_y_nombres(tipo_doc) if tipo_doc else {
            "dni": "",
            "apellido_paterno": "",
            "apellido_materno": "",
            "primer_nombre": "",
            "segundo_nombre": "",
        }
        return data

    # ---------- Parse helpers ----------
    def _parse_panel_with_retry(self, driver, tries: int = 4) -> Dict[str, Any]:
        last_exc: Optional[Exception] = None
        for _ in range(tries):
            try:
                panel = driver.find_element(By.CSS_SELECTOR, "div.panel.panel-primary")
                return self._parse_panel(panel)
            except StaleElementReferenceException as e:
                last_exc = e
                time.sleep(0.2)
        raise last_exc if last_exc else RuntimeError("No se pudo parsear SUNAT (stale).")

    def _clean_label(self, s: str) -> str:
        s = (s or "").strip()
        return re.sub(r":\s*$", "", s)

    def _extract_value(self, cell) -> str:
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

    def _parse_panel(self, panel) -> Dict[str, Any]:
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

                label = self._clean_label(label_el[0].text)
                value = self._extract_value(col_value).strip() or "-"

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

    def _extraer_dni_y_nombres(self, tipo_documento_text: str) -> Dict[str, str]:
        s = (tipo_documento_text or "").strip()
        m = re.search(r"\bDNI\s*([0-9]{8})\b", s)
        dni = m.group(1) if m else ""

        partes = s.split("-", 1)
        nombre_raw = partes[1].strip() if len(partes) == 2 else ""

        apellidos, nombres = ("", "")
        if "," in nombre_raw:
            apellidos, nombres = [x.strip() for x in nombre_raw.split(",", 1)]
        else:
            nombres = nombre_raw.strip()

        ap_tokens = [t for t in apellidos.split() if t]
        nom_tokens = [t for t in nombres.split() if t]

        return {
            "dni": dni,
            "apellido_paterno": ap_tokens[0] if len(ap_tokens) >= 1 else "",
            "apellido_materno": ap_tokens[1] if len(ap_tokens) >= 2 else "",
            "primer_nombre": nom_tokens[0] if len(nom_tokens) >= 1 else "",
            "segundo_nombre": " ".join(nom_tokens[1:]) if len(nom_tokens) >= 2 else "",
        }
































import time
from typing import Any, Dict, Tuple

from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from core.human import human_pause, type_like_human
from models.persona import Persona

class AfpService:
    def __init__(self, url_afp: str) -> None:
        self.url_afp = url_afp

    def consultar_afiliacion(self, driver, wait: WebDriverWait, persona: Persona, intentos: int = 3) -> Tuple[Dict[str, Any], str]:
        driver.get(self.url_afp)
        wait.until(EC.presence_of_element_located((By.ID, "ctl00_ContentPlaceHolder1_txtNumeroDoc")))
        human_pause(1.0, 2.0)

        type_like_human(driver.find_element(By.ID, "ctl00_ContentPlaceHolder1_txtNumeroDoc"), persona.dni)
        human_pause(0.4, 1.0)

        type_like_human(driver.find_element(By.ID, "ctl00_ContentPlaceHolder1_txtAp_pat"), persona.apellido_paterno)
        human_pause(0.4, 1.0)

        type_like_human(driver.find_element(By.ID, "ctl00_ContentPlaceHolder1_txtAp_mat"), persona.apellido_materno)
        human_pause(0.4, 1.0)

        type_like_human(driver.find_element(By.ID, "ctl00_ContentPlaceHolder1_txtPri_nom"), persona.primer_nombre)
        human_pause(0.4, 1.0)

        el_seg = driver.find_element(By.ID, "ctl00_ContentPlaceHolder1_txtSeg_nom")
        el_seg.clear()
        if persona.segundo_nombre:
            type_like_human(el_seg, persona.segundo_nombre)
            human_pause(0.4, 1.0)

        for intento in range(1, intentos + 1):
            print(f"[AFP] Intento {intento}/{intentos}: enviando búsqueda...")
            self._try_submit(driver)

            end = time.time() + 12
            while time.time() < end:
                if driver.find_elements(By.ID, "ctl00_ContentPlaceHolder1_pnlConfirmar"):
                    return self._leer_resultado(driver, persona.dni), "ok"

                if self._is_suspicious_page(driver):
                    return {
                        "dni": persona.dni,
                        "error": "AFP bloqueó la consulta: 'la consulta es sospechosa' (anti-abuso).",
                    }, "suspicious"

                time.sleep(0.5)

            human_pause(1.5, 3.0)

        return {"dni": persona.dni, "error": "No apareció el panel de resultados luego de reintentos."}, "timeout"

    def _try_submit(self, driver) -> None:
        try:
            el = driver.find_element(By.ID, "ctl00_ContentPlaceHolder1_txtNumeroDoc")
            el.send_keys(Keys.ENTER)
        except Exception:
            pass

        human_pause(0.3, 0.8)

        try:
            btn = driver.find_element(By.ID, "ctl00_ContentPlaceHolder1_btnBuscar")
            driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", btn)
            human_pause(0.2, 0.6)
            btn.click()
        except Exception:
            pass

    def _is_suspicious_page(self, driver) -> bool:
        src = (driver.page_source or "").lower()
        return "sospechosa" in src or "consulta es sospechosa" in src

    def _leer_resultado(self, driver, dni: str) -> Dict[str, Any]:
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



























from typing import Any, Dict

from selenium.webdriver.support.ui import WebDriverWait

from models.persona import Persona
from core.human import human_pause
from core.driver_factory import ChromeDriverFactory
from services.sunat_service import SunatService
from services.afp_service import AfpService

class ConsultaOrchestrator:
    def __init__(
        self,
        driver_factory: ChromeDriverFactory,
        sunat_service: SunatService,
        afp_service: AfpService,
        profile_sunat_dir: str,
        profile_afp_dir: str,
    ) -> None:
        self.driver_factory = driver_factory
        self.sunat_service = sunat_service
        self.afp_service = afp_service
        self.profile_sunat_dir = profile_sunat_dir
        self.profile_afp_dir = profile_afp_dir

    def ejecutar(self, ruc: str, show_windows: bool = True) -> Dict[str, Any]:
        salida: Dict[str, Any] = {"SUNAT": {}, "AFP": {}}

        # 1) SUNAT
        d1 = self.driver_factory.create(self.profile_sunat_dir, show_window=show_windows)
        w1 = WebDriverWait(d1, 30)
        try:
            sunat_data = self.sunat_service.consultar_por_ruc(d1, w1, ruc)
            salida["SUNAT"] = {"RESULTADOS": sunat_data}
        except Exception as e:
            salida["SUNAT"] = {"ERROR": f"{type(e).__name__}: {e}"}
            return salida
        finally:
            try:
                d1.quit()
            except Exception:
                pass

        persona_dict = (salida["SUNAT"].get("RESULTADOS") or {}).get("datos_persona") or {}
        if not persona_dict.get("dni") or not persona_dict.get("apellido_paterno") or not persona_dict.get("primer_nombre"):
            salida["AFP"] = {"ERROR": "No se pudo extraer DNI/nombres desde SUNAT (Tipo de Documento)."}
            return salida

        persona = Persona(
            dni=persona_dict.get("dni", ""),
            apellido_paterno=persona_dict.get("apellido_paterno", ""),
            apellido_materno=persona_dict.get("apellido_materno", ""),
            primer_nombre=persona_dict.get("primer_nombre", ""),
            segundo_nombre=persona_dict.get("segundo_nombre", "") or "",
        )

        human_pause(2.0, 4.0)

        # 2) AFP
        d2 = self.driver_factory.create(self.profile_afp_dir, show_window=show_windows)
        w2 = WebDriverWait(d2, 30)
        try:
            afp_data, status = self.afp_service.consultar_afiliacion(d2, w2, persona, intentos=3)

            if status == "ok":
                salida["AFP"] = {"RESULTADOS": afp_data}
                return salida

            if status == "suspicious":
                salida["AFP"] = {"ERROR": afp_data.get("error", "Consulta sospechosa"), "DEBUG": afp_data}
                # opcional: dejar navegador abierto para revisión manual
                print("\n[AFP] Se detectó bloqueo 'consulta sospechosa'. Dejo el navegador abierto.")
                print("[AFP] Ciérralo manualmente y presiona ENTER aquí para finalizar.\n")
                input()
                return salida

            salida["AFP"] = {"ERROR": afp_data.get("error", "Timeout AFP"), "DEBUG": afp_data}
            return salida

        finally:
            # si hubo suspicious, probablemente ya lo cerraste manual; igual intentamos
            try:
                d2.quit()
            except Exception:
                pass


















from config import (
    URL_SUNAT, URL_AFP,
    CHROMEDRIVER_PATH,
    PROFILE_SUNAT_DIR, PROFILE_AFP_DIR
)
from core.driver_factory import ChromeDriverFactory
from services.sunat_service import SunatService
from services.afp_service import AfpService
from orchestrators.consulta_orchestrator import ConsultaOrchestrator

def main():
    ruc = "10788016005"

    driver_factory = ChromeDriverFactory(CHROMEDRIVER_PATH)
    sunat_service = SunatService(URL_SUNAT)
    afp_service = AfpService(URL_AFP)

    orchestrator = ConsultaOrchestrator(
        driver_factory=driver_factory,
        sunat_service=sunat_service,
        afp_service=afp_service,
        profile_sunat_dir=PROFILE_SUNAT_DIR,
        profile_afp_dir=PROFILE_AFP_DIR,
    )

    resultado = orchestrator.ejecutar(ruc, show_windows=True)

    print("\nSUNAT\n", resultado.get("SUNAT"))
    print("\nAFP\n", resultado.get("AFP"))

if __name__ == "__main__":
    main()
