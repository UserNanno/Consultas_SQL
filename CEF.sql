import re
from selenium.common.exceptions import TimeoutException

URL_SUNAT = "https://e-consultaruc.sunat.gob.pe/cl-ti-itmrconsruc/FrameCriterioBusquedaWeb.jsp"
URL_SBS = "https://servicios.sbs.gob.pe/ReporteSituacionPrevisional/Afil_Consulta.aspx"


def extraer_dni_y_nombres(tipo_documento_text: str) -> dict:
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


def sunat_get_persona(driver, wait, ruc: str) -> dict:
    if not re.fullmatch(r"\d{11}", ruc):
        raise ValueError("El RUC debe tener 11 dígitos.")

    driver.get(URL_SUNAT)

    inp = wait.until(EC.presence_of_element_located((By.ID, "txtRuc")))
    inp.clear()
    inp.send_keys(ruc)

    driver.find_element(By.ID, "btnAceptar").click()

    # Espera a que aparezca panel resultado
    panel = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "div.panel.panel-primary")))

    # En SUNAT, el campo "Tipo de Documento" aparece en la ficha.
    # Una forma práctica sin parseo complejo: buscar el texto completo del panel
    txt = panel.text

    # Busca línea que contenga "Tipo de Documento"
    tipo_doc_line = ""
    for line in txt.splitlines():
        if "Tipo de Documento" in line:
            # a veces sale "Tipo de Documento DNI 123..." o en líneas separadas
            tipo_doc_line = line
            break

    # Si no está en esa misma línea, intenta hallar "DNI 8digitos - APELLIDOS, NOMBRES"
    if not tipo_doc_line:
        m = re.search(r"DNI\s*\d{8}\s*-\s*[^\n]+", txt)
        tipo_doc_line = m.group(0) if m else ""

    persona = extraer_dni_y_nombres(tipo_doc_line)

    if not persona["dni"]:
        raise RuntimeError("No pude extraer DNI desde SUNAT (Tipo de Documento).")

    return persona


def sbs_get_afp(driver, wait, persona: dict, intentos: int = 3) -> dict:
    driver.get(URL_SBS)

    wait.until(EC.presence_of_element_located((By.ID, "ctl00_ContentPlaceHolder1_txtNumeroDoc")))

    def fill(id_, value):
        el = driver.find_element(By.ID, id_)
        el.clear()
        el.send_keys(value)

    fill("ctl00_ContentPlaceHolder1_txtNumeroDoc", persona["dni"])
    fill("ctl00_ContentPlaceHolder1_txtAp_pat", persona["apellido_paterno"])
    fill("ctl00_ContentPlaceHolder1_txtAp_mat", persona["apellido_materno"])
    fill("ctl00_ContentPlaceHolder1_txtPri_nom", persona["primer_nombre"])

    el_seg = driver.find_element(By.ID, "ctl00_ContentPlaceHolder1_txtSeg_nom")
    el_seg.clear()
    if persona.get("segundo_nombre"):
        el_seg.send_keys(persona["segundo_nombre"])

    for i in range(intentos):
        # Click buscar
        driver.find_element(By.ID, "ctl00_ContentPlaceHolder1_btnBuscar").click()

        # Espera resultado
        try:
            wait_res = WebDriverWait(driver, 12)
            wait_res.until(EC.presence_of_element_located((By.ID, "ctl00_ContentPlaceHolder1_pnlConfirmar")))

            def safe_text(id_):
                try:
                    return driver.find_element(By.ID, id_).text.strip()
                except Exception:
                    return ""

            return {
                "dni": persona["dni"],
                "info_al": safe_text("ctl00_ContentPlaceHolder1_lblFechaReg"),
                "afiliado_desde": safe_text("ctl00_ContentPlaceHolder1_lblFec_ing"),
                "afp_actual": safe_text("ctl00_ContentPlaceHolder1_lblAfp_act"),
                "codigo_spp": safe_text("ctl00_ContentPlaceHolder1_lblCod_afi"),
                "situacion": safe_text("ctl00_ContentPlaceHolder1_lblSituacion"),
                "devengue_ultimo_aporte": safe_text("ctl00_ContentPlaceHolder1_lblFec_dev"),
                "detalle_situacion": safe_text("ctl00_ContentPlaceHolder1_TxtBoxSit_Det"),
                "detalle_aportes_obligatorios": safe_text("ctl00_ContentPlaceHolder1_TxtBoxApor_Obl"),
            }
        except TimeoutException:
            # SBS a veces se “defiende”; reintenta
            time.sleep(1.2)

    raise RuntimeError("SBS: no apareció el panel de resultados (pnlConfirmar) tras reintentos.")










    # --- NUEVO PASO: SUNAT + SBS ---
    ruc = "10788016005"  # luego lo pedirás por GUI/entrada
    # OJO: usa un wait “largo” para estas páginas
    wait_ext = WebDriverWait(driver, 30)

    # Abrimos tab SUNAT
    driver.switch_to.new_window("tab")
    persona = sunat_get_persona(driver, wait_ext, ruc)
    print("SUNAT persona:", persona)

    # Abrimos tab SBS
    driver.switch_to.new_window("tab")
    afp_info = sbs_get_afp(driver, wait_ext, persona)
    print("SBS AFP:", afp_info)

    # Cierra tabs extra y vuelve al flujo normal (opcional)
    # (puedes cerrar 2 tabs recientes y quedarte con la original)
