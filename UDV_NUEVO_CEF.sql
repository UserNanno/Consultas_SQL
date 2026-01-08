from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select
from selenium.common.exceptions import TimeoutException, NoSuchElementException

from pages.base_page import BasePage


class RiesgosPage(BasePage):
    """
    Página post-login donde existe el link al módulo (criesgos),
    y luego la pantalla del formulario + resultados (tablas).
    """

    MENU = (By.ID, "Menu")
    TAB_DETALLADA = (By.ID, "idOp4")
    TAB_OTROS_REPORTES = (By.ID, "idOp6")

    # Contenedor de contenido (para screenshot de solo tablas)
    CONTENIDO = (By.ID, "Contenido")

    # Bloque "Otros Reportes" (tabla simple con lista)
    OTROS_LIST = (By.ID, "OtrosReportes")
    LNK_CARTERAS_TRANSFERIDAS = (By.XPATH, "//ul[@id='OtrosReportes']//a[normalize-space()='Carteras Transferidas']")

    # Tabla adicional (solo aparece en algunos DNI, pero en la vista de Carteras Transferidas)
    TBL_CARTERAS = (By.CSS_SELECTOR, "table#expand.Crw")

    # Flechas expand (dentro de la tabla #expand)
    ARROWS = (By.CSS_SELECTOR, "table#expand div.arrow[title*='Rectificaciones']")

    LINK_MODULO = (
        By.CSS_SELECTOR,
        "a.descripcion[onclick*=\"/criesgos/criesgos/criesgos.jsp\"]"
    )

    SELECT_TIPO_DOC = (By.ID, "as_tipo_doc")
    INPUT_DOC = (By.CSS_SELECTOR, "input[name='as_doc_iden']")
    BTN_CONSULTAR = (By.ID, "btnConsultar")

    # Tablas de resultado (por su header visible)
    TBL_DATOS_DEUDOR = (
        By.XPATH,
        "//table[contains(@class,'Crw')][.//b[contains(@class,'F') and contains(normalize-space(.),'Datos del Deudor')]]"
    )
    TBL_POSICION = (
        By.XPATH,
        "//table[contains(@class,'Crw')][.//span[contains(@class,'F') and contains(normalize-space(.),'Posición Consolidada del Deudor')]]"
    )

    # LOGOUT (2 PASOS) ===
    # 1) Sale del módulo riesgos
    LINK_SALIR_CRIESGOS = (By.CSS_SELECTOR, "a[href*='/criesgos/logout?c_c_producto=00002']")

    # 2) Sale del portal
    BTN_SALIR_PORTAL = (By.CSS_SELECTOR, "a[onclick*=\"goTo('logout')\"]")

    def open_modulo_deuda(self):
        # Click al link del módulo
        link = self.wait.until(EC.element_to_be_clickable(self.LINK_MODULO))
        link.click()

    def consultar_por_dni(self, dni: str):
        # Seleccionar tipo doc = DNI (value=11)
        sel = self.wait.until(EC.presence_of_element_located(self.SELECT_TIPO_DOC))
        Select(sel).select_by_value("11")

        # Escribir DNI
        inp = self.wait.until(EC.element_to_be_clickable(self.INPUT_DOC))
        inp.click()
        inp.clear()
        inp.send_keys(dni)

        # Click Consultar
        btn = self.wait.until(EC.element_to_be_clickable(self.BTN_CONSULTAR))
        btn.click()

    def go_detallada(self):
       self.wait.until(EC.presence_of_element_located(self.MENU))
       self.wait.until(EC.element_to_be_clickable(self.TAB_DETALLADA)).click()
       self._wait_tab_loaded()

    def go_otros_reportes(self):
       self.wait.until(EC.presence_of_element_located(self.MENU))
       self.wait.until(EC.element_to_be_clickable(self.TAB_OTROS_REPORTES)).click()
       # Espera a que cargue el contenido base
       self.wait.until(EC.presence_of_element_located(self.CONTENIDO))

    def click_carteras_transferidas(self):
        """
        Desde 'Otros Reportes' (buscaotrosreportes...), entra a 'Carteras Transferidas'
        (buscarinfocarterastransferidas...).
        """
        self.wait.until(EC.presence_of_element_located(self.OTROS_LIST))
        self.wait.until(EC.element_to_be_clickable(self.LNK_CARTERAS_TRANSFERIDAS)).click()
        # Espera la tabla #expand o al menos que cambie URL al endpoint esperado (si aplica)
        # No hacemos hard-fail si la URL no cambia por alguna razón; lo importante es el DOM.
        def _loaded(d):
            try:
                # Si existe #expand ya estamos en la vista nueva
                d.find_element(*self.TBL_CARTERAS)
                return True
            except Exception:
                # fallback: URL contiene buscarinfocarterastransferidas
                return "buscarinfocarterastransferidas" in (d.current_url or "")
        self.wait.until(_loaded)
        # Asegurar que el contenido exista
        self.wait.until(EC.presence_of_element_located(self.CONTENIDO))
    def has_carteras_table(self) -> bool:
        """
        Verifica si existe table#expand (Información de Carteras Transferidas) en la vista actual.
        """
        try:
            self.driver.find_element(*self.TBL_CARTERAS)
            return True
        except NoSuchElementException:
            return False
    def expand_all_rectificaciones(self, expected: int = 2):
        """
        Hace click en las flechas 'arrow' dentro de #expand.
        Por defecto intentamos expandir 2 (como mencionaste).
        """
        # Esperar que existan flechas (si no hay, simplemente no hace nada)
        try:
            self.wait.until(lambda d: len(d.find_elements(*self.ARROWS)) > 0)
        except TimeoutException:
            return
        arrows = self.driver.find_elements(*self.ARROWS)
        # click a las primeras `expected` flechas
        for i, arrow in enumerate(arrows[:expected]):
            try:
                self.driver.execute_script("arguments[0].scrollIntoView({block:'center'});", arrow)
                try:
                    arrow.click()
                except Exception:
                    self.driver.execute_script("arguments[0].click();", arrow)
                # Esperar que se muestre la fila detalle siguiente (tr.Vde) asociada:
                # la estructura suele ser: tr.master + tr.Vde (display:none -> display:table-row)
                def _expanded(d):
                    try:
                        master_tr = arrow.find_element(By.XPATH, "./ancestor::tr[contains(@class,'master')]")
                        detail_tr = master_tr.find_element(By.XPATH, "following-sibling::tr[contains(@class,'Vde')][1]")
                        style = (detail_tr.get_attribute("style") or "").lower()
                        # cuando se despliega, suele quitar display:none o poner display:table-row
                        return "display: none" not in style
                    except Exception:
                        return True  # si no podemos verificar, no bloqueamos el flujo
                self.wait.until(_expanded)
            except Exception:
                # no bloqueamos por un arrow que falle
                continue
    def screenshot_contenido(self, out_path):
        """
        Screenshot SOLO del div#Contenido (mejor que pantalla completa).
        """
        el = self.wait.until(EC.presence_of_element_located(self.CONTENIDO))
        self.driver.execute_script("arguments[0].scrollIntoView({block:'start'});", el)
        el.screenshot(str(out_path))

    def _wait_tab_loaded(self):
       """
       Espera simple post-click.
       Como no tenemos selector del contenido, usamos un par de esperas:
       - que el body exista
       - que termine el readyState
       """
       self.wait.until(lambda d: d.execute_script("return document.readyState") == "complete")


    def extract_datos_deudor(self) -> dict:
        """
        Devuelve dict: {campo: valor} usando los <b class="Dz"> como labels
        y <span class="Dz"> como valor (cuando aplique).
        """
        tbl = self.wait.until(EC.presence_of_element_located(self.TBL_DATOS_DEUDOR))
        rows = tbl.find_elements(By.CSS_SELECTOR, "tbody tr")

        data = {}
        for r in rows:
            # Tomamos solo filas con datos (tr.Def típicamente)
            tds = r.find_elements(By.CSS_SELECTOR, "td")
            if len(tds) < 2:
                continue

            # Recorremos en pares label/valor
            i = 0
            while i < len(tds) - 1:
                label_el = None
                try:
                    label_el = tds[i].find_element(By.CSS_SELECTOR, "b.Dz")
                except Exception:
                    pass

                label = (label_el.text.strip() if label_el else tds[i].text.strip())
                value_text = tds[i + 1].text.strip()

                # Si no parece label válido, salta
                if label:
                    # Limpieza típica (espacios/linebreaks)
                    label = " ".join(label.split())
                    value_text = " ".join(value_text.split())
                    data[label] = value_text

                i += 2

        return data

    def extract_posicion_consolidada(self) -> list:
        """
        Devuelve lista de filas: [[concepto, saldo_mn, saldo_me, total], ...]
        """
        tbl = self.wait.until(EC.presence_of_element_located(self.TBL_POSICION))
        trs = tbl.find_elements(By.CSS_SELECTOR, "tbody tr")

        out = []
        for tr in trs:
            tds = tr.find_elements(By.CSS_SELECTOR, "td")
            if len(tds) != 4:
                continue

            row = [" ".join(td.text.split()) for td in tds]
            # Filtrar header tipo "SALDOS / Saldo MN / ..."
            if row[0].upper() == "SALDOS":
                continue

            out.append(row)

        return out

    def logout_modulo(self):
        """
        1) Click en 'Salir' del módulo /criesgos/logout...
        2) Espera a que cargue la pantalla siguiente
        """
        link = self.wait.until(EC.element_to_be_clickable(self.LINK_SALIR_CRIESGOS))
        link.click()

        # tras salir del módulo, debería aparecer el botón de logout del portal
        self.wait.until(EC.presence_of_element_located(self.BTN_SALIR_PORTAL))

    def logout_portal(self):
        """
        Click en el botón final del portal: goTo('logout')
        """
        btn = self.wait.until(EC.element_to_be_clickable(self.BTN_SALIR_PORTAL))
        btn.click()
