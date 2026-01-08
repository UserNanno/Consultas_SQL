from pathlib import Path
import logging

from pages.sbs.cerrar_sesiones_page import CerrarSesionesPage  # ✅ NUEVO
from pages.sbs.login_page import LoginPage
from pages.sbs.riesgos_page import RiesgosPage
from pages.copilot_page import CopilotPage
from services.copilot_service import CopilotService
from config.settings import URL_LOGIN


class SbsFlow:
    def __init__(self, driver, usuario: str, clave: str):
        self.driver = driver
        self.usuario = usuario
        self.clave = clave

    def _pre_cerrar_sesion_activa(self):
        """
        Paso previo obligatorio: cerrar sesión activa en SBS antes del login normal.
        """
        logging.info("[SBS] Pre-step: cerrar sesión activa (cerrarSesiones.jsf)")

        # opcional: limpia cookies antes de ir a cerrar sesiones
        try:
            self.driver.delete_all_cookies()
        except Exception:
            pass

        page = CerrarSesionesPage(self.driver)
        page.open()
        page.cerrar_sesion(self.usuario, self.clave)

        logging.info("[SBS] Pre-step OK: sesión activa cerrada")

    def run(
        self,
        dni: str,
        captcha_img_path: Path,
        detallada_img_path: Path,
        otros_img_path: Path,
    ) -> dict:
        # ==========================================================
        # 0) PRE-STEP: Cerrar sesión activa (siempre al iniciar)
        # ==========================================================
        try:
            self._pre_cerrar_sesion_activa()
        except Exception as e:
            # Decide el comportamiento:
            # - Si quieres que sea OBLIGATORIO: usa "raise"
            # - Si quieres que no bloquee: deja warning y sigue
            logging.warning("[SBS] Pre-step cerrar sesión falló, se continúa igual. Detalle=%r", e)

        # ==========================================================
        # 1) Flujo SBS normal
        # ==========================================================
        logging.info("[SBS] Ir a login")
        self.driver.get(URL_LOGIN)

        login_page = LoginPage(self.driver)

        logging.info("[SBS] Capturar captcha")
        login_page.capture_image(captcha_img_path)

        # --- Copilot en nueva pestaña (y luego cerrarla) ---
        original_handle = self.driver.current_window_handle
        self.driver.switch_to.new_window("tab")
        copilot = CopilotService(CopilotPage(self.driver))

        logging.info("[SBS] Resolver captcha con Copilot")
        captcha = copilot.resolve_captcha(captcha_img_path)

        try:
            self.driver.close()
        except Exception:
            pass
        self.driver.switch_to.window(original_handle)

        logging.info("[SBS] Login (usuario=%s) + ingresar captcha", self.usuario)
        login_page.fill_form(self.usuario, self.clave, captcha)

        riesgos = RiesgosPage(self.driver)

        logging.info("[SBS] Abrir módulo deuda")
        riesgos.open_modulo_deuda()

        logging.info("[SBS] Consultar DNI=%s", dni)
        riesgos.consultar_por_dni(dni)

        logging.info("[SBS] Extraer datos")
        datos_deudor = riesgos.extract_datos_deudor()
        posicion = riesgos.extract_posicion_consolidada()

        logging.info("[SBS] Ir a Detallada + screenshot")
        riesgos.go_detallada()
        self.driver.save_screenshot(str(detallada_img_path))

        logging.info("[SBS] Ir a Otros Reportes")
        riesgos.go_otros_reportes()

        logging.info("[SBS] Intentar Carteras Transferidas (no bloqueante)")
        try:
            disponible = riesgos.otros_reportes_disponible()
            logging.info("[SBS] Otros Reportes disponible=%s", disponible)

            loaded = riesgos.click_carteras_transferidas()
            logging.info("[SBS] Carteras Transferidas loaded=%s", loaded)

            if loaded and riesgos.has_carteras_table():
                logging.info("[SBS] Expandir rectificaciones (si hay)")
                riesgos.expand_all_rectificaciones(expected=2)

            logging.info("[SBS] Screenshot contenido (rápido + fallback)")
            riesgos.screenshot_contenido(str(otros_img_path))

        except Exception as e:
            logging.exception("[SBS] Error en Otros Reportes/Carteras: %r", e)
            riesgos.screenshot_contenido(str(otros_img_path))

        logging.info("[SBS] Logout módulo")
        riesgos.logout_modulo()

        logging.info("[SBS] Logout portal")
        riesgos.logout_portal()

        logging.info("[SBS] Fin flujo OK")
        return {
            "datos_deudor": datos_deudor,
            "posicion": posicion,
        }
