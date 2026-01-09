from pathlib import Path
import logging

from pages.sbs.cerrar_sesiones_page import CerrarSesionesPage
from pages.sbs.login_page import LoginPage
from pages.sbs.riesgos_page import RiesgosPage
from pages.copilot_page import CopilotPage
from services.copilot_service import CopilotService
from config.settings import URL_LOGIN


class SbsFlow:
    # Pre-step debe correr SOLO una vez por ejecución del proceso
    _prestep_done: bool = False

    def __init__(self, driver, usuario: str, clave: str):
        self.driver = driver
        self.usuario = usuario
        self.clave = clave

    def _pre_cerrar_sesion_activa(self):
        logging.info("[SBS] Pre-step: cerrar sesión activa (cerrarSesiones.jsf)")

        try:
            self.driver.delete_all_cookies()
        except Exception:
            pass

        page = CerrarSesionesPage(self.driver)
        page.open()
        outcome = page.cerrar_sesion(self.usuario, self.clave)

        if outcome == "CERRADA":
            logging.info("[SBS] Pre-step OK: sesión activa cerrada")
        else:
            logging.info("[SBS] Pre-step OK: no existían sesiones activas (continuar)")

    def run(
        self,
        dni: str,
        captcha_img_path: Path,
        detallada_img_path: Path,
        otros_img_path: Path,
    ) -> dict:
        # ==========================================================
        # 0) PRE-STEP: Cerrar sesión activa (SOLO 1 VEZ POR EJECUCIÓN)
        # ==========================================================
        if not SbsFlow._prestep_done:
            try:
                self._pre_cerrar_sesion_activa()
                SbsFlow._prestep_done = True
            except Exception as e:
                logging.warning("[SBS] Pre-step cerrar sesión falló, se continúa igual. Detalle=%r", e)
        else:
            logging.info("[SBS] Pre-step omitido (ya se ejecutó en esta corrida)")

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

        # ==========================================================
        # 1.A0) CAMINO "SIN SALDOS" / "SOLO OTROS REPORTES"
        # - Detallada NO habilitada
        # - Histórica NO habilitada
        # - Consolidado muestra "no presenta saldos"
        # - Otros Reportes SÍ habilitado
        # -> Saltar el paso "Detallada screenshot" y ir directo a Otros Reportes
        # ==========================================================
        if (not riesgos.detallada_habilitada()) and (not riesgos.historica_habilitada()) and riesgos.otros_reportes_habilitado():
            if riesgos.mensaje_no_saldos():
                logging.warning(
                    "[SBS] Modo SIN_SALDOS/SOLO_OTROS: no hay Posición Consolidada; se omite Detallada y se va directo a Otros Reportes."
                )

                datos_deudor = {}
                try:
                    datos_deudor = riesgos.extract_datos_deudor()
                except Exception as e:
                    logging.warning("[SBS] No se pudo extraer Datos del Deudor (SIN_SALDOS): %r", e)

                # Evidencia del consolidado (con el mensaje). Reusamos detallada_img_path para no romper Excel.
                try:
                    riesgos.screenshot_contenido(str(detallada_img_path))
                except Exception:
                    try:
                        self.driver.save_screenshot(str(detallada_img_path))
                    except Exception:
                        pass

                # Ir a Otros Reportes y mantener lógica no bloqueante
                try:
                    riesgos.go_otros_reportes()

                    disponible = riesgos.otros_reportes_disponible()
                    logging.info("[SBS] Otros Reportes disponible=%s (SIN_SALDOS)", disponible)

                    loaded = riesgos.click_carteras_transferidas()
                    logging.info("[SBS] Carteras Transferidas loaded=%s (SIN_SALDOS)", loaded)

                    if loaded and riesgos.has_carteras_table():
                        riesgos.expand_all_rectificaciones(expected=2)

                    riesgos.screenshot_contenido(str(otros_img_path))

                except Exception as e:
                    logging.warning("[SBS] Otros Reportes falló (SIN_SALDOS): %r", e)
                    try:
                        riesgos.screenshot_contenido(str(otros_img_path))
                    except Exception:
                        pass

                # Logout y retorno parcial
                try:
                    logging.info("[SBS] Logout módulo")
                    riesgos.logout_modulo()
                    logging.info("[SBS] Logout portal")
                    riesgos.logout_portal()
                except Exception:
                    pass

                logging.info("[SBS] Fin flujo OK (modo SIN_SALDOS)")
                return {
                    "datos_deudor": datos_deudor,
                    "posicion": [],  # no existe posición consolidada en este caso
                    "modo": "SIN_SALDOS",
                }

        # ==========================================================
        # 1.A) CAMINO "HISTORICA" (Detallada NO habilitada)
        # ==========================================================
        if (not riesgos.detallada_habilitada()) and riesgos.historica_habilitada():
            logging.warning(
                "[SBS] Modo HISTORICA: Detallada no está habilitada. Se captura evidencia y se continúa sin Posición Consolidada."
            )

            # Asegurar que estamos en Histórica
            try:
                riesgos.go_historica()
            except Exception:
                pass

            # En Histórica sí existe "Datos del Deudor" -> lo extraemos si se puede
            datos_deudor = {}
            try:
                datos_deudor = riesgos.extract_datos_deudor()
            except Exception as e:
                logging.warning("[SBS] No se pudo extraer Datos del Deudor en Histórica: %r", e)

            # Evidencia principal (guardamos en detallada_img_path aunque sea Histórica)
            try:
                riesgos.screenshot_contenido(str(detallada_img_path))
            except Exception:
                try:
                    self.driver.save_screenshot(str(detallada_img_path))
                except Exception:
                    pass

            # Intentar Otros Reportes (puede estar habilitado)
            try:
                riesgos.go_otros_reportes()
                disponible = riesgos.otros_reportes_disponible()
                logging.info("[SBS] Otros Reportes disponible=%s (modo Histórica)", disponible)

                loaded = riesgos.click_carteras_transferidas()
                logging.info("[SBS] Carteras Transferidas loaded=%s (modo Histórica)", loaded)

                if loaded and riesgos.has_carteras_table():
                    riesgos.expand_all_rectificaciones(expected=2)

                riesgos.screenshot_contenido(str(otros_img_path))
            except Exception as e:
                logging.warning("[SBS] Otros Reportes falló en modo Histórica: %r", e)
                try:
                    riesgos.screenshot_contenido(str(otros_img_path))
                except Exception:
                    pass

            # Logout y retorno parcial
            try:
                logging.info("[SBS] Logout módulo")
                riesgos.logout_modulo()
                logging.info("[SBS] Logout portal")
                riesgos.logout_portal()
            except Exception:
                pass

            logging.info("[SBS] Fin flujo OK (modo HISTORICA)")
            return {
                "datos_deudor": datos_deudor,
                "posicion": [],  # no hay posición consolidada
                "modo": "HISTORICA",
            }

        # ==========================================================
        # 1.B) CAMINO NORMAL (Consolidado -> Detallada -> Otros Reportes)
        # ==========================================================
        logging.info("[SBS] Extraer datos (modo normal)")
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

        logging.info("[SBS] Fin flujo OK (modo normal)")
        return {
            "datos_deudor": datos_deudor,
            "posicion": posicion,
            "modo": "NORMAL",
        }
