2026-01-08 18:37:41,199 INFO === LOG INICIALIZADO ===
2026-01-08 18:37:41,199 INFO LOG_PATH=D:\Datos de Usuarios\T72496\Desktop\PrismaProject\prisma_selenium.log
2026-01-08 18:37:41,200 INFO === INICIO EJECUCION ===
2026-01-08 18:37:41,200 INFO DNI_TITULAR=76500660 | DNI_CONYUGE=
2026-01-08 18:37:41,200 INFO NUMOPORTUNIDAD=O0019148418 | PRODUCTO=TARJETA DE CREDITO | DESPRODUCTO=TARJETA NUEVA
2026-01-08 18:37:41,200 INFO APP_DIR=D:\Datos de Usuarios\T72496\Desktop\PrismaProject
2026-01-08 18:37:41,202 INFO MATANALISTA runtime=T72496
2026-01-08 18:37:41,209 INFO SBS user runtime=T10595
2026-01-08 18:37:48,202 INFO RESULTS_DIR=D:\Datos de Usuarios\T72496\Desktop\PrismaProject\results
2026-01-08 18:37:48,202 INFO OUTPUT_XLSM=D:\Datos de Usuarios\T72496\Desktop\PrismaProject\results\O0019148418_T72496.xlsm
2026-01-08 18:37:48,203 INFO == FLUJO SBS (TITULAR) INICIO ==
2026-01-08 18:37:48,203 INFO [SBS] Pre-step: cerrar sesión activa (cerrarSesiones.jsf)
2026-01-08 18:37:50,799 INFO [SBS] Pre-step OK: sesión activa cerrada
2026-01-08 18:37:50,799 INFO [SBS] Ir a login
2026-01-08 18:37:51,165 INFO [SBS] Capturar captcha
2026-01-08 18:37:51,413 INFO [SBS] Resolver captcha con Copilot
2026-01-08 18:38:01,749 INFO [SBS] Login (usuario=T10595) + ingresar captcha
2026-01-08 18:38:04,993 INFO [SBS] Abrir módulo deuda
2026-01-08 18:38:05,931 INFO [SBS] Consultar DNI=76500660
2026-01-08 18:38:07,324 INFO [SBS] Extraer datos
2026-01-08 18:38:10,524 INFO === FIN EJECUCION ===
2026-01-08 18:38:10,532 ERROR EXCEPCION:
Traceback (most recent call last):
  File "D:\Datos de Usuarios\T72496\Desktop\PrismaProject\utils\decorators.py", line 9, in wrapper
    return fn(*args, **kwargs)
           ^^^^^^^^^^^^^^^^^^^
  File "D:\Datos de Usuarios\T72496\Desktop\PrismaProject\main.py", line 143, in run_app
    _ = SbsFlow(driver, sbs_user, sbs_pass).run(
        ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "D:\Datos de Usuarios\T72496\Desktop\PrismaProject\services\sbs_flow.py", line 96, in run
    datos_deudor = riesgos.extract_datos_deudor()
                   ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "D:\Datos de Usuarios\T72496\Desktop\PrismaProject\pages\sbs\riesgos_page.py", line 210, in extract_datos_deudor
    tbl = self.wait.until(EC.presence_of_element_located(self.TBL_DATOS_DEUDOR))
          ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "D:\Datos de Usuarios\T72496\Desktop\PrismaProject\venv\Lib\site-packages\selenium\webdriver\support\wait.py", line 113, in until
    value = method(self._driver)
            ^^^^^^^^^^^^^^^^^^^^
  File "D:\Datos de Usuarios\T72496\Desktop\PrismaProject\venv\Lib\site-packages\selenium\webdriver\support\expected_conditions.py", line 92, in _predicate
    return driver.find_element(*locator)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "D:\Datos de Usuarios\T72496\Desktop\PrismaProject\venv\Lib\site-packages\selenium\webdriver\remote\webdriver.py", line 802, in find_element
    return self.execute(Command.FIND_ELEMENT, {"using": by, "value": value})["value"]
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "D:\Datos de Usuarios\T72496\Desktop\PrismaProject\venv\Lib\site-packages\selenium\webdriver\remote\webdriver.py", line 432, in execute
    self.error_handler.check_response(response)
  File "D:\Datos de Usuarios\T72496\Desktop\PrismaProject\venv\Lib\site-packages\selenium\webdriver\remote\errorhandler.py", line 231, in check_response
    raise exception_class(message, screen, stacktrace, alert_text)  # type: ignore[call-arg]  # mypy is not smart enough here
    ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
selenium.common.exceptions.UnexpectedAlertPresentException: Alert Text: A la última fecha del Reporte no hay Información de Posición Consolidada,

el deudor muestra información en las pantallas siguientes:

- Historica

a continuación se muestra la Información Histórica.
Message: unexpected alert open: {Alert text : A la última fecha del Reporte no hay Información de Posición Consolidada,

el deudor muestra información en las pantallas siguientes:

- Historica

a continuación se muestra la Información Histórica.}
  (Session info: MicrosoftEdge=143.0.3650.96)
Stacktrace:
Symbols not available. Dumping unresolved backtrace:
	0x7ff68efd87d5
	0x7ff68ef48e44
	0x7ff68f3331f2
	0x7ff68eddcd4e
	0x7ff68ed9982a
	0x7ff68ed98b33
	0x7ff68ed99653
	0x7ff68ee822e4
	0x7ff68ee9109c
	0x7ff68ee8ac7f
	0x7ff68f069b37
	0x7ff68ef546a6
	0x7ff68ef4eab4
	0x7ff68ef4ebf9
	0x7ff68ef42cbd
	0x7ff8c03e259d
	0x7ff8c278af78


