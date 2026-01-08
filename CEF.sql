2026-01-08 10:14:49,246 INFO === LOG INICIALIZADO ===
2026-01-08 10:14:49,249 INFO LOG_PATH=D:\Datos de Usuarios\T72496\Desktop\PrismaProject\prisma_selenium.log
2026-01-08 10:14:49,250 INFO === INICIO EJECUCION ===
2026-01-08 10:14:49,250 INFO DNI_TITULAR=72811352 | DNI_CONYUGE=
2026-01-08 10:14:49,250 INFO APP_DIR=D:\Datos de Usuarios\T72496\Desktop\PrismaProject
2026-01-08 10:14:53,564 INFO RESULTS_DIR=D:\Datos de Usuarios\T72496\Desktop\PrismaProject\results
2026-01-08 10:14:53,564 INFO OUTPUT_XLSM=D:\Datos de Usuarios\T72496\Desktop\PrismaProject\results\Macro_out_72811352.xlsm
2026-01-08 10:14:53,565 INFO == FLUJO SBS (TITULAR) INICIO ==
2026-01-08 10:14:53,565 INFO [SBS] Ir a login
2026-01-08 10:14:54,774 INFO [SBS] Capturar captcha
2026-01-08 10:14:55,366 INFO [SBS] Resolver captcha con Copilot
2026-01-08 10:15:19,325 INFO [SBS] Login (usuario=T10595) + ingresar captcha
2026-01-08 10:15:25,652 INFO [SBS] Abrir módulo deuda
2026-01-08 10:15:26,584 INFO [SBS] Consultar DNI=72811352
2026-01-08 10:15:28,214 INFO [SBS] Extraer datos
2026-01-08 10:15:30,552 INFO [SBS] Ir a Detallada + screenshot
2026-01-08 10:15:31,528 INFO [SBS] Ir a Otros Reportes
2026-01-08 10:15:32,077 INFO [SBS] Intentar Carteras Transferidas (no bloqueante)
2026-01-08 10:15:32,597 INFO [SBS] Carteras Transferidas loaded=True
2026-01-08 10:15:32,628 INFO [SBS] Expandir rectificaciones (si hay)
2026-01-08 10:15:33,205 INFO [SBS] Screenshot contenido (rápido + fallback)
2026-01-08 10:15:33,534 INFO [SBS] Logout módulo
2026-01-08 10:15:34,091 INFO [SBS] Logout portal
2026-01-08 10:15:34,619 INFO [SBS] Fin flujo OK
2026-01-08 10:15:34,619 INFO == FLUJO SBS (TITULAR) FIN ==
2026-01-08 10:15:34,619 INFO == FLUJO SUNAT (TITULAR) INICIO ==
2026-01-08 10:15:38,596 INFO == FLUJO SUNAT (TITULAR) FIN ==
2026-01-08 10:15:38,596 INFO == FLUJO RBM (TITULAR) INICIO ==
2026-01-08 10:16:13,390 INFO === FIN EJECUCION ===
2026-01-08 10:16:13,408 ERROR EXCEPCION:
Traceback (most recent call last):
  File "D:\Datos de Usuarios\T72496\Desktop\PrismaProject\utils\decorators.py", line 9, in wrapper
    return fn(*args, **kwargs)
           ^^^^^^^^^^^^^^^^^^^
  File "D:\Datos de Usuarios\T72496\Desktop\PrismaProject\main.py", line 148, in run_app
    rbm_titular = RbmFlow(driver).run(
                  ^^^^^^^^^^^^^^^^^^^^
  File "D:\Datos de Usuarios\T72496\Desktop\PrismaProject\services\rbm_flow.py", line 10, in run
    self.page.open()
  File "D:\Datos de Usuarios\T72496\Desktop\PrismaProject\pages\rbm\rbm_page.py", line 28, in open
    self.wait.until(EC.presence_of_element_located(self.SELECT_TIPO_DOC))
  File "D:\Datos de Usuarios\T72496\Desktop\PrismaProject\venv\Lib\site-packages\selenium\webdriver\support\wait.py", line 122, in until
    raise TimeoutException(message, screen, stacktrace)
selenium.common.exceptions.TimeoutException: Message: 
Stacktrace:
Symbols not available. Dumping unresolved backtrace:
	0x7ff68efd87d5
	0x7ff68ef48e44
	0x7ff68f3331f2
	0x7ff68eda2d9e
	0x7ff68eda2ffb
	0x7ff68edde917
	0x7ff68ed9a2e5
	0x7ff68eddc8de
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


