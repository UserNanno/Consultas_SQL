2026-01-08 10:10:20,161 INFO === LOG INICIALIZADO ===
2026-01-08 10:10:20,163 INFO LOG_PATH=D:\Datos de Usuarios\T72496\Desktop\PrismaProject\prisma_selenium.log
2026-01-08 10:10:20,164 INFO === INICIO EJECUCION ===
2026-01-08 10:10:20,164 INFO DNI_TITULAR=78801600 | DNI_CONYUGE=
2026-01-08 10:10:20,164 INFO APP_DIR=D:\Datos de Usuarios\T72496\Desktop\PrismaProject
2026-01-08 10:10:28,692 INFO RESULTS_DIR=D:\Datos de Usuarios\T72496\Desktop\PrismaProject\results
2026-01-08 10:10:28,692 INFO OUTPUT_XLSM=D:\Datos de Usuarios\T72496\Desktop\PrismaProject\results\Macro_out_78801600.xlsm
2026-01-08 10:10:28,692 INFO == FLUJO SBS (TITULAR) INICIO ==
2026-01-08 10:10:28,693 INFO [SBS] Ir a login
2026-01-08 10:10:29,337 INFO [SBS] Capturar captcha
2026-01-08 10:10:30,018 INFO [SBS] Resolver captcha con Copilot
2026-01-08 10:10:49,250 INFO [SBS] Login (usuario=T10595) + ingresar captcha
2026-01-08 10:10:53,450 INFO [SBS] Abrir módulo deuda
2026-01-08 10:10:54,128 INFO [SBS] Consultar DNI=78801600
2026-01-08 10:10:55,478 INFO [SBS] Extraer datos
2026-01-08 10:10:57,312 INFO [SBS] Ir a Detallada + screenshot
2026-01-08 10:10:57,971 INFO [SBS] Ir a Otros Reportes
2026-01-08 10:10:58,544 INFO [SBS] Intentar Carteras Transferidas (no bloqueante)
2026-01-08 10:11:28,799 ERROR [SBS] Error en Otros Reportes/Carteras: TimeoutException()
Traceback (most recent call last):
  File "D:\Datos de Usuarios\T72496\Desktop\PrismaProject\services\sbs_flow.py", line 54, in run
    loaded = riesgos.click_carteras_transferidas()
             ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "D:\Datos de Usuarios\T72496\Desktop\PrismaProject\pages\sbs\riesgos_page.py", line 70, in click_carteras_transferidas
    self.wait.until(EC.presence_of_element_located(self.OTROS_LIST))
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

2026-01-08 10:11:29,028 INFO [SBS] Logout módulo
2026-01-08 10:11:29,618 INFO [SBS] Logout portal
2026-01-08 10:11:30,413 INFO [SBS] Fin flujo OK
2026-01-08 10:11:30,413 INFO == FLUJO SBS (TITULAR) FIN ==
2026-01-08 10:11:30,413 INFO == FLUJO SUNAT (TITULAR) INICIO ==
2026-01-08 10:12:05,747 INFO === FIN EJECUCION ===
2026-01-08 10:12:05,762 ERROR EXCEPCION:
Traceback (most recent call last):
  File "D:\Datos de Usuarios\T72496\Desktop\PrismaProject\utils\decorators.py", line 9, in wrapper
    return fn(*args, **kwargs)
           ^^^^^^^^^^^^^^^^^^^
  File "D:\Datos de Usuarios\T72496\Desktop\PrismaProject\main.py", line 141, in run_app
    SunatFlow(driver).run(dni=dni_titular, out_img_path=sunat_img_path)
  File "D:\Datos de Usuarios\T72496\Desktop\PrismaProject\services\sunat_flow.py", line 9, in run
    self.page.buscar_por_dni(dni)
  File "D:\Datos de Usuarios\T72496\Desktop\PrismaProject\pages\sunat\sunat_page.py", line 25, in buscar_por_dni
    inp = self.wait.until(EC.element_to_be_clickable(self.TXT_NUM_DOC))
          ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "D:\Datos de Usuarios\T72496\Desktop\PrismaProject\venv\Lib\site-packages\selenium\webdriver\support\wait.py", line 122, in until
    raise TimeoutException(message, screen, stacktrace)
selenium.common.exceptions.TimeoutException: Message: 


