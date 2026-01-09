2026-01-08 22:34:50,538 INFO === LOG INICIALIZADO ===
2026-01-08 22:34:50,539 INFO LOG_PATH=D:\Datos de Usuarios\T72496\Desktop\PrismaProject\prisma_selenium.log
2026-01-08 22:34:50,539 INFO === INICIO EJECUCION ===
2026-01-08 22:34:50,539 INFO DNI_TITULAR=76500660 | DNI_CONYUGE=
2026-01-08 22:34:50,540 INFO NUMOPORTUNIDAD=O0019098179 | PRODUCTO=CREDITO EFECTIVO | DESPRODUCTO=LD/RE
2026-01-08 22:34:50,540 INFO APP_DIR=D:\Datos de Usuarios\T72496\Desktop\PrismaProject
2026-01-08 22:34:50,542 INFO MATANALISTA runtime=T72496
2026-01-08 22:34:50,548 INFO SBS user runtime=T10595
2026-01-08 22:34:56,572 INFO RESULTS_DIR=D:\Datos de Usuarios\T72496\Desktop\PrismaProject\results
2026-01-08 22:34:56,572 INFO OUTPUT_XLSM=D:\Datos de Usuarios\T72496\Desktop\PrismaProject\results\O0019098179_T72496.xlsm
2026-01-08 22:34:56,599 INFO == FLUJO SBS (TITULAR) INICIO ==
2026-01-08 22:34:56,599 INFO [SBS] Pre-step: cerrar sesión activa (cerrarSesiones.jsf)
2026-01-08 22:35:01,627 INFO [SBS] Pre-step OK: no existían sesiones activas (continuar)
2026-01-08 22:35:01,628 INFO [SBS] Ir a login
2026-01-08 22:35:02,197 INFO [SBS] Capturar captcha
2026-01-08 22:35:02,807 INFO [SBS] Resolver captcha con Copilot
2026-01-08 22:35:36,294 INFO [SBS] Login (usuario=T10595) + ingresar captcha
2026-01-08 22:35:40,472 INFO [SBS] Abrir módulo deuda
2026-01-08 22:35:42,073 INFO [SBS] Consultar DNI=76500660
2026-01-08 22:35:44,221 WARNING [SBS] Alert al consultar DNI=76500660: A la última fecha del Reporte no hay Información de Posición Consolidada,

el deudor muestra información en las pantallas siguientes:

- Historica

a continuación se muestra la Información Histórica.
2026-01-08 22:35:44,222 INFO [SBS] Extraer datos
2026-01-08 22:36:19,563 INFO === FIN EJECUCION ===
2026-01-08 22:36:19,576 ERROR EXCEPCION:
Traceback (most recent call last):
  File "D:\Datos de Usuarios\T72496\Desktop\PrismaProject\utils\decorators.py", line 9, in wrapper
    return fn(*args, **kwargs)
           ^^^^^^^^^^^^^^^^^^^
  File "D:\Datos de Usuarios\T72496\Desktop\PrismaProject\main.py", line 143, in run_app
    _ = SbsFlow(driver, sbs_user, sbs_pass).run(
        ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "D:\Datos de Usuarios\T72496\Desktop\PrismaProject\services\sbs_flow.py", line 97, in run
    posicion = riesgos.extract_posicion_consolidada()
               ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "D:\Datos de Usuarios\T72496\Desktop\PrismaProject\pages\sbs\riesgos_page.py", line 253, in extract_posicion_consolidada
    tbl = self.wait.until(EC.presence_of_element_located(self.TBL_POSICION))
          ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
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


