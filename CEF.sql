2026-01-09 10:19:08,489 INFO === LOG INICIALIZADO ===
2026-01-09 10:19:08,490 INFO LOG_PATH=D:\Datos de Usuarios\T10595\Desktop\Prueba\prisma_selenium.log
2026-01-09 10:19:08,490 INFO === INICIO EJECUCION ===
2026-01-09 10:19:08,490 INFO DNI_TITULAR=45859259 | DNI_CONYUGE=43937903
2026-01-09 10:19:08,490 INFO NUMOPORTUNIDAD=O0019282303 | PRODUCTO=CREDITO EFECTIVO | DESPRODUCTO=LD + COMPRA DE DUDA Y/O CONSOLIDACION
2026-01-09 10:19:08,490 INFO APP_DIR=D:\Datos de Usuarios\T10595\Desktop\Prueba
2026-01-09 10:19:08,491 INFO MATANALISTA runtime=T10595
2026-01-09 10:19:08,493 INFO SBS user runtime=T10595
2026-01-09 10:19:12,855 ERROR EXCEPCION:
Traceback (most recent call last):
  File "utils\decorators.py", line 9, in wrapper
  File "main.py", line 105, in run_app
  File "infrastructure\selenium_driver.py", line 15, in create
  File "selenium\webdriver\remote\webdriver.py", line 914, in set_window_size
    self.set_window_rect(width=int(width), height=int(height))
  File "selenium\webdriver\remote\webdriver.py", line 986, in set_window_rect
    return self.execute(Command.SET_WINDOW_RECT, {"x": x, "y": y, "width": width, "height": height})["value"]
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "selenium\webdriver\remote\webdriver.py", line 432, in execute
    self.error_handler.check_response(response)
  File "selenium\webdriver\remote\errorhandler.py", line 232, in check_response
    raise exception_class(message, screen, stacktrace)
selenium.common.exceptions.WebDriverException: Message: unknown error: unhandled inspector error: {"code":-32000,"message":"Browser window not found"}
  (Session info: MicrosoftEdge=143.0.3650.96)
Stacktrace:
Symbols not available. Dumping unresolved backtrace:
	0x7ff750f487d5
	0x7ff750eb8e44
	0x7ff7512a31f2
	0x7ff750cbee5e
	0x7ff750cbd9cf
	0x7ff750cbe14f
	0x7ff750cbe090
	0x7ff750cb148a
	0x7ff750cb35f2
	0x7ff750d606c8
	0x7ff750d323ea
	0x7ff750d0a2e5
	0x7ff750d4c8de
	0x7ff750d0982a
	0x7ff750d08b33
	0x7ff750d09653
	0x7ff750df22e4
	0x7ff750e0109c
	0x7ff750dfac7f
	0x7ff750fd9b37
	0x7ff750ec46a6
	0x7ff750ebeab4
	0x7ff750ebebf9
	0x7ff750eb2cbd
	0x7ffd5ef4259d
	0x7ffd60a4af78
 
 
