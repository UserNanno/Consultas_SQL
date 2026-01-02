(venv) D:\Datos de Usuarios\T72496\Desktop\PrismaProject>py main.py
Traceback (most recent call last):
  File "D:\Datos de Usuarios\T72496\Desktop\PrismaProject\main.py", line 32, in <module>
    main()
  File "D:\Datos de Usuarios\T72496\Desktop\PrismaProject\utils\decorators.py", line 9, in wrapper
    return fn(*args, **kwargs)
           ^^^^^^^^^^^^^^^^^^^
  File "D:\Datos de Usuarios\T72496\Desktop\PrismaProject\main.py", line 27, in main
    login_page.fill_form(USUARIO, CLAVE, test)
  File "D:\Datos de Usuarios\T72496\Desktop\PrismaProject\pages\login_page.py", line 13, in fill_form
    self.driver.find_element(By.ID, "c_c_test").send_keys(test)
    ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "D:\Datos de Usuarios\T72496\Desktop\PrismaProject\venv\Lib\site-packages\selenium\webdriver\remote\webdriver.py", line 802, in find_element
    return self.execute(Command.FIND_ELEMENT, {"using": by, "value": value})["value"]
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "D:\Datos de Usuarios\T72496\Desktop\PrismaProject\venv\Lib\site-packages\selenium\webdriver\remote\webdriver.py", line 432, in execute
    self.error_handler.check_response(response)
  File "D:\Datos de Usuarios\T72496\Desktop\PrismaProject\venv\Lib\site-packages\selenium\webdriver\remote\errorhandler.py", line 232, in check_response
    raise exception_class(message, screen, stacktrace)
selenium.common.exceptions.NoSuchElementException: Message: no such element: Unable to locate element: {"method":"css selector","selector":"[id="c_c_test"]"}
  (Session info: MicrosoftEdge=143.0.3650.96); For documentation on this error, please visit: https://www.selenium.dev/documentation/webdriver/troubleshooting/errors#nosuchelementexception
Stacktrace:
Symbols not available. Dumping unresolved backtrace:
        0x7ff6280a87d5
        0x7ff628018e44
        0x7ff6284031f2
        0x7ff627e72d9e
        0x7ff627e72ffb
        0x7ff627eae917
        0x7ff627e6a2e5
        0x7ff627eac8de
        0x7ff627e6982a
        0x7ff627e68b33
        0x7ff627e69653
        0x7ff627f522e4
        0x7ff627f6109c
        0x7ff627f5ac7f
        0x7ff628139b37
        0x7ff6280246a6
        0x7ff62801eab4
        0x7ff62801ebf9
        0x7ff628012cbd
        0x7ffdda13259d
        0x7ffddad2af78


(venv) D:\Datos de Usuarios\T72496\Desktop\PrismaProject>
