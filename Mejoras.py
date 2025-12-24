(venv) D:\Datos de Usuarios\T72496\Desktop\MODELOS_RPTs\WebAutomatic>py autosbs.py
Captura guardada: D:\Datos de Usuarios\T72496\Desktop\MODELOS_RPTs\WebAutomatic\captura.png
Traceback (most recent call last):
  File "D:\Datos de Usuarios\T72496\Desktop\MODELOS_RPTs\WebAutomatic\autosbs.py", line 237, in <module>
    main()
    ~~~~^^
  File "D:\Datos de Usuarios\T72496\Desktop\MODELOS_RPTs\WebAutomatic\autosbs.py", line 208, in main
    wait.until(lambda d: len(d.window_handles) >= 2)
    ~~~~~~~~~~^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "D:\Datos de Usuarios\T72496\Desktop\MODELOS_RPTs\venv\Lib\site-packages\selenium\webdriver\support\wait.py", line 122, in until
    raise TimeoutException(message, screen, stacktrace)
selenium.common.exceptions.TimeoutException: Message:


(venv) D:\Datos de Usuarios\T72496\Desktop\MODELOS_RPTs\WebAutomatic>
