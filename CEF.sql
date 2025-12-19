(venv) D:\Datos de Usuarios\T72496\Desktop\MODELOS_RPTs\WebAutomatic>py main.py
Traceback (most recent call last):
  File "D:\Datos de Usuarios\T72496\Desktop\MODELOS_RPTs\WebAutomatic\main.py", line 178, in <module>
    resultados = consultar_muchos(rucs, headless=False)
  File "D:\Datos de Usuarios\T72496\Desktop\MODELOS_RPTs\WebAutomatic\main.py", line 152, in consultar_muchos
    driver = build_driver(
        headless=headless,
        user_data_dir=None,       # opcional
        profile_dir="Default",
    )
  File "D:\Datos de Usuarios\T72496\Desktop\MODELOS_RPTs\WebAutomatic\main.py", line 83, in build_driver
    driver = webdriver.Chrome(
        service=service,
        options=opts,
        desired_capabilities=caps,
    )
TypeError: WebDriver.__init__() got an unexpected keyword argument 'desired_capabilities'

(venv) D:\Datos de Usuarios\T72496\Desktop\MODELOS_RPTs\WebAutomatic>
