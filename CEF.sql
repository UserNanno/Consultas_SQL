(venv) D:\Datos de Usuarios\T72496\Desktop\MODELOS_RPTs\WebAutomatic>py reconocimiento.py
Traceback (most recent call last):
  File "D:\Datos de Usuarios\T72496\Desktop\MODELOS_RPTs\venv\Lib\site-packages\selenium\webdriver\common\driver_finder.py", line 67, in _binary_paths
    output = SeleniumManager().binary_paths(self._to_args())
  File "D:\Datos de Usuarios\T72496\Desktop\MODELOS_RPTs\venv\Lib\site-packages\selenium\webdriver\common\selenium_manager.py", line 54, in binary_paths
    return self._run(args)
           ~~~~~~~~~^^^^^^
  File "D:\Datos de Usuarios\T72496\Desktop\MODELOS_RPTs\venv\Lib\site-packages\selenium\webdriver\common\selenium_manager.py", line 131, in _run
    raise WebDriverException(
        f"Unsuccessful command executed: {command}; code: {completed_proc.returncode}\n{result}\n{stderr}"
    )
selenium.common.exceptions.WebDriverException: Message: Unsuccessful command executed: D:\Datos de Usuarios\T72496\Desktop\MODELOS_RPTs\venv\Lib\site-packages\selenium\webdriver\common\windows\selenium-manager.exe --browser chrome --language-binding python --output json; code: 65
{'code': 65, 'message': 'Unsuccessful response (403 Forbidden) for URL https://storage.googleapis.com/chrome-for-testing-public/143.0.7499.169/win64/chromedriver-win64.zip', 'driver_path': '', 'browser_path': ''}



The above exception was the direct cause of the following exception:

Traceback (most recent call last):
  File "D:\Datos de Usuarios\T72496\Desktop\MODELOS_RPTs\WebAutomatic\reconocimiento.py", line 58, in <module>
    main()
    ~~~~^^
  File "D:\Datos de Usuarios\T72496\Desktop\MODELOS_RPTs\WebAutomatic\reconocimiento.py", line 21, in main
    driver = webdriver.Chrome(options=options)
  File "D:\Datos de Usuarios\T72496\Desktop\MODELOS_RPTs\venv\Lib\site-packages\selenium\webdriver\chrome\webdriver.py", line 46, in __init__
    super().__init__(
    ~~~~~~~~~~~~~~~~^
        browser_name=DesiredCapabilities.CHROME["browserName"],
        ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    ...<3 lines>...
        keep_alive=keep_alive,
        ^^^^^^^^^^^^^^^^^^^^^^
    )
    ^
  File "D:\Datos de Usuarios\T72496\Desktop\MODELOS_RPTs\venv\Lib\site-packages\selenium\webdriver\chromium\webdriver.py", line 51, in __init__
    if finder.get_browser_path():
       ~~~~~~~~~~~~~~~~~~~~~~~^^
  File "D:\Datos de Usuarios\T72496\Desktop\MODELOS_RPTs\venv\Lib\site-packages\selenium\webdriver\common\driver_finder.py", line 47, in get_browser_path
    return self._binary_paths()["browser_path"]
           ~~~~~~~~~~~~~~~~~~^^
  File "D:\Datos de Usuarios\T72496\Desktop\MODELOS_RPTs\venv\Lib\site-packages\selenium\webdriver\common\driver_finder.py", line 78, in _binary_paths
    raise NoSuchDriverException(msg) from err
selenium.common.exceptions.NoSuchDriverException: Message: Unable to obtain driver for chrome; For documentation on this error, please visit: https://www.selenium.dev/documentation/webdriver/troubleshooting/errors/driver_location


(venv) D:\Datos de Usuarios\T72496\Desktop\MODELOS_RPTs\WebAutomatic>
