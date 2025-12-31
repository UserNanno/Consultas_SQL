Me salio esto:

(venv) D:\Datos de Usuarios\T72496\Desktop\PrismaProject\dist>main.exe
Traceback (most recent call last):
  File "selenium\webdriver\common\driver_finder.py", line 67, in _binary_paths
  File "selenium\webdriver\common\selenium_manager.py", line 46, in binary_paths
  File "selenium\webdriver\common\selenium_manager.py", line 99, in _get_binary
selenium.common.exceptions.WebDriverException: Message: Unable to obtain working Selenium Manager binary; C:\Users\T72496\AppData\Local\Temp\_MEI138922\selenium\webdriver\common\windows\selenium-manager.exe


The above exception was the direct cause of the following exception:

Traceback (most recent call last):
  File "main.py", line 249, in <module>
  File "main.py", line 47, in wrapper
  File "main.py", line 191, in main
  File "selenium\webdriver\edge\webdriver.py", line 48, in __init__
  File "selenium\webdriver\chromium\webdriver.py", line 51, in __init__
  File "selenium\webdriver\common\driver_finder.py", line 47, in get_browser_path
  File "selenium\webdriver\common\driver_finder.py", line 78, in _binary_paths
selenium.common.exceptions.NoSuchDriverException: Message: Unable to obtain driver for MicrosoftEdge; For documentation on this error, please visit: https://www.selenium.dev/documentation/webdriver/troubleshooting/errors/driver_location

[PYI-18184:ERROR] Failed to execute script 'main' due to unhandled exception!

(venv) D:\Datos de Usuarios\T72496\Desktop\PrismaProject\dist>



