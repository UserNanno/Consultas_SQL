pyinstaller --onefile --noconsole ^
 --name PrismaProject ^
 --collect-all selenium ^
 --collect-all openpyxl ^
 --collect-all PIL ^
 --hidden-import openpyxl.cell._writer ^
 --hidden-import win32com.client ^
 --hidden-import pythoncom ^
 --hidden-import pywintypes ^
 --add-binary "D:\Datos de Usuarios\T72496\Desktop\PrismaProject\venv\Lib\site-packages\selenium\webdriver\common\windows\selenium-manager.exe;selenium\webdriver\common\windows" ^
 app\gui_app.py
