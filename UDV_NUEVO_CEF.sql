(venv) D:\Datos de Usuarios\T72496\Desktop\PrismaProject>pyinstaller --onefile --noconsole ^ --name PrismaProject ^ --collect-all selenium ^ --collect-all openpyxl ^ --collect-all PIL ^ --hidden-import openpyxl.cell._writer ^ --add-binary "%VENV%\Lib\site-packages\selenium\webdriver\common\windows\selenium-manager.exe;selenium\webdriver\common\windows" ^ app\gui_app.py
207 INFO: PyInstaller: 6.17.0, contrib hooks: unknown
207 INFO: Python: 3.12.0
288 INFO: Platform: Windows-11-10.0.22631-SP0
288 INFO: Python environment: D:\Datos de Usuarios\T72496\Desktop\PrismaProject\venv
290 INFO: wrote D:\Datos de Usuarios\T72496\Desktop\PrismaProject\PrismaProject.spec
3133 INFO: Module search paths (PYTHONPATH):
['D:\\Datos de '
 'Usuarios\\T72496\\Desktop\\PrismaProject\\venv\\Scripts\\pyinstaller.exe',
 'C:\\Users\\T72496\\AppData\\Local\\Programs\\Python\\Python312\\python312.zip',
 'C:\\Users\\T72496\\AppData\\Local\\Programs\\Python\\Python312\\DLLs',
 'C:\\Users\\T72496\\AppData\\Local\\Programs\\Python\\Python312\\Lib',
 'C:\\Users\\T72496\\AppData\\Local\\Programs\\Python\\Python312',
 'D:\\Datos de Usuarios\\T72496\\Desktop\\PrismaProject\\venv',
 'D:\\Datos de '
 'Usuarios\\T72496\\Desktop\\PrismaProject\\venv\\Lib\\site-packages',
 'D:\\Datos de '
 'Usuarios\\T72496\\Desktop\\PrismaProject\\venv\\Lib\\site-packages\\win32',
 'D:\\Datos de '
 'Usuarios\\T72496\\Desktop\\PrismaProject\\venv\\Lib\\site-packages\\win32\\lib',
 'D:\\Datos de '
 'Usuarios\\T72496\\Desktop\\PrismaProject\\venv\\Lib\\site-packages\\Pythonwin',
 'D:\\Datos de Usuarios\\T72496\\Desktop\\PrismaProject']
3640 INFO: Appending 'binaries' from .spec
ERROR: Unable to find 'D:\\Datos de Usuarios\\T72496\\Desktop\\PrismaProject\\%VENV%\\Lib\\site-packages\\selenium\\webdriver\\common\\windows\\selenium-manager.exe' when adding binary and data files.

(venv) D:\Datos de Usuarios\T72496\Desktop\PrismaProject>
