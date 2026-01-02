pyinstaller --onefile --noconsole ^
  --collect-all selenium ^
  --add-binary "D:\Datos de Usuarios\T72496\Desktop\PrismaProject\venv\Lib\site-packages\selenium\webdriver\common\windows\selenium-manager.exe;selenium\webdriver\common\windows" ^
  main.py
