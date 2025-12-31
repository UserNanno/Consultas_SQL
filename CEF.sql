empaquetar
pyinstaller --onefile --noconsole ^   --hidden-import selenium ^   --hidden-import selenium.webdriver ^   --add-data "venv\Lib\site-packages\selenium\webdriver\common\windows\selenium-manager.exe;selenium\webdriver\common\windows" ^   main.py
