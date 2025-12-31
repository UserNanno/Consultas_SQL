pyinstaller --onefile --log-level=DEBUG main.py


pyinstaller --onefile --noconsole ^
  --collect-all selenium ^
  --add-binary "venv\Lib\site-packages\selenium\webdriver\common\windows\selenium-manager.exe;selenium\webdriver\common\windows" ^
  main.py
