pyinstaller --onefile --noconsole ^
  --collect-all selenium ^
  --add-binary "...\selenium-manager.exe;selenium\webdriver\common\windows" ^
  main.py
