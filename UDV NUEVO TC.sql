Pefecto. Funciona excelente.

Ahora mi necesidad es empaquetarlo en un .exe

para lo cual he usado

pyinstaller --onefile --noconsole ^
  --collect-all selenium ^
  --add-binary "D:\Datos de Usuarios\T72496\Desktop\PrismaProject\venv\Lib\site-packages\selenium\webdriver\common\windows\selenium-manager.exe;selenium\webdriver\common\windows" ^
  main.py

y si lo hace perfecto. Ejecuta bien el .exe

El unico problema es que depende de que macro.xlsm se encuentre en D:\Datos de Usuarios\T72496\Desktop\PrismaProject

Pero este .exe será enviado a diferentes personas con usuarios distintos, por ende, rutas distintas. No se podrá usar el .xlsm como ruta relativa? Que busque el .xlsm en la misma ruta que se ejeucta el .exe? También actualmente se está guardando las imagenes y el excel que se pega las imagenes (macro editada) en: C:\Users\T72496\AppData\Local\Temp\PrismaProject

No habría forma de moverlo a otro lado o que el resultado se coloque dentro de una carpeta results al mismo nivel del .exe?
