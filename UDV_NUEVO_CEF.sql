(venv) D:\Datos de Usuarios\T72496\Desktop\PrismaProject>py -m app.gui_app.py
Traceback (most recent call last):
  File "<frozen runpy>", line 189, in _run_module_as_main
  File "<frozen runpy>", line 112, in _get_module_details
  File "D:\Datos de Usuarios\T72496\Desktop\PrismaProject\app\gui_app.py", line 1, in <module>
    from ui.main_windows import MainWindow
  File "D:\Datos de Usuarios\T72496\Desktop\PrismaProject\ui\main_windows.py", line 3, in <module>
    from controllers.consulta_controller import ConsultaController
  File "D:\Datos de Usuarios\T72496\Desktop\PrismaProject\controllers\consulta_controller.py", line 5, in <module>
    from main import run_app
ImportError: cannot import name 'run_app' from 'main' (D:\Datos de Usuarios\T72496\Desktop\PrismaProject\main.py)

(venv) D:\Datos de Usuarios\T72496\Desktop\PrismaProject>
