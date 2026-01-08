(venv) D:\Datos de Usuarios\T72496\Desktop\PrismaProject>py -m app.gui_app
Traceback (most recent call last):
  File "<frozen runpy>", line 198, in _run_module_as_main
  File "<frozen runpy>", line 88, in _run_code
  File "D:\Datos de Usuarios\T72496\Desktop\PrismaProject\app\gui_app.py", line 1, in <module>
    from ui.main_window import MainWindow
  File "D:\Datos de Usuarios\T72496\Desktop\PrismaProject\ui\main_window.py", line 8, in <module>
    from ui.sbs_credentials_window import SbsCredentialsWindow  # <-- NUEVO
    ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "D:\Datos de Usuarios\T72496\Desktop\PrismaProject\ui\sbs_credentials_window.py", line 4, in <module>
    from config.settings import USUARIO, CLAVE
ImportError: cannot import name 'USUARIO' from 'config.settings' (D:\Datos de Usuarios\T72496\Desktop\PrismaProject\config\settings.py)

(venv) D:\Datos de Usuarios\T72496\Desktop\PrismaProject>
