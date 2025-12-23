(venv) D:\Datos de Usuarios\T72496\Desktop\MODELOS_RPTs>pip install pytesseract opencv-python pillow
Requirement already satisfied: pytesseract in d:\datos de usuarios\t72496\desktop\modelos_rpts\venv\lib\site-packages (0.3.13)
Collecting opencv-python
  Using cached opencv_python-4.12.0.88-cp37-abi3-win_amd64.whl.metadata (19 kB)
Requirement already satisfied: pillow in d:\datos de usuarios\t72496\desktop\modelos_rpts\venv\lib\site-packages (12.0.0)
Requirement already satisfied: packaging>=21.3 in d:\datos de usuarios\t72496\desktop\modelos_rpts\venv\lib\site-packages (from pytesseract) (25.0)
Collecting numpy<2.3.0,>=2 (from opencv-python)
  Using cached numpy-2.2.6.tar.gz (20.3 MB)
  Installing build dependencies ... done
  Getting requirements to build wheel ... done
  Installing backend dependencies ... done
  Preparing metadata (pyproject.toml) ... error
  error: subprocess-exited-with-error

  × Preparing metadata (pyproject.toml) did not run successfully.
  │ exit code: 1
  ╰─> [21 lines of output]
      + D:\Datos de Usuarios\T72496\Desktop\MODELOS_RPTs\venv\Scripts\python.exe C:\Users\T72496\AppData\Local\Temp\pip-install-bj8ahike\numpy_c29ea2ee5ac448c6ac72e9646382bf69\vendored-meson\meson\meson.py setup C:\Users\T72496\AppData\Local\Temp\pip-install-bj8ahike\numpy_c29ea2ee5ac448c6ac72e9646382bf69 C:\Users\T72496\AppData\Local\Temp\pip-install-bj8ahike\numpy_c29ea2ee5ac448c6ac72e9646382bf69\.mesonpy-ekf_7_he -Dbuildtype=release -Db_ndebug=if-release -Db_vscrt=md --native-file=C:\Users\T72496\AppData\Local\Temp\pip-install-bj8ahike\numpy_c29ea2ee5ac448c6ac72e9646382bf69\.mesonpy-ekf_7_he\meson-python-native-file.ini
      The Meson build system
      Version: 1.5.2
      Source dir: C:\Users\T72496\AppData\Local\Temp\pip-install-bj8ahike\numpy_c29ea2ee5ac448c6ac72e9646382bf69
      Build dir: C:\Users\T72496\AppData\Local\Temp\pip-install-bj8ahike\numpy_c29ea2ee5ac448c6ac72e9646382bf69\.mesonpy-ekf_7_he
      Build type: native build
      Project name: NumPy
      Project version: 2.2.6
      WARNING: Failed to activate VS environment: Could not find C:\Program Files (x86)\Microsoft Visual Studio\Installer\vswhere.exe

      ..\meson.build:1:0: ERROR: Unknown compiler(s): [['icl'], ['cl'], ['cc'], ['gcc'], ['clang'], ['clang-cl'], ['pgcc']]
      The following exception(s) were encountered:
      Running `icl ""` gave "[WinError 2] El sistema no puede encontrar el archivo especificado"
      Running `cl /?` gave "[WinError 2] El sistema no puede encontrar el archivo especificado"
      Running `cc --version` gave "[WinError 2] El sistema no puede encontrar el archivo especificado"
      Running `gcc --version` gave "[WinError 2] El sistema no puede encontrar el archivo especificado"
      Running `clang --version` gave "[WinError 2] El sistema no puede encontrar el archivo especificado"
      Running `clang-cl /?` gave "[WinError 2] El sistema no puede encontrar el archivo especificado"
      Running `pgcc --version` gave "[WinError 2] El sistema no puede encontrar el archivo especificado"

      A full log can be found at C:\Users\T72496\AppData\Local\Temp\pip-install-bj8ahike\numpy_c29ea2ee5ac448c6ac72e9646382bf69\.mesonpy-ekf_7_he\meson-logs\meson-log.txt
      [end of output]

  note: This error originates from a subprocess, and is likely not a problem with pip.
error: metadata-generation-failed

× Encountered error while generating package metadata.
╰─> numpy

note: This is an issue with the package mentioned above, not pip.
hint: See above for details.

(venv) D:\Datos de Usuarios\T72496\Desktop\MODELOS_RPTs>
