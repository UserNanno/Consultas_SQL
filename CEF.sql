(venv) D:\Datos de Usuarios\T72496\Desktop\MODELOS_RPTs>pip install easyocr pillow
Collecting easyocr
  Using cached easyocr-1.7.2-py3-none-any.whl.metadata (10 kB)
Requirement already satisfied: pillow in d:\datos de usuarios\t72496\desktop\modelos_rpts\venv\lib\site-packages (12.0.0)
Collecting torch (from easyocr)
  Using cached torch-2.9.1-cp314-cp314-win_amd64.whl.metadata (30 kB)
Collecting torchvision>=0.5 (from easyocr)
  Using cached torchvision-0.24.1-cp314-cp314-win_amd64.whl.metadata (5.9 kB)
Collecting opencv-python-headless (from easyocr)
  Using cached opencv_python_headless-4.12.0.88-cp37-abi3-win_amd64.whl.metadata (20 kB)
Collecting scipy (from easyocr)
  Using cached scipy-1.16.3-cp314-cp314-win_amd64.whl.metadata (60 kB)
Requirement already satisfied: numpy in d:\datos de usuarios\t72496\desktop\modelos_rpts\venv\lib\site-packages (from easyocr) (2.3.4)
Collecting scikit-image (from easyocr)
  Using cached scikit_image-0.26.0-cp314-cp314-win_amd64.whl.metadata (15 kB)
Collecting python-bidi (from easyocr)
  Using cached python_bidi-0.6.7-cp314-cp314-win_amd64.whl.metadata (5.0 kB)
Requirement already satisfied: PyYAML in d:\datos de usuarios\t72496\desktop\modelos_rpts\venv\lib\site-packages (from easyocr) (6.0.3)
Collecting Shapely (from easyocr)
  Using cached shapely-2.1.2-cp314-cp314-win_amd64.whl.metadata (7.1 kB)
Collecting pyclipper (from easyocr)
  Using cached pyclipper-1.4.0-cp314-cp314-win_amd64.whl.metadata (8.8 kB)
Collecting ninja (from easyocr)
  Using cached ninja-1.13.0-py3-none-win_amd64.whl.metadata (5.1 kB)
Collecting filelock (from torch->easyocr)
  Downloading filelock-3.20.1-py3-none-any.whl.metadata (2.1 kB)
Requirement already satisfied: typing-extensions>=4.10.0 in d:\datos de usuarios\t72496\desktop\modelos_rpts\venv\lib\site-packages (from torch->easyocr) (4.15.0)
Collecting sympy>=1.13.3 (from torch->easyocr)
  Downloading sympy-1.14.0-py3-none-any.whl.metadata (12 kB)
Collecting networkx>=2.5.1 (from torch->easyocr)
  Downloading networkx-3.6.1-py3-none-any.whl.metadata (6.8 kB)
Requirement already satisfied: jinja2 in d:\datos de usuarios\t72496\desktop\modelos_rpts\venv\lib\site-packages (from torch->easyocr) (3.1.6)
Collecting fsspec>=0.8.5 (from torch->easyocr)
  Downloading fsspec-2025.12.0-py3-none-any.whl.metadata (10 kB)
Requirement already satisfied: setuptools in d:\datos de usuarios\t72496\desktop\modelos_rpts\venv\lib\site-packages (from torch->easyocr) (80.9.0)
Collecting mpmath<1.4,>=1.1.0 (from sympy>=1.13.3->torch->easyocr)
  Downloading mpmath-1.3.0-py3-none-any.whl.metadata (8.6 kB)
Requirement already satisfied: MarkupSafe>=2.0 in d:\datos de usuarios\t72496\desktop\modelos_rpts\venv\lib\site-packages (from jinja2->torch->easyocr) (3.0.3)
Collecting numpy (from easyocr)
  Using cached numpy-2.2.6.tar.gz (20.3 MB)
  Installing build dependencies ... done
  Getting requirements to build wheel ... done
  Installing backend dependencies ... done
  Preparing metadata (pyproject.toml) ... error
  error: subprocess-exited-with-error

  × Preparing metadata (pyproject.toml) did not run successfully.
  │ exit code: 1
  ╰─> [21 lines of output]
      + D:\Datos de Usuarios\T72496\Desktop\MODELOS_RPTs\venv\Scripts\python.exe C:\Users\T72496\AppData\Local\Temp\pip-install-425pwn5t\numpy_1a47f0d0a0f34fa9a2a4c71cc3294da6\vendored-meson\meson\meson.py setup C:\Users\T72496\AppData\Local\Temp\pip-install-425pwn5t\numpy_1a47f0d0a0f34fa9a2a4c71cc3294da6 C:\Users\T72496\AppData\Local\Temp\pip-install-425pwn5t\numpy_1a47f0d0a0f34fa9a2a4c71cc3294da6\.mesonpy-v8boe3f7 -Dbuildtype=release -Db_ndebug=if-release -Db_vscrt=md --native-file=C:\Users\T72496\AppData\Local\Temp\pip-install-425pwn5t\numpy_1a47f0d0a0f34fa9a2a4c71cc3294da6\.mesonpy-v8boe3f7\meson-python-native-file.ini
      The Meson build system
      Version: 1.5.2
      Source dir: C:\Users\T72496\AppData\Local\Temp\pip-install-425pwn5t\numpy_1a47f0d0a0f34fa9a2a4c71cc3294da6
      Build dir: C:\Users\T72496\AppData\Local\Temp\pip-install-425pwn5t\numpy_1a47f0d0a0f34fa9a2a4c71cc3294da6\.mesonpy-v8boe3f7
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

      A full log can be found at C:\Users\T72496\AppData\Local\Temp\pip-install-425pwn5t\numpy_1a47f0d0a0f34fa9a2a4c71cc3294da6\.mesonpy-v8boe3f7\meson-logs\meson-log.txt
      [end of output]

  note: This error originates from a subprocess, and is likely not a problem with pip.
error: metadata-generation-failed

× Encountered error while generating package metadata.
╰─> numpy

note: This is an issue with the package mentioned above, not pip.
hint: See above for details.

(venv) D:\Datos de Usuarios\T72496\Desktop\MODELOS_RPTs>
