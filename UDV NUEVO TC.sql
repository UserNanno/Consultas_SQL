Actulamente este es mi config/settings.py

from pathlib import Path
import os
import sys
import tempfile

EDGE_EXE = r"C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe"
DEBUG_PORT = 9223

URL_LOGIN = "https://extranet.sbs.gob.pe/app/login.jsp"
URL_COPILOT = "https://m365.cloud.microsoft/chat/?auth=2"
URL_SUNAT = "https://e-consultaruc.sunat.gob.pe/cl-ti-itmrconsruc/FrameCriterioBusquedaWeb.jsp"

USUARIO = "T10595"
CLAVE = "44445555"  # solo números

# DNI a consultar en el módulo
DNI_CONSULTA = "72811352"
# DNI_CONYUGE_CONSULTA = "" -> FALTA IMPLEMENTAR

# Base dir (por si empaquetas)
if getattr(sys, "frozen", False):
    BASE_DIR = Path(sys.executable).resolve().parent
else:
    BASE_DIR = Path(__file__).resolve().parent

TEMP_DIR = Path(tempfile.gettempdir()) / "PrismaProject"
TEMP_DIR.mkdir(parents=True, exist_ok=True)

MACRO_XLSM_PATH = Path(r"D:\Datos de Usuarios\T72496\Desktop\PrismaProject\Macro.xlsm")
OUTPUT_XLSM_PATH = TEMP_DIR / "Macro_out.xlsm"

RESULT_IMG_PATH = TEMP_DIR / "resultado.png"
DETALLADA_IMG_PATH = TEMP_DIR / "detallada.png"
OTROS_IMG_PATH = TEMP_DIR / "otros_reportes.png"


SUNAT_IMG_PATH = TEMP_DIR / "sunat_panel.png"

IMG_PATH = TEMP_DIR / "captura.png"
EXCEL_PATH = TEMP_DIR / "consulta_deuda.xlsx"


