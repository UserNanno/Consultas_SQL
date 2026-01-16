from pathlib import Path
import sys
EDGE_EXE = r"C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe"
DEBUG_PORT = 9223
URL_SBS_CERRAR_SESIONES = "https://extranet.sbs.gob.pe/CambioClave/pages/cerrarSesiones.jsf"
URL_LOGIN = "https://extranet.sbs.gob.pe/app/login.jsp"
URL_COPILOT = "https://m365.cloud.microsoft/chat/?auth=2"
URL_SUNAT = "https://e-consultaruc.sunat.gob.pe/cl-ti-itmrconsruc/FrameCriterioBusquedaWeb.jsp"
URL_RBM = "https://suitebancapersonas.lima.bcp.com.pe:444/Consumos/FiltroClienteConsumo"
DNI_CONSULTA = "72811352"
DNI_CONYUGE_CONSULTA = "78801600"

def app_dir() -> Path:
   """
   Carpeta base:
   - en .exe: carpeta donde está el ejecutable
   - en dev: raíz del proyecto (asumiendo config/settings.py)
   """
   if getattr(sys, "frozen", False):
       return Path(sys.executable).resolve().parent
   return Path(__file__).resolve().parents[1]

APP_DIR = app_dir()
# Plantilla al lado del exe
MACRO_XLSM_PATH = APP_DIR / "Macro.xlsm"
# Carpeta results al lado del exe
RESULTS_DIR = APP_DIR / "results"
RESULTS_DIR.mkdir(parents=True, exist_ok=True)
# Output y evidencias
OUTPUT_XLSM_PATH = RESULTS_DIR / "Macro_out.xlsm"
RESULT_IMG_PATH = RESULTS_DIR / "resultado.png"
DETALLADA_IMG_PATH = RESULTS_DIR / "detallada.png"
OTROS_IMG_PATH = RESULTS_DIR / "otros_reportes.png"
SUNAT_IMG_PATH = RESULTS_DIR / "sunat_panel.png"
IMG_PATH = RESULTS_DIR / "captura.png"
RBM_CONSUMOS_IMG_PATH = RESULTS_DIR / "rbm_consumos.png"
RBM_CEM_IMG_PATH = RESULTS_DIR / "rbm_cem.png"
