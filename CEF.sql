from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time

# =========================
# CONFIG
# =========================
CHROMEDRIVER_PATH = r"D:\Datos de Usuarios\T72496\Downloads\chromedriver-win64\chromedriver-win64\chromedriver.exe"
URL_LOGIN = "https://extranet.sbs.gob.pe/app/login.jsp"

USUARIO = "TU_USUARIO"
CLAVE = "123456"  # solo números

# =========================
# DRIVER
# =========================
opts = Options()
opts.add_argument("--window-size=1200,800")

service = Service(CHROMEDRIVER_PATH)
driver = webdriver.Chrome(service=service, options=opts)
wait = WebDriverWait(driver, 20)

# =========================
# LOGIN
# =========================
driver.get(URL_LOGIN)

# Usuario
inp_user = wait.until(EC.presence_of_element_located((By.ID, "c_c_usuario")))
inp_user.clear()
inp_user.send_keys(USUARIO)

# Esperar keypad
wait.until(EC.presence_of_element_located((By.ID, "ulKeypad")))

# Limpiar clave (si existe)
try:
    driver.find_element(By.CSS_SELECTOR, "#ulKeypad li.WEB_zonaIngresobtnBorrar").click()
except Exception:
    pass

# Ingresar clave con clicks
for d in CLAVE:
    tecla = wait.until(
        EC.element_to_be_clickable(
            (By.XPATH, f"//ul[@id='ulKeypad']//li[normalize-space()='{d}']")
        )
    )
    tecla.click()
    time.sleep(0.2)

# ACEPTAR
btn = wait.until(EC.element_to_be_clickable((By.ID, "btnIngresar")))
btn.click()

print("Login ejecutado. Revisa el navegador.")
input("ENTER para cerrar...")

driver.quit()










pip install pytesseract pillow




from PIL import Image
import pytesseract

# si estás en Windows, probablemente necesites:
# pytesseract.pytesseract.tesseract_cmd = r"C:\Program Files\Tesseract-OCR\tesseract.exe"

img = Image.open("imagen.png")

texto = pytesseract.image_to_string(
    img,
    config="--psm 7 -c tessedit_char_whitelist=ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
)

print(texto.strip())







import cv2

img = cv2.imread("imagen.png", 0)
_, img = cv2.threshold(img, 150, 255, cv2.THRESH_BINARY)
cv2.imwrite("proc.png", img)


