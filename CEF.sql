import os
import time
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

URL_LOGIN = "https://extranet.sbs.gob.pe/app/login.jsp"

def human_pause(a=0.2, b=0.7):
    import random, time
    time.sleep(random.uniform(a, b))

def type_like_human(el, text: str, min_delay: float = 0.03, max_delay: float = 0.10) -> None:
    import random, time
    el.clear()
    for ch in text:
        el.send_keys(ch)
        time.sleep(random.uniform(min_delay, max_delay))

def clear_keypad_if_possible(driver):
    # 1) intentar click en el LI "limpiar"
    try:
        limpiar = driver.find_element(By.CSS_SELECTOR, "#ulKeypad li.WEB_zonaIngresobtnBorrar")
        limpiar.click()
        return
    except Exception:
        pass

    # 2) intentar ejecutar la función JS (si está disponible)
    try:
        driver.execute_script("if (typeof limpiarCodigo === 'function') { limpiarCodigo(); }")
    except Exception:
        pass

def click_keypad_digits(driver, wait: WebDriverWait, password: str):
    """
    password: solo dígitos (ej: "478095")
    """
    if not password.isdigit():
        raise ValueError("La clave para este keypad debe ser solo numérica (dígitos).")

    # asegurar keypad visible
    wait.until(EC.presence_of_element_located((By.ID, "ulKeypad")))

    clear_keypad_if_possible(driver)
    human_pause(0.2, 0.5)

    for d in password:
        # Opción robusta: buscar por el texto visible del LI
        # (sirve incluso si el orden cambia)
        li = wait.until(
            EC.element_to_be_clickable(
                (By.XPATH, f"//ul[@id='ulKeypad']//li[normalize-space(text())='{d}']")
            )
        )
        li.click()
        human_pause(0.15, 0.45)

def login_extranet_sbs(driver, usuario: str, clave_numerica: str, timeout: int = 25):
    wait = WebDriverWait(driver, timeout)
    driver.get(URL_LOGIN)

    # Usuario
    user_inp = wait.until(EC.presence_of_element_located((By.ID, "c_c_usuario")))
    # a veces tienen watermark; click antes ayuda
    user_inp.click()
    type_like_human(user_inp, usuario, 0.02, 0.06)
    human_pause(0.3, 0.8)

    # Clave por keypad
    click_keypad_digits(driver, wait, clave_numerica)
    human_pause(0.3, 0.8)

    # ACEPTAR
    btn = wait.until(EC.element_to_be_clickable((By.ID, "btnIngresar")))
    btn.click()

    # Aquí depende del sitio: podrías esperar un elemento post-login
    # Ejemplo genérico: esperar que cambie la URL o que aparezca algún contenedor
    # wait.until(EC.url_changes(URL_LOGIN))

if __name__ == "__main__":
    # Sugerencia: NO hardcodees credenciales en código
    USUARIO = os.environ.get("SBS_USER", "TU_USUARIO")
    CLAVE = os.environ.get("SBS_PASS", "123456")  # clave numérica

    driver = build_chrome_driver(user_data_dir=PROFILE_SBS_DIR, show_window=True)
    try:
        login_extranet_sbs(driver, USUARIO, CLAVE)
        print("Login disparado. Revisa si entró correctamente.")
        input("ENTER para cerrar...")
    finally:
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

