pip install easyocr pillow


import easyocr
from PIL import Image
import re

# Inicializar OCR
reader = easyocr.Reader(['en'], gpu=False)

# Cargar imagen
img = Image.open("captura.png")

# Leer texto (optimizado para códigos cortos)
result = reader.readtext(
    img,
    detail=0,
    allowlist='ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789'
)

if result:
    # Tomar el texto más probable
    text = result[0]

    # Limpiar espacios y saltos
    text = re.sub(r'\s+', '', text)

    # Forzar máximo 4 caracteres
    text = text[:4]

    print("OCR:", text)
else:
    print("No se detectó texto")
