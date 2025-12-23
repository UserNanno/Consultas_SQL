https://github.com/UB-Mannheim/tesseract/wiki

from PIL import Image, ImageOps
import pytesseract
import re

# Ajusta esta ruta si tu tesseract.exe está en otro lado
pytesseract.pytesseract.tesseract_cmd = r"C:\Program Files\Tesseract-OCR\tesseract.exe"

img_path = "captura.png"   # tu imagen guardada

img = Image.open(img_path)

# Preproceso simple: escala de grises + umbral
gray = ImageOps.grayscale(img)
bw = gray.point(lambda x: 255 if x > 160 else 0, mode="1")  # mueve 160 según tu imagen

# Config: asume una sola línea/palabra, y limita caracteres
config = r'--oem 3 --psm 7 -c tessedit_char_whitelist=ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789'

text = pytesseract.image_to_string(bw, config=config).strip()

# Limpieza (por si mete espacios/saltos)
text = re.sub(r"\s+", "", text)

print("OCR:", text)

