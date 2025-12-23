https://github.com/UB-Mannheim/tesseract/wiki

pip install pytesseract opencv-python pillow


import pytesseract
import cv2

# ⚠️ Solo si NO lo agregaste al PATH
pytesseract.pytesseract.tesseract_cmd = r"C:\Program Files\Tesseract-OCR\tesseract.exe"

# Leer imagen en blanco y negro
img = cv2.imread("captura.png", cv2.IMREAD_GRAYSCALE)

# Mejorar contraste
_, img = cv2.threshold(img, 150, 255, cv2.THRESH_BINARY)

# Configuración OCR (una sola línea, pocos caracteres)
config = "--psm 7 -c tessedit_char_whitelist=ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

texto = pytesseract.image_to_string(img, config=config)

print(texto.strip())
