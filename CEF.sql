Tengo esto 

import pytesseract
from PIL import Image
import os
import re

BASE = r"D:\Datos de Usuarios\T72496\Desktop\MODELOS_RPTs\WebAutomatic\Helpers\tesseract"

pytesseract.pytesseract.tesseract_cmd = os.path.join(BASE, "tesseract.exe")
os.environ["TESSDATA_PREFIX"] = BASE

img = Image.open("captura.png")

config = (
    r'--oem 3 --psm 8 '
    r'-c tessedit_char_whitelist=ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789'
)

text = pytesseract.image_to_string(img, config=config)
text = re.sub(r"\s+", "", text)
text = text[:4]

print("OCR:", text)

pero cuando ejecuto me sale OCR:

