import pytesseract
from PIL import Image
import os
import re

BASE = r"C:\Users\usern\Downloads\Data\Ncesario"

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
