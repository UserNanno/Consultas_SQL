import os
import re
from PIL import Image, ImageOps, ImageEnhance, ImageFilter
import pytesseract

BASE = r"D:\Datos de Usuarios\T72496\Desktop\MODELOS_RPTs\WebAutomatic\Helpers\tesseract"
pytesseract.pytesseract.tesseract_cmd = os.path.join(BASE, "tesseract.exe")
os.environ["TESSDATA_PREFIX"] = BASE

def preprocess(img: Image.Image) -> Image.Image:
    # 1) a gris
    img = img.convert("L")

    # 2) escala x3 (muchísimo mejor para OCR de pantallazos)
    w, h = img.size
    img = img.resize((w * 3, h * 3), Image.Resampling.LANCZOS)

    # 3) sube contraste
    img = ImageEnhance.Contrast(img).enhance(2.5)

    # 4) reduce ruido + binariza
    img = img.filter(ImageFilter.MedianFilter(size=3))
    img = ImageOps.autocontrast(img)

    # Umbral (binarización simple)
    img = img.point(lambda p: 255 if p > 170 else 0)

    return img

img = Image.open("captura.png")
img_p = preprocess(img)

# whitelist amplia para código (incluye signos típicos)
whitelist = r"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789_()[]{};=<>:+-*/'\"#.,"

# Prueba varios PSM; 7 (línea) y 6 (bloque) suelen ir mejor para código
psm_candidates = [7, 6, 8, 13]

best = ""
for psm in psm_candidates:
    config = f'--oem 3 --psm {psm} -c tessedit_char_whitelist="{whitelist}"'
    t = pytesseract.image_to_string(img_p, config=config)
    t = t.strip()
    if len(t) > len(best):
        best = t

# Limpieza final: quita saltos repetidos, conserva espacios si te sirven
best_clean = re.sub(r"[ \t]+", " ", best)
best_clean = re.sub(r"\n+", "\n", best_clean).strip()

print("OCR RAW:\n", best)
print("\nOCR CLEAN:\n", best_clean)
