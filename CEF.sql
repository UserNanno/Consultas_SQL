pip install rapidocr-onnxruntime pillow


from rapidocr_onnxruntime import RapidOCR
from PIL import Image
import re

img_path = "captura.png"

ocr = RapidOCR()  # descarga/carga modelos la primera vez
result, _ = ocr(img_path)

# result: lista de [ [box], text, score ]
texts = [r[1] for r in (result or [])]
text = "".join(texts)

# limpieza y whitelist (solo A-Z a-z 0-9)
text = re.sub(r"[^A-Za-z0-9]", "", text)

# si esperas exactamente 4 chars:
text = text[:4]
print("OCR:", text)
