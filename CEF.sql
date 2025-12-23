pip install easyocr opencv-python


import easyocr
import cv2

reader = easyocr.Reader(['en'], gpu=False)

img = cv2.imread("imagen.png")

result = reader.readtext(
    img,
    allowlist='ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789'
)

if result:
    texto = result[0][1]
    print(texto)
else:
    print("No se detectó texto")


result = reader.readtext(
    img,
    detail=0,
    allowlist='ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789'
)


A9X3Q
