pip install pyautogui pillow


import pyautogui

# Coordenadas (x, y, ancho, alto)
# OJO: son píxeles de pantalla
region = (500, 300, 200, 80)

img = pyautogui.screenshot(region=region)

img.save("captura.png")



import pyautogui
print(pyautogui.position())
