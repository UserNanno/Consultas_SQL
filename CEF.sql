Mira he tenido que crear un pip.ini en esta ruta C:\Users\T72496\pip

pip.ini:


[global]
index-url = https://artifactory.lima.bcp.com.pe/artifactory/api/pypi/python-pypi-cache/simple
trusted-host = artifactory.lima.bcp.com.pe

Y he tenido que hacer: pip install --no-deps "D:\Datos de Usuarios\T72496\Desktop\PrismaProject\Dependencias\pyinstaller-6.17.0-py3-none-win_amd64.whl"

aca da librería que tenia que usar

(venv) D:\Datos de Usuarios\T72496\Desktop\PrismaProject\Dependencias>dir
 El volumen de la unidad D no tiene etiqueta.
 El número de serie del volumen es: E87C-EAAA

 Directorio de D:\Datos de Usuarios\T72496\Desktop\PrismaProject\Dependencias

26/12/2025  13:37    <DIR>          .
26/12/2025  13:36    <DIR>          ..
26/12/2025  13:29           163,286 certifi-2025.10.5-py3-none-any.whl
26/12/2025  13:37               128 pipDependencias.txt
26/12/2025  13:37         1,378,685 pyinstaller-6.17.0-py3-none-win_amd64.whl
26/12/2025  13:12         9,655,249 selenium-4.39.0-py3-none-any.whl
26/12/2025  13:32            44,614 typing_extensions-4.15.0-py3-none-any.whl
26/12/2025  13:31           131,182 urllib3-2.6.2-py3-none-any.whl
26/12/2025  13:33           176,841 websockets-15.0.1-cp312-cp312-win_amd64.whl
26/12/2025  13:34            82,616 websocket_client-1.9.0-py3-none-any.whl
               8 archivos     11,632,601 bytes
               2 dirs  257,074,970,624 bytes libres



Ahora qu eya tengo python 3.12 y las dependencias necesarias. Como empaqueto para que no tenga problema los usuarios al ejecuta
