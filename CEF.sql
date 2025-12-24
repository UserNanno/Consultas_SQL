(venv) D:\Datos de Usuarios\T72496\Desktop\MODELOS_RPTs\WebAutomatic>py reconocimiento.py
Traceback (most recent call last):
  File "D:\Datos de Usuarios\T72496\Desktop\MODELOS_RPTs\WebAutomatic\reconocimiento.py", line 18, in <module>
    text = pytesseract.image_to_string(img, config=config)
  File "D:\Datos de Usuarios\T72496\Desktop\MODELOS_RPTs\venv\Lib\site-packages\pytesseract\pytesseract.py", line 486, in image_to_string
    return {
           ~
    ...<2 lines>...
        Output.STRING: lambda: run_and_get_output(*args),
        ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    }[output_type]()
    ~~~~~~~~~~~~~~^^
  File "D:\Datos de Usuarios\T72496\Desktop\MODELOS_RPTs\venv\Lib\site-packages\pytesseract\pytesseract.py", line 489, in <lambda>
    Output.STRING: lambda: run_and_get_output(*args),
                           ~~~~~~~~~~~~~~~~~~^^^^^^^
  File "D:\Datos de Usuarios\T72496\Desktop\MODELOS_RPTs\venv\Lib\site-packages\pytesseract\pytesseract.py", line 352, in run_and_get_output
    run_tesseract(**kwargs)
    ~~~~~~~~~~~~~^^^^^^^^^^
  File "D:\Datos de Usuarios\T72496\Desktop\MODELOS_RPTs\venv\Lib\site-packages\pytesseract\pytesseract.py", line 284, in run_tesseract
    raise TesseractError(proc.returncode, get_errors(error_string))
pytesseract.pytesseract.TesseractError: (1, 'Error opening data file D:\\Datos de Usuarios\\T72496\\Desktop\\MODELOS_RPTs\\WebAutomatic\\Helpers\\tesseract/eng.traineddata Please make sure the TESSDATA_PREFIX environment variable is set to your "tessdata" directory. Failed loading language \'eng\' Tesseract couldn\'t load any languages! Could not initialize tesseract.')

(venv) D:\Datos de Usuarios\T72496\Desktop\MODELOS_RPTs\WebAutomatic>
