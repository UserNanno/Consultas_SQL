(venv) D:\Datos de Usuarios\T72496\Desktop\PrismaProject>py main.py
Traceback (most recent call last):
  File "D:\Datos de Usuarios\T72496\Desktop\PrismaProject\main.py", line 11, in <module>
    from services.excel_service import ExcelService
  File "D:\Datos de Usuarios\T72496\Desktop\PrismaProject\services\excel_service.py", line 1, in <module>
    from openpyxl import Workbook
  File "D:\Datos de Usuarios\T72496\Desktop\PrismaProject\venv\Lib\site-packages\openpyxl\__init__.py", line 7, in <module>
    from openpyxl.workbook import Workbook
  File "D:\Datos de Usuarios\T72496\Desktop\PrismaProject\venv\Lib\site-packages\openpyxl\workbook\__init__.py", line 4, in <module>
    from .workbook import Workbook
  File "D:\Datos de Usuarios\T72496\Desktop\PrismaProject\venv\Lib\site-packages\openpyxl\workbook\workbook.py", line 7, in <module>
    from openpyxl.worksheet.worksheet import Worksheet
  File "D:\Datos de Usuarios\T72496\Desktop\PrismaProject\venv\Lib\site-packages\openpyxl\worksheet\worksheet.py", line 24, in <module>
    from openpyxl.cell import Cell, MergedCell
  File "D:\Datos de Usuarios\T72496\Desktop\PrismaProject\venv\Lib\site-packages\openpyxl\cell\__init__.py", line 3, in <module>
    from .cell import Cell, WriteOnlyCell, MergedCell
  File "D:\Datos de Usuarios\T72496\Desktop\PrismaProject\venv\Lib\site-packages\openpyxl\cell\cell.py", line 26, in <module>
    from openpyxl.styles import numbers, is_date_format
  File "D:\Datos de Usuarios\T72496\Desktop\PrismaProject\venv\Lib\site-packages\openpyxl\styles\__init__.py", line 4, in <module>
    from .alignment import Alignment
  File "D:\Datos de Usuarios\T72496\Desktop\PrismaProject\venv\Lib\site-packages\openpyxl\styles\alignment.py", line 5, in <module>
    from openpyxl.descriptors import Bool, MinMax, Min, Alias, NoneSet
  File "D:\Datos de Usuarios\T72496\Desktop\PrismaProject\venv\Lib\site-packages\openpyxl\descriptors\__init__.py", line 4, in <module>
    from .sequence import Sequence
  File "D:\Datos de Usuarios\T72496\Desktop\PrismaProject\venv\Lib\site-packages\openpyxl\descriptors\sequence.py", line 4, in <module>
    from openpyxl.xml.functions import Element
  File "D:\Datos de Usuarios\T72496\Desktop\PrismaProject\venv\Lib\site-packages\openpyxl\xml\functions.py", line 36, in <module>
    from et_xmlfile import xmlfile
ModuleNotFoundError: No module named 'et_xmlfile'

(venv) D:\Datos de Usuarios\T72496\Desktop\PrismaProject>
