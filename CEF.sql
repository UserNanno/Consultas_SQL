
from __future__ import annotations

from pathlib import Path
from typing import Tuple
import logging

from openpyxl import load_workbook
from openpyxl.drawing.image import Image as XLImage
from openpyxl.utils.cell import coordinate_from_string, column_index_from_string
from openpyxl.utils import get_column_letter
from PIL import Image as PILImage


class XlsmSessionWriter:
    """
    Abre un XLSM (plantilla) una sola vez, permite insertar múltiples imágenes
    en distintas hojas/rangos, y guarda al final.
    Preserva macros con keep_vba=True.
    """

    def __init__(self, template_xlsm: Path):
        self.template_xlsm = Path(template_xlsm)
        self.wb = None
        self._opened = False

    def __enter__(self):
        self.open()
        return self

    def __exit__(self, exc_type, exc, tb):
        self.close()

    def open(self):
        if self._opened:
            return
        if not self.template_xlsm.exists():
            raise FileNotFoundError(f"No existe la plantilla: {self.template_xlsm}")
        # Preserva macros
        self.wb = load_workbook(self.template_xlsm, keep_vba=True)
        self._opened = True

    def close(self):
        self.wb = None
        self._opened = False

    def add_image_to_range(
        self,
        sheet_name: str,
        img_path: Path,
        anchor_cell: str,
        bottom_right_cell: str,
        scale_up: bool = False,
    ):
        """
        Inserta una imagen anclada en anchor_cell, redimensionada para encajar dentro
        del rango anchor_cell:bottom_right_cell.

        Parámetros:
        - sheet_name: Nombre de la hoja destino.
        - img_path: Ruta de la imagen a insertar.
        - anchor_cell: Celda superior izquierda (ej. "B3").
        - bottom_right_cell: Celda inferior derecha (ej. "F10").
        - scale_up: Si False, no se amplían imágenes pequeñas (recomendado).
        """
        if not self._opened or self.wb is None:
            raise RuntimeError("XlsmSessionWriter no está abierto. Usa open() o with ... as writer")

        if sheet_name not in self.wb.sheetnames:
            raise ValueError(f"No existe la hoja '{sheet_name}'. Hojas: {self.wb.sheetnames}")

        img_path = Path(img_path)
        if not img_path.exists():
            raise FileNotFoundError(f"No existe la imagen: {img_path}")

        ws = self.wb[sheet_name]

        target_w_px, target_h_px = self._range_size_pixels(ws, anchor_cell, bottom_right_cell)
        resized_path = self._resize_to_fit(img_path, target_w_px, target_h_px, scale_up=scale_up)

        # Insertar imagen anclada en la celda
        ws.add_image(XLImage(str(resized_path)), anchor_cell)

    def save(self, out_path: Path):
        """
        Guarda el libro en la ruta indicada.
        Protección: no permite sobrescribir la plantilla original .xlsm.
        """
        if not self._opened or self.wb is None:
            raise RuntimeError("XlsmSessionWriter no está abierto. Usa open() o with ... as writer")

        out_path = Path(out_path)
        # Protección: nunca guardar sobre la plantilla
        if out_path.resolve() == self.template_xlsm.resolve():
            raise ValueError("No se permite guardar sobre la plantilla Macro.xlsm. Usa un archivo de salida.")

        out_path.parent.mkdir(parents=True, exist_ok=True)
        self.wb.save(out_path)

    # ---------------- helpers ----------------
    def _resize_to_fit(self, img_path: Path, max_w: int, max_h: int, scale_up: bool) -> Path:
        """
        Redimensiona la imagen para que quepa en (max_w x max_h) manteniendo proporción.
        Si scale_up=False, no se escala por encima del tamaño original.
        Guarda PNG temporal junto a la imagen original.
        """
        out_path = img_path.with_name(img_path.stem + "_resized.png")
        with PILImage.open(img_path) as im:
            w, h = im.size
            if w <= 0 or h <= 0:
                raise ValueError("Imagen con tamaño inválido")

            scale = min(max_w / w, max_h / h)
            if not scale_up:
                scale = min(scale, 1.0)

            new_w = max(1, int(w * scale))
            new_h = max(1, int(h * scale))

            im2 = im.resize((new_w, new_h), PILImage.LANCZOS)
            im2.save(out_path)

        return out_path

    def _range_size_pixels(self, ws, top_left: str, bottom_right: str) -> Tuple[int, int]:
        """
        Calcula el tamaño en píxeles del rango top_left:bottom_right considerando
        los anchos de columnas y alturas de filas actuales del Worksheet.
        Se aplica un pequeño margen interno.
        """
        tl_col_letter, tl_row = coordinate_from_string(top_left)
        br_col_letter, br_row = coordinate_from_string(bottom_right)

        tl_col = column_index_from_string(tl_col_letter)
        br_col = column_index_from_string(br_col_letter)

        # Ancho total sumando columnas (aprox. conversión de ancho de Excel a px)
        total_w_px = 0
        for c in range(tl_col, br_col + 1):
            col_letter = get_column_letter(c)
            col_dim = ws.column_dimensions.get(col_letter)
            # Valor por defecto de Excel ~8.43 (caracteres)
            width_chars = float(col_dim.width) if (col_dim and col_dim.width is not None) else 8.43
            # Conversión típica: ~7 px por carácter + ~5 px de padding
            total_w_px += int(width_chars * 7 + 5)

        # Alto total sumando filas (altura en puntos -> px con 96 DPI)
        total_h_px = 0
        for r in range(tl_row, br_row + 1):
            row_dim = ws.row_dimensions.get(r)
            height_pt = float(row_dim.height) if (row_dim and row_dim.height is not None) else 15.0
            total_h_px += int(height_pt * 96 / 72)

        # margen pequeño para evitar que toque bordes
        return max(50, total_w_px - 10), max(50, total_h_px - 10)
    
    
    def write_cell(self, sheet_name: str, cell: str, value):
       """
       Escribe un valor en una celda específica.
       """
       if not self._opened or self.wb is None:
           raise RuntimeError("XlsmSessionWriter no está abierto. Usa open() o with ... as writer")
       if sheet_name not in self.wb.sheetnames:
           raise ValueError(f"No existe la hoja '{sheet_name}'. Hojas: {self.wb.sheetnames}")
       ws = self.wb[sheet_name]
       ws[cell].value = value
       logging.info("XLSM write_cell: %s!%s = %r", sheet_name, cell, value)
