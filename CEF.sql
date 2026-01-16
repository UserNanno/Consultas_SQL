excel_postprocess.py
# utils/excel_postprocess.py
from __future__ import annotations

import logging
from pathlib import Path
from typing import Optional


def apply_excel_triggers_for_inicio(
    xlsm_path: Path,
    segmento: Optional[str],
    segmento_riesgo: Optional[str],
    pdh: Optional[str],
    sheet_name: str = "Inicio",
):
    """
    Abre el XLSM en Excel (COM) para que se ejecuten eventos/macros existentes,
    y re-escribe celdas clave para disparar la lógica asociada a listas desplegables.

    Requiere: pywin32 (win32com)
    """
    xlsm_path = Path(xlsm_path)
    if not xlsm_path.exists():
        raise FileNotFoundError(f"No existe XLSM: {xlsm_path}")

    try:
        import win32com.client as win32  # type: ignore
    except Exception as e:
        raise RuntimeError(
            "No se pudo importar win32com. Instala pywin32 o incluye pywin32 en tu build de PyInstaller."
        ) from e

    logging.info("[XLSM] Post-proceso Excel (COM) para disparar triggers: %s", xlsm_path)

    xl = win32.DispatchEx("Excel.Application")
    xl.Visible = False
    xl.DisplayAlerts = False

    # clave: permitir eventos
    xl.EnableEvents = True

    wb = xl.Workbooks.Open(str(xlsm_path))
    try:
        ws = wb.Worksheets(sheet_name)

        def set_cell(addr: str, value):
            # Si viene None, no tocar (evita borrar si faltó dato)
            if value is None:
                return
            # Forzar "change": vaciar y volver a setear (simula acción de usuario)
            ws.Range(addr).Value = ""
            ws.Range(addr).Value = value

        set_cell("C11", segmento)
        set_cell("C12", segmento_riesgo)
        set_cell("C13", pdh)

        # Fuerza recálculo completo (por si hay fórmulas dependientes)
        xl.CalculateFull()

        wb.Save()
        logging.info("[XLSM] Post-proceso OK: triggers aplicados en %s!C11:C13", sheet_name)

    finally:
        wb.Close(SaveChanges=True)
        xl.Quit()
