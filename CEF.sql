from pathlib import Path

base_excel_path = Path(EXCEL_PATH)
excel_path = base_excel_path.with_name(
    f"{base_excel_path.stem}_{dni}{base_excel_path.suffix}"
)
