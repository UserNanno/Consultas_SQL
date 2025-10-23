import pandas as pd
import re

def parse_fecha_hora_esp(series):
    """
    Convierte strings como '01/10/2025 08:42 a. m.' o '01/10/2025 3:25 p. m.'
    en datetime de 24 horas, respetando el día/mes correcto (formato español).
    """
    def clean_and_parse(value):
        if pd.isna(value):
            return pd.NaT
        s = str(value).strip().lower()
        s = re.sub(r'\s+', ' ', s)
        # Limpieza de puntos y espacios en a. m. / p. m.
        s = (s.replace('a. m.', 'AM')
               .replace('p. m.', 'PM')
               .replace('a. m', 'AM')
               .replace('p. m', 'PM')
               .replace('a m', 'AM')
               .replace('p m', 'PM')
               .strip())
        try:
            # 🔹 formato explícito día/mes/año con 12h
            return pd.to_datetime(s, format="%d/%m/%Y %I:%M %p", errors="coerce", dayfirst=True)
        except Exception:
            return pd.to_datetime(s, errors="coerce", dayfirst=True)
    return series.apply(clean_and_parse)


df_pendientes_tcstock["FECINICIOPASO"] = parse_fecha_hora_esp(df_pendientes_tcstock["FECINICIOPASO"])


df_pendientes_tcstock["FECINICIOPASO"] = df_pendientes_tcstock["FECINICIOPASO"].dt.tz_localize("America/Lima", nonexistent="NaT", ambiguous="NaT")
