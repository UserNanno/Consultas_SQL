import pandas as pd
import re

def parse_fecha_hora_esp(series):
    """
    Convierte strings tipo '01/10/2025 10:54 a. m.' o '01/10/2025 3:25 p. m.'
    a datetime con formato 24 horas.
    """
    def clean_and_parse(value):
        if pd.isna(value):
            return pd.NaT
        s = str(value).strip().lower()
        # normaliza separadores y elimina puntos/espacios raros
        s = re.sub(r'\s+', ' ', s)
        s = s.replace('.', '').replace(' a m', ' AM').replace(' p m', ' PM')
        s = s.replace(' a. m', ' AM').replace(' p. m', ' PM')
        s = s.replace('a m', 'AM').replace('p m', 'PM')
        s = s.replace('a. m.', 'AM').replace('p. m.', 'PM')
        try:
            # pandas reconocerá formatos tipo "01/10/2025 03:25 PM"
            return pd.to_datetime(s, format="%d/%m/%Y %I:%M %p", errors="coerce")
        except Exception:
            return pd.to_datetime(s, errors="coerce")
    return series.apply(clean_and_parse)



df_pendientes_tcstock_base["FECINICIOPASO"] = parse_fecha_hora_esp(df_pendientes_tcstock_base["FECINICIOPASO"])


df_pendientes_cef_base["FECINICIOEVALUACION"] = parse_fecha_hora_esp(df_pendientes_cef_base["FECINICIOEVALUACION"])



df_pendientes_tcstock_base["FECHA"] = df_pendientes_tcstock_base["FECINICIOPASO"].dt.date
df_pendientes_tcstock_base["HORA"] = df_pendientes_tcstock_base["FECINICIOPASO"].dt.time
df_pendientes_tcstock_base["FECHAHORA"] = df_pendientes_tcstock_base["FECINICIOPASO"]


df_pendientes_tcstock_base["FECHAHORA"] = (
    df_pendientes_tcstock_base["FECHAHORA"].dt.tz_localize("America/Lima", nonexistent="NaT", ambiguous="NaT")
)

