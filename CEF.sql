    DATE_ADD(DATE_TRUNC('month', FEC_DESE_DATE), (FLOOR((DAY(FEC_DESE_DATE) - 1) / 7) * 7)) AS SEMANA_DESE_INI

[DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE] Cannot resolve "date_add(date_trunc(month, FEC_DESE_DATE), (FLOOR(((day(FEC_DESE_DATE) - 1) / 7)) * 7))" due to data type mismatch: The second parameter requires the ("INT" or "SMALLINT" or "TINYINT") type, however "(FLOOR(((day(FEC_DESE_DATE) - 1) / 7)) * 7)" has the type "BIGINT". SQLSTATE: 42K09; line 88, pos 4
