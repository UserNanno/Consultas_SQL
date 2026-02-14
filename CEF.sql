# DIAS UTILES DEL MES ACTUAL =
VAR MesActual = MAX(Fact[CODMES])
VAR DiasUtiles =
    CALCULATE(
        MAX(Fact[ACUM_DIA_UTIL_EQ_MES]),
        Fact[CODMES] = MesActual
    )
RETURN
"# DIAS UTILES DEL MES ACTUAL" 
& UNICHAR(10) & UNICHAR(10) &
FORMAT(DiasUtiles, "0.0")
