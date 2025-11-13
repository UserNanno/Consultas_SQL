PASS_RATE_UND :=
VAR Aprobadas :=
    CALCULATE(
        SUM( WEEKLY[CTDSOLICITUDES] ),
        WEEKLY[ESTADOSOLICITUD] = "APROBADO"
    )
VAR Totales :=
    CALCULATE(
        SUM( WEEKLY[CTDSOLICITUDES] ),
        REMOVEFILTERS( WEEKLY[ESTADOSOLICITUD] )
    )
RETURN
    DIVIDE( Aprobadas, Totales )




PASS_RATE_MTO :=
VAR MontoAprobado :=
    CALCULATE(
        SUM( WEEKLY[MTOAPROBADO] ),
        WEEKLY[ESTADOSOLICITUD] = "APROBADO"
    )
VAR MontoSolicitado :=
    CALCULATE(
        SUM( WEEKLY[MTOSOLICITADO] ),
        REMOVEFILTERS( WEEKLY[ESTADOSOLICITUD] )
    )
RETURN
    DIVIDE( MontoAprobado, MontoSolicitado )




DISBURSEMENT_RATE :=
VAR MontoDesembolsado :=
    SUM( WEEKLY[MTODESEMBOLSADO] )           -- ya solo existe cuando hay desembolso
VAR MontoAprobado :=
    CALCULATE(
        SUM( WEEKLY[MTOAPROBADO] ),
        WEEKLY[ESTADOSOLICITUD] = "APROBADO"
    )
RETURN
    DIVIDE( MontoDesembolsado, MontoAprobado )




CTDSOLICITUDES_DESEMBOLSADAS :=
CALCULATE(
    SUM( WEEKLY[CTDSOLICITUDES] ),
    NOT ISBLANK( WEEKLY[FECDESEMBOLSO] )
    -- alternativamente:
    -- WEEKLY[MTODESEMBOLSADO] > 0
)




DISBURSEMENT_RATE_UND :=
VAR Desembolsadas =
    CALCULATE(
        SUM( WEEKLY[CTDSOLICITUDES] ),
        NOT ISBLANK( WEEKLY[FECDESEMBOLSO] )
    )
VAR Aprobadas =
    CALCULATE(
        SUM( WEEKLY[CTDSOLICITUDES] ),
        WEEKLY[ESTADOSOLICITUD] = "APROBADO",
        REMOVEFILTERS( WEEKLY[ESTADOSOLICITUD] )
    )
RETURN
    DIVIDE( Desembolsadas, Aprobadas )
