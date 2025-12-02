flowchart TD
    %% ========= ORÍGENES =========
    APPS["TP_BASESOLICITUDESAPPS<br/>Base Apps<br/>CODMES, CODSOLICITUD,<br/>MATANALISTA, RESULTADOANALISTA, PRODUCTO"]
    SF["TP_BASESOLICITUDESSALESFORCE<br/>Base Salesforce<br/>CODMESEVALUACION, CODSOLICITUD,<br/>MATRICULA, ESTADOSOLICITUD, PRODUCTOS, ETAPA"]
    COL["TP_COLABORADOR<br/>Foto diaria de colaboradores activos<br/>Matrícula, superior, unidad, área, servicio,<br/>clasificación a CANAL (DCA, TLMK, ENALTA, BEX, OTROS)"]
    IND["TP_SOLICITUDINDIVIDUO<br/>Info del solicitante titular<br/>Segmento (BEX, PYME, etc.) y tipo de renta"]
    TC["TP_TIPOCAMBIO<br/>TC_USD_SOL = 3.81"]
    CONS_SRC["M_SOLICITUDCREDITOCONSUMO<br/>Solicitudes de crédito consumo"]
    TCRED_SRC["M_SOLICITUDTARJETACREDITO<br/>Solicitudes de tarjeta crédito"]
    FERIADOS["T72496_DE_DIASNOHABILES<br/>Tabla de días no hábiles"]

    %% ========= DERIVADOS DE COLABORADOR =========
    COLAAN["TP_COLABORADORANALISTA<br/>Última foto del mes por matrícula<br/>Obtiene MATORGANICO, MATSUPERIOR,<br/>Área del analista"]
    COLVEN["TP_COLABORADORVENDEDOR<br/>Foto diaria vendedor<br/>Unidad, área, servicio y canal del vendedor"]

    %% ========= NORMALIZACIÓN DE SOLICITUDES =========
    BASE_SOL["TP_BASE_SOLICITUDES<br/>Full outer join Apps + Salesforce<br/>• Unifica CODMES/CODSOLICITUD<br/>• Normaliza ESTADOSOLICITUD (incluye PENDIENTE+DESESTIMADA→RECHAZADO)<br/>• Unifica MATANALISTA y PRODUCTO<br/>• Marca ORIGEN (AMBOS / SOLO_SF)<br/>• Adjunta área analista y MATSUPERIOR"]
    CONS["TP_SOLICITUDCREDITOCONSUMO<br/>Normaliza consumo<br/>• Mapea CODPRODUCTO a PRODUCTO (LD, compra deuda, convenio)<br/>• Limpia DESCAMPANIA<br/>• Calcula fechas/hora en timestamp<br/>• Define moneda (SOLES/DOLARES)<br/>• Convierte montos a soles usando TC_USD_SOL<br/>• Calcula MTODESEMBOLSADO según moneda y FECDESEMBOLSO"]
    TCRED["TP_SOLICITUDTARJETACREDITO<br/>Normaliza tarjeta crédito<br/>• Clasifica DESTIPOPERACION (stock/nueva)<br/>• Normaliza DESPRODUCTO y DESCAMPANIA<br/>• Clasifica tipo evaluación riesgo (100%, PREA, REACTIVO, OTROS)<br/>• Fechas/hora en timestamp<br/>• Convierte montos a soles con TC_USD_SOL<br/>• Usa FECEMISIONTARJETACREDITO como desembolso"]

    UNION_CTE["TP_UNION<br/>UNION ALL<br/>Une consumo + tarjeta en una sola base común"]

    %% ========= ENRIQUECIMIENTO Y CÁLCULO DE TIEMPOS =========
    BASE_UNION["TP_BASE_UNION<br/>Enriquece solicitudes unificadas<br/>• Join con vendedor (unidad/área/canal)<br/>• Join con individuo (segmento, renta)<br/>• Calcula tiempos en horas:<br/>  - solicitud→desembolso<br/>  - solicitud→inicio evaluación<br/>  - inicio→fin evaluación<br/>  - solicitud→fin evaluación<br/>  - fin evaluación→desembolso"]
    
    CAL["CALENDARIO<br/>Genera fechas 2024-01-01 a 2025-12-31"]
    DIMCAL["DIMCALENDARIO<br/>Marca FLGDIAUTIL (1/0)<br/>según domingo/feriado"]

    %% ========= LÓGICA DE NEGOCIO (CENTRO, ESTADO) =========
    PREV["TP_BASE_PREV<br/>Capa lógica negocio<br/>• Une BASE_UNION + BASE_SOLICITUDES + DIMCALENDARIO<br/>• Marca FLGDIAUTIL = 'SI'/'NO'<br/>• Clasifica CENTROATENCION:<br/>  - PUNTO DE CONTACTO (red comercial/TLMK sin analista y estado ACEPTADA/DESACTIVADA)<br/>  - CENTRALIZADO (analista de TRIBU RIESGOS)<br/>• Determina ESTADOSOLICITUD final:<br/>  - CENTRALIZADO: usa estado analista/SF<br/>  - PUNTO DE CONTACTO: mapea ACEPTADA→APROBADO, DESACTIVADA→RECHAZADO<br/>• Lleva tiempos y montos (solicitado, aprobado, desembolsado)"]

    %% ========= CÁLCULO DE FLAGS Y MONTOS =========
    BASE["TP_BASE<br/>Construye métricas por solicitud<br/>• Con CENTROATENCION y ESTADOSOLICITUD:<br/>  - Flags de denegada (integra/parcial)<br/>  - Flags de aprobada (integra/parcial)<br/>  - Flags de desembolsada / no desembolsada (integra/parcial)<br/>• Calcula montos:<br/>  - Monto denegado (integro/parcial/total)<br/>  - Monto aprobado (integro/parcial/total)<br/>  - Monto desembolsado (base/integro/parcial)<br/>  - Monto no desembolsado (base/integro/parcial)"]

    %% ========= BRACKETS DE TIEMPOS =========
    TIME["TP_BASE_TIEMPOS<br/>Clasifica por tramos de tiempo<br/>• Filtra CENTROATENCION en ('CENTRALIZADO','PUNTO DE CONTACTO')<br/>• Define brackets:<br/>  - TIEMPOATENCIONSOLICITUD<br/>  - TIEMPODERIVACIONANALISTA<br/>  - TIEMPOATENCIONANALISTA<br/>  - TIEMPOATENCIONSOLICITUDANALISTA<br/>  - TIEMPODESEMBOLSOPOSTATENCIONANALISTA<br/>  en: SIN INF., 0-12h, 12-24h, >24h"]

    FINAL["SELECT FINAL → T72496_UNIONWEEKLY<br/>• Agrupa por CODMES, CENTROATENCION, CANAL,<br/>  analista, supervisor, área vendedor, producto,<br/>  campaña, riesgo, segmento, renta, brackets de tiempo<br/>• Suma:<br/>  - Cantidades de solicitudes (aprobadas, denegadas,<br/>    desembolsadas, no desembolsadas, íntegro/parcial)<br/>  - Montos solicitados, aprobados, denegados,<br/>    desembolsados y no desembolsados"]

    %% ========= FLUJOS =========

    %% COLABORADORES → DERIVADOS
    COL --> COLAAN
    COL --> COLVEN

    %% SOLICITUDES ORIGEN → BASE_SOL
    APPS --> BASE_SOL
    SF --> BASE_SOL
    COLAAN --> BASE_SOL

    %% TIPOCAMBIO + SOLICITUDES ORIGEN → NORMALIZADAS
    TC --> CONS
    TC --> TCRED
    CONS_SRC --> CONS
    TCRED_SRC --> TCRED

    %% CONSUMO + TC → UNION
    CONS --> UNION_CTE
    TCRED --> UNION_CTE

    %% UNION + VENDEDOR + INDIVIDUO → BASE_UNION
    UNION_CTE --> BASE_UNION
    COLVEN --> BASE_UNION
    IND --> BASE_UNION

    %% CALENDARIO → DIMCALENDARIO
    CAL --> DIMCAL
    FERIADOS --> DIMCAL

    %% BASE_UNION + BASE_SOL + DIMCAL → PREV
    BASE_UNION --> PREV
    BASE_SOL --> PREV
    DIMCAL --> PREV

    %% PREV → BASE (FLAGS Y MONTOS)
    PREV --> BASE

    %% BASE → TIME (BRACKETS DE TIEMPO)
    BASE --> TIME

    %% TIME → FINAL (AGREGACIÓN)
    TIME --> FINAL
