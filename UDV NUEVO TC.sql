-- =====================================================================================
-- OBJETIVO:
-- Reescritura 100% SQL (CTEs) para obtener el MISMO resultado final del notebook:
--   catalog_lhcl_prod_bcp.bcp_edv_rbmper.T57182_SOLICITUDES
-- con renombramientos consistentes para evitar redundancias.
--
-- NOTA:
-- 1) Mantiene lógica clave:
--    - RBM: dedup por (CIC, NUMSOLICITUD_RBM) priorizando Approve/Other/Decline y fecha eval desc
--    - Ventas: join por (CIC + CANAL + NUMSOLICITUD_CORTO)
--    - SLF/Consumo: join por (CODMES + NUMSOLICITUD_RBM)
--    - Leads: prioridad por CIC (y match exacto por LEAD_CEF; fallback por CIC para Reactivo/CONS)
--    - Segmento: por CIC/CODMES desde md_evaluacionsolicitudcredito
--    - BASE/BASE1/BASE2: misma lógica (resultado_final_unico y RK)
--    - Clasificación: mapeo por Reglas (MAX_BY)
--    - TIPO_LEAD final: CON_LEAD si CIC existe en leads del mes, sino SIN_LEAD
--
-- 2) Si ya tienes otra fuente de leads “oficial” (ej. tabla T57182_LEADS),
--    puedes reemplazar CTE LEADS_MES por esa tabla (manteniendo mismas llaves).
-- =====================================================================================

CREATE OR REPLACE TABLE catalog_lhcl_prod_bcp.bcp_edv_rbmper.T57182_SOLICITUDES
USING DELTA
LOCATION 'abfss://bcp-edv-rbmper@adlscu1lhclbackp05.dfs.core.windows.net/data/in/T57182/REVIEW29'
AS
WITH
-- =========================
-- Parámetros de ejecución
-- =========================
PARAMS AS (
  SELECT
    202511 AS CODMES,     -- mes base
    202512 AS CODMES_1    -- mes posterior (para ventas / TC)
),

-- =========================
-- 1) RBM: base mínima + derivaciones
-- =========================
RBM_BASE AS (
  SELECT
    CAST(date_format(A.FECEVALUACION,'yyyyMM') AS INT)  AS CODMES_EVAL,
    CAST(A.FECEVALUACION AS DATE)                       AS FEC_EVALUACION,
    TRIM(A.CODINTERNOCOMPUTACIONAL)                     AS CIC,
    TRIM(A.NUMSOLICITUDEVALUACION)                      AS NUMSOLICITUD_RBM,
    A.DESCANALVENTARBMPER                               AS CANAL_RBM,
    A.DESTIPDECISIONRESULTADORBM                        AS RESULTADO_CDA,    -- Approve/Decline/Other
    A.DESDECISIONEVALUACION                             AS DECISION_RBM,
    A.DESCAMPANIASOLICITUD                              AS CAMPANIA_RBM,
    A.DESTIPEVALUACIONSOLICITUDCREDITO                  AS TIPO_CAMPANIA_RBM,

    -- para invalidar Banca Móvil df_Regular (igual notebook)
    CASE
      WHEN A.DESCANALVENTARBMPER='BANCA MÓVIL' AND A.DESEVALUACION='df_Regular' THEN '0.No valido'
      WHEN A.DESCAMPANIASOLICITUD='Reactivo' THEN '1.Reactivo'
      ELSE '2.No Reactivo'
    END AS EVALUACION_REACTIVO,

    -- solicitud corto (para match con ventas)
    CASE
      WHEN A.DESCANALVENTARBMPER='SALEFORCE' THEN SUBSTR(TRIM(A.NUMSOLICITUDEVALUACION), 5, 7)
      WHEN A.DESCANALVENTARBMPER='LOANS'     THEN SUBSTR(TRIM(A.NUMSOLICITUDEVALUACION), 3, 8)
      ELSE TRIM(A.NUMSOLICITUDEVALUACION)
    END AS NUMSOLICITUD_CORTO,

    -- jerarquía resultado (Approve primero)
    CASE
      WHEN A.DESTIPDECISIONRESULTADORBM='Approve' THEN 1
      WHEN A.DESTIPDECISIONRESULTADORBM='Decline' THEN 3
      ELSE 2
    END AS RESULTADO_JERARQ,

    -- LEAD_CEF (misma lógica notebook)
    CASE
      WHEN A.DESTIPEVALUACIONSOLICITUDCREDITO IN ('Regular LD','Cuotealo')
        AND A.DESCAMPANIASOLICITUD IN ('100% Aprobado','Pre-Aprobado') THEN 'LD APR'
      WHEN A.DESTIPEVALUACIONSOLICITUDCREDITO IN ('Regular LD','Cuotealo')
        AND A.DESCAMPANIASOLICITUD='CEF Shield' THEN 'LD SHD'
      WHEN A.DESTIPEVALUACIONSOLICITUDCREDITO='Regular LD' AND A.DESCAMPANIASOLICITUD='Convenio' THEN 'LD CONV'
      WHEN A.DESTIPEVALUACIONSOLICITUDCREDITO='Regular LD' AND A.DESCAMPANIASOLICITUD='Invitado' THEN 'LD INV'

      WHEN A.DESTIPEVALUACIONSOLICITUDCREDITO='Compra de Deuda'
        AND A.DESCAMPANIASOLICITUD IN ('100% Aprobado','Pre-Aprobado') THEN 'CDD APR'
      WHEN A.DESTIPEVALUACIONSOLICITUDCREDITO='Compra de Deuda' AND A.DESCAMPANIASOLICITUD='Convenio' THEN 'CDD CONV'

      WHEN A.DESTIPEVALUACIONSOLICITUDCREDITO='LD + Consolidación'
        AND A.DESCAMPANIASOLICITUD IN ('100% Aprobado','Pre-Aprobado') THEN 'REE APR'
      WHEN A.DESTIPEVALUACIONSOLICITUDCREDITO='LD + Consolidación' AND A.DESCAMPANIASOLICITUD='Convenio' THEN 'REE CONV'

      WHEN A.DESTIPEVALUACIONSOLICITUDCREDITO='LD + Compra de Deuda + Consolidación'
        AND A.DESCAMPANIASOLICITUD IN ('100% Aprobado','Pre-Aprobado') THEN 'CONS APR'
      WHEN A.DESTIPEVALUACIONSOLICITUDCREDITO='LD + Compra de Deuda + Consolidación' AND A.DESCAMPANIASOLICITUD='Convenio' THEN 'CONS CONV'

      ELSE 'REACTIVO'
    END AS LEAD_CEF

  FROM catalog_lhcl_prod_bcp.bcp_ddv_rbmrbmper_modelogestion_vu.md_evaluacionsolicitudcredito A
  CROSS JOIN PARAMS P
  WHERE A.TIPPRODUCTOSOLICITUDRBM='CC'
    AND A.DESCANALVENTARBMPER NOT IN ('YAPE','OTROS')
    AND A.CODMESEVALUACION = P.CODMES
),

RBM_UNICOS AS (
  SELECT *
  FROM RBM_BASE
  WHERE EVALUACION_REACTIVO <> '0.No valido'
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY CIC, NUMSOLICITUD_RBM
    ORDER BY RESULTADO_JERARQ ASC, FEC_EVALUACION DESC
  ) = 1
),

-- =========================
-- 2) Tipo de cambio: último día por mes (y moneda origen)
-- =========================
TC_BASE AS (
  SELECT
    CAST(date_format(A.FECTIPCAMBIO,'yyyyMM') AS INT) AS CODMES_TC,
    A.FECTIPCAMBIO                                  AS FEC_TC,
    A.CODMONEDAORIGEN                               AS CODMONEDA_ORIGEN,
    A.MTOCAMBIOMONEDAORIGENMONEDADESTINO            AS TC_A_SOL
  FROM catalog_lhcl_prod_bcp.bcp_udv_int_vu.H_TIPOCAMBIO A
  CROSS JOIN PARAMS P
  WHERE A.CODMONEDADESTINO='0001'
    AND A.CODAPP='GLM'
    AND CAST(date_format(A.FECTIPCAMBIO,'yyyyMM') AS INT) BETWEEN P.CODMES AND P.CODMES_1
),

TC_MAXDAY AS (
  SELECT CODMES_TC, MAX(FEC_TC) AS FEC_TC
  FROM TC_BASE
  GROUP BY CODMES_TC
),

TC_MES AS (
  SELECT
    A.CODMES_TC,
    A.FEC_TC,
    A.CODMONEDA_ORIGEN,
    A.TC_A_SOL
  FROM TC_BASE A
  JOIN TC_MAXDAY B
    ON A.CODMES_TC=B.CODMES_TC
   AND A.FEC_TC=B.FEC_TC
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY A.CODMES_TC, A.CODMONEDA_ORIGEN
    ORDER BY A.FEC_TC DESC
  ) = 1
),

-- =========================
-- 3) Ventas (MDPREST): cuenta + cliente + soles + canal + codsolicitud corto
-- =========================
CLIENTE AS (
  SELECT
    CODCLAVEPARTYCLI,
    TRIM(CODINTERNOCOMPUTACIONAL) AS CIC
  FROM catalog_lhcl_prod_bcp.bcp_udv_int_vu.M_CLIENTE
),

VTA_CUENTA AS (
  SELECT
    CAST(date_format(A.FECAPERTURA,'yyyyMM') AS INT) AS CODMES_APERTURA,
    CAST(A.FECAPERTURA AS DATE)                      AS FEC_APERTURA_VTA,
    CAST(A.FECDESEMBOLSO AS DATE)                    AS FEC_DESEMBOLSO_VTA,
    A.CODCLAVECTA                                    AS CODCLAVECTA_VTA,
    TRIM(A.CODSOLICITUD)                             AS CODSOLICITUD_VTA,
    A.CODCLAVEPARTYCLI,
    A.CODPRODUCTO                                    AS CODPRODUCTO_VTA,
    A.MTODESEMBOLSADO                                AS MTO_DESEMBOLSADO_VTA,
    A.CODMONEDA                                      AS CODMONEDA_VTA
  FROM catalog_lhcl_prod_bcp.bcp_udv_int_vu.M_CUENTACREDITOPERSONAL A
  CROSS JOIN PARAMS P
  WHERE CAST(date_format(A.FECAPERTURA,'yyyyMM') AS INT) BETWEEN P.CODMES AND P.CODMES_1
    AND A.FLGREGELIMINADOFUENTE='N'
    AND A.CODPRODUCTO IN ('CPEEFM','CPECMC','CPEDPP','CPECEM','CPEECV','CPEGEN','CPEADH','CPEFIA')
),

VENTAS_CONSOL AS (
  SELECT
    A.CODMES_APERTURA,
    A.FEC_APERTURA_VTA,
    A.FEC_DESEMBOLSO_VTA,
    A.CODCLAVECTA_VTA,
    A.CODSOLICITUD_VTA,
    C.CIC,
    A.CODPRODUCTO_VTA,
    A.CODMONEDA_VTA,
    CASE
      WHEN A.CODMONEDA_VTA <> '0001' THEN A.MTO_DESEMBOLSADO_VTA * COALESCE(T.TC_A_SOL, 0)
      ELSE A.MTO_DESEMBOLSADO_VTA
    END AS MTO_DESEMBOLSO_SOL_VTA,

    -- codsolicitud corto (para match con RBM)
    CASE
      WHEN SUBSTR(TRIM(A.CODSOLICITUD_VTA),1,2) IN ('CX','DX') THEN SUBSTR(TRIM(A.CODSOLICITUD_VTA),3,8) -- LOANS
      WHEN TRIM(A.CODSOLICITUD_VTA) LIKE '2________'          THEN SUBSTR(TRIM(A.CODSOLICITUD_VTA),3,7) -- SF
      WHEN A.CODSOLICITUD_VTA LIKE '      2________'          THEN SUBSTR(TRIM(A.CODSOLICITUD_VTA),3,7) -- SF
      WHEN TRIM(A.CODSOLICITUD_VTA) LIKE 'O%'                 THEN SUBSTR(TRIM(A.CODSOLICITUD_VTA),3,7) -- SF
      ELSE TRIM(A.CODSOLICITUD_VTA) -- BMO / CUOTEALO
    END AS NUMSOLICITUD_CORTO_VTA,

    -- canal venta (para match con RBM)
    CASE
      WHEN A.CODPRODUCTO_VTA='CPEFIA'                          THEN 'CUOTEALO'
      WHEN SUBSTR(TRIM(A.CODSOLICITUD_VTA),1,2)='PE'           THEN 'CUOTEALO'
      WHEN SUBSTR(TRIM(A.CODSOLICITUD_VTA),1,2)='JB'           THEN 'BANCA MÓVIL'
      WHEN SUBSTR(TRIM(A.CODSOLICITUD_VTA),1,2)='YP'           THEN 'YAPE'
      WHEN A.CODPRODUCTO_VTA='CPEYAP'                          THEN 'YAPE'
      WHEN SUBSTR(TRIM(A.CODSOLICITUD_VTA),1,2) IN ('CX','DX') THEN 'LOANS'
      WHEN TRIM(A.CODSOLICITUD_VTA) LIKE '2________'           THEN 'SALEFORCE'
      WHEN A.CODSOLICITUD_VTA LIKE '      2________'           THEN 'SALEFORCE'
      WHEN TRIM(A.CODSOLICITUD_VTA) LIKE 'O%'                  THEN 'SALEFORCE'
      ELSE 'OTROS'
    END AS CANAL_VTA

  FROM VTA_CUENTA A
  LEFT JOIN TC_MES T
    ON A.CODMES_APERTURA = T.CODMES_TC
   AND A.CODMONEDA_VTA   = T.CODMONEDA_ORIGEN
  LEFT JOIN CLIENTE C
    ON A.CODCLAVEPARTYCLI = C.CODCLAVEPARTYCLI
),

-- =========================
-- 4) Solicitud consumo (m_solicitudcreditoconsumo): mínimo necesario
-- =========================
SLF_SOL AS (
  SELECT
    CAST(date_format(A.FECSOLICITUD,'yyyyMM') AS INT) AS CODMES_SOL,
    CAST(A.FECSOLICITUD AS DATE)                      AS FEC_SOLICITUD,
    TRIM(A.CODSOLICITUD)                              AS NUMSOLICITUD_RBM, -- se joinea con numsolicitudevaluacion
    TRIM(A.CODINTERNOCOMPUTACIONAL)                   AS CIC,
    A.MTOSOLICITADO                                   AS MTO_SOLICITADO,
    A.MTOAPROBADO                                     AS MTO_APROBADO
  FROM catalog_lhcl_prod_bcp.bcp_udv_int_vu.m_solicitudcreditoconsumo A
  CROSS JOIN PARAMS P
  WHERE CAST(date_format(A.FECSOLICITUD,'yyyyMM') AS INT) = P.CODMES
),

-- =========================
-- 5) Join RBM + ventas + SLF
-- =========================
RBM_VTA_SLF AS (
  SELECT
    -- llaves base
    A.CODMES_EVAL AS CODMES,
    A.FEC_EVALUACION,
    A.CIC,
    A.NUMSOLICITUD_RBM,
    A.NUMSOLICITUD_CORTO,
    A.CANAL_RBM,

    -- RBM decisión
    A.RESULTADO_CDA,
    A.DECISION_RBM,
    A.CAMPANIA_RBM,
    A.TIPO_CAMPANIA_RBM,
    A.LEAD_CEF,

    -- ventas
    B.FEC_APERTURA_VTA,
    B.FEC_DESEMBOLSO_VTA,
    B.CODCLAVECTA_VTA,
    B.MTO_DESEMBOLSO_SOL_VTA,

    -- solicitud consumo
    C.FEC_SOLICITUD,
    C.MTO_SOLICITADO,
    C.MTO_APROBADO

  FROM RBM_UNICOS A
  LEFT JOIN VENTAS_CONSOL B
    ON A.NUMSOLICITUD_CORTO = B.NUMSOLICITUD_CORTO_VTA
   AND A.CANAL_RBM          = B.CANAL_VTA
   AND A.CIC                = B.CIC
  LEFT JOIN SLF_SOL C
    ON A.CODMES_EVAL        = C.CODMES_SOL
   AND A.NUMSOLICITUD_RBM   = C.NUMSOLICITUD_RBM
),

-- =========================
-- 6) Leads BDI: prioridad + LEAD_CEF (igual notebook)
-- =========================
LEADS_RAW AS (
  SELECT
    ROUND(A.CODMES,0)                       AS CODMES_LEAD,
    TRIM(A.CODINTERNOCOMPUTACIONAL)         AS CIC,
    ROUND(A.TIPVENTA,0)                     AS TIPVENTA,
    ROUND(A.TIPOFERTA,0)                    AS TIPOFERTA,
    A.DESCAMPANA                            AS DESCAMPANA_LEAD,
    A.CODCONDICIONCLIENTE,
    ROUND(A.MTOFINALOFERTADOSOL,1)          AS MTO_OFERTA,
    ROUND(A.NUMPLAZO,0)                     AS PLAZO_LEAD,
    A.PCTTASAEFECTIVAANUAL                  AS TEA_LEAD,
    A.FECINICIOVIGENCIA,

    CASE
      WHEN A.TIPVENTA=6   AND A.TIPOFERTA=3  AND A.CODCONDICIONCLIENTE IN ('APR','PRE') THEN 1
      WHEN A.TIPVENTA=6   AND A.TIPOFERTA=3  AND A.CODCONDICIONCLIENTE='OPT'            THEN 2
      WHEN A.TIPVENTA=6   AND A.TIPOFERTA=41 AND A.CODCONDICIONCLIENTE IN ('APR','PRE') THEN 3
      WHEN A.TIPVENTA=6   AND A.TIPOFERTA=38 AND A.CODCONDICIONCLIENTE IN ('APR','PRE') THEN 4
      WHEN A.TIPVENTA=6   AND A.TIPOFERTA IN (89,98)                                   THEN 5
      WHEN A.TIPVENTA=6   AND A.TIPOFERTA=3  AND A.CODCONDICIONCLIENTE='INV'            THEN 6
      WHEN A.TIPVENTA=6   AND A.TIPOFERTA=187                                           THEN 7
      WHEN A.TIPVENTA=110 AND A.TIPOFERTA=3                                              THEN 8
      WHEN A.TIPVENTA=110 AND A.TIPOFERTA=41                                             THEN 9
      WHEN A.TIPVENTA=110 AND A.TIPOFERTA=38                                             THEN 10
      ELSE 11
    END AS PRIORIDAD_LEAD,

    CASE
      WHEN A.TIPVENTA=6   AND A.TIPOFERTA=3  AND A.CODCONDICIONCLIENTE IN ('APR','PRE') THEN 'LD APR'
      WHEN A.TIPVENTA=6   AND A.TIPOFERTA=3  AND A.CODCONDICIONCLIENTE='OPT'            THEN 'LD OPT'
      WHEN A.TIPVENTA=6   AND A.TIPOFERTA IN (89,98)                                   THEN 'LD SHD'
      WHEN A.TIPVENTA=6   AND A.TIPOFERTA=41 AND A.CODCONDICIONCLIENTE IN ('APR','PRE') THEN 'CDD APR'
      WHEN A.TIPVENTA=6   AND A.TIPOFERTA=187                                           THEN 'CDD INS'
      WHEN A.TIPVENTA=6   AND A.TIPOFERTA=38 AND A.CODCONDICIONCLIENTE IN ('APR','PRE') THEN 'REE APR'
      WHEN A.TIPVENTA=6   AND A.TIPOFERTA=3  AND A.CODCONDICIONCLIENTE='INV'            THEN 'LD INV'
      WHEN A.TIPVENTA=110 AND A.TIPOFERTA=3                                              THEN 'LD CONV'
      WHEN A.TIPVENTA=110 AND A.TIPOFERTA=41                                             THEN 'CDD CONV'
      WHEN A.TIPVENTA=110 AND A.TIPOFERTA=38                                             THEN 'REE CONV'
      WHEN A.TIPVENTA=127 AND A.TIPOFERTA=3                                              THEN 'LD MULTI'
      ELSE 'OTRO'
    END AS LEAD_CEF
  FROM catalog_lhcl_prod_bcp.bcp_edv_rbmper.hm_bdi A
  CROSS JOIN PARAMS P
  WHERE A.CODPAUTARBM <> 41
    AND (A.TIPVENTA = 6 OR A.TIPVENTA = 110)
    AND A.DESCAMPANA NOT IN ('CREDITO PERSONAL, VENTA AMPLIACION PLAZO', 'VENTA SKIP')
    AND to_date(A.FECINICIOVIGENCIA,'yyyyMM') = date_trunc('MM', to_date(cast(P.CODMES AS STRING),'yyyyMM'))
),

-- equivalencia al filtro N=1 del notebook (por CODMES, CIC, TIPVENTA, TIPOFERTA)
LEADS_BASE AS (
  SELECT *
  FROM LEADS_RAW
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY CODMES_LEAD, CIC, TIPVENTA, TIPOFERTA
    ORDER BY FECINICIOVIGENCIA
  ) = 1
),

LEADS_UNICOS AS (
  SELECT *
  FROM LEADS_BASE
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY CIC
    ORDER BY PRIORIDAD_LEAD ASC
  ) = 1
),

-- “leads del mes” para TIPO_LEAD final (CON_LEAD / SIN_LEAD)
LEADS_MES AS (
  SELECT DISTINCT CODMES_LEAD AS CODMES, CIC
  FROM LEADS_BASE
),

-- =========================
-- 7) Solicitudes + Lead (match exacto por CIC+LEAD_CEF con fallback por CIC)
--    Replica tu lógica de union all separando REACTIVO/CONS.
-- =========================
SOLICITUDES_LEAD AS (
  -- no reactivo / no cons: match exacto por LEAD_CEF
  SELECT
    A.*,
    COALESCE(B.CODMES_LEAD, C.CODMES_LEAD) AS CODMES_LEAD,
    COALESCE(B.TIPVENTA,    C.TIPVENTA)    AS TIPVENTA_LEAD,
    COALESCE(B.TIPOFERTA,   C.TIPOFERTA)   AS TIPOFERTA_LEAD,
    COALESCE(B.DESCAMPANA_LEAD, C.DESCAMPANA_LEAD) AS DESCAMPANA_LEAD,
    COALESCE(B.CODCONDICIONCLIENTE, C.CODCONDICIONCLIENTE) AS CODCONDICIONCLIENTE_LEAD,
    COALESCE(B.MTO_OFERTA,  C.MTO_OFERTA)  AS MTO_OFERTA,
    COALESCE(B.PLAZO_LEAD,  C.PLAZO_LEAD)  AS PLAZO_LEAD,
    COALESCE(B.TEA_LEAD,    C.TEA_LEAD)    AS TEA_LEAD
  FROM RBM_VTA_SLF A
  LEFT JOIN LEADS_UNICOS B
    ON A.CIC = B.CIC
   AND A.LEAD_CEF = B.LEAD_CEF
  LEFT JOIN LEADS_UNICOS C
    ON A.CIC = C.CIC
  WHERE A.LEAD_CEF NOT IN ('REACTIVO','CONS APR','CONS CONV')

  UNION ALL

  -- reactivo/cons: fallback solo por CIC (igual notebook)
  SELECT
    A.*,
    COALESCE(B.CODMES_LEAD, C.CODMES_LEAD) AS CODMES_LEAD,
    COALESCE(B.TIPVENTA,    C.TIPVENTA)    AS TIPVENTA_LEAD,
    COALESCE(B.TIPOFERTA,   C.TIPOFERTA)   AS TIPOFERTA_LEAD,
    COALESCE(B.DESCAMPANA_LEAD, C.DESCAMPANA_LEAD) AS DESCAMPANA_LEAD,
    COALESCE(B.CODCONDICIONCLIENTE, C.CODCONDICIONCLIENTE) AS CODCONDICIONCLIENTE_LEAD,
    COALESCE(B.MTO_OFERTA,  C.MTO_OFERTA)  AS MTO_OFERTA,
    COALESCE(B.PLAZO_LEAD,  C.PLAZO_LEAD)  AS PLAZO_LEAD,
    COALESCE(B.TEA_LEAD,    C.TEA_LEAD)    AS TEA_LEAD
  FROM RBM_VTA_SLF A
  LEFT JOIN LEADS_UNICOS B
    ON A.CIC = B.CIC
   AND A.LEAD_CEF = B.LEAD_CEF
  LEFT JOIN LEADS_UNICOS C
    ON A.CIC = C.CIC
  WHERE A.LEAD_CEF IN ('REACTIVO','CONS APR','CONS CONV')
),

-- =========================
-- 8) Segmento por CIC/Mes (igual notebook)
-- =========================
SEGMENTO_CIC AS (
  SELECT DISTINCT
    CAST(date_format(A.FECEVALUACION,'yyyyMM') AS INT) AS CODMES,
    TRIM(A.CODINTERNOCOMPUTACIONAL)                    AS CIC,
    CASE
      WHEN A.codsegmentobancario='CON' AND A.codsubsegmentoconsumo='A' THEN 'Consumo A'
      WHEN A.codsegmentobancario='CON' AND A.codsubsegmentoconsumo='B' THEN 'Consumo B'
      WHEN A.codsegmentobancario='CON' AND A.codsubsegmentoconsumo='C' THEN 'Consumo C'
      WHEN A.codsegmentobancario IN ('ENA','BEX')                      THEN 'AFLUENTE'
      ELSE 'OTROS'
    END AS SEGMENTO
  FROM catalog_lhcl_prod_bcp.bcp_ddv_rbmrbmper_modelogestion_vu.md_evaluacionsolicitudcredito A
  CROSS JOIN PARAMS P
  WHERE A.tipproductosolicitudrbm='CC'
    AND A.descanalventarbmper NOT IN ('YAPE','OTROS')
    AND CAST(date_format(A.FECEVALUACION,'yyyyMM') AS INT) = P.CODMES
),

-- =========================
-- 9) BASE (equivalente a T57182_BASE del notebook)
--    Incluye exclusión centralizado para Salesforce y luego UNION ALL con CENTRALIZADO.
-- =========================
BASE_DIGITAL AS (
  SELECT DISTINCT
    A.CODMES                                AS CODMESEVALUACION,
    A.FEC_EVALUACION                        AS FECEVALUACION,
    A.CIC                                   AS CODINTERNOCOMPUTACIONAL,

    CASE WHEN A.CANAL_RBM='SALEFORCE' THEN 'SALESFORCE' ELSE A.CANAL_RBM END AS TIPO_CANAL,
    CASE WHEN A.CANAL_RBM='SALEFORCE' THEN 'SALESFORCE' ELSE 'DIGITAL' END   AS CANAL_F,
    'PUNTO DE CONTACTO'                     AS TIPO_EVALUACION,

    CASE
      WHEN A.MTO_DESEMBOLSO_SOL_VTA IS NOT NULL THEN '1. Aprobado'
      WHEN A.RESULTADO_CDA='Approve'            THEN '1. Aprobado'
      ELSE '2. Denegado'
    END AS RESULTADO_FINAL,

    CASE WHEN A.DESCAMPANA_LEAD IS NOT NULL THEN '1. Con lead' ELSE '2. Sin lead' END AS TIPO_LEAD_TMP,

    A.MTO_SOLICITADO                         AS MTOSOLICITADO_SLF,
    COALESCE(A.MTO_DESEMBOLSO_SOL_VTA,0)      AS MTODESEMBOLSADOSOL_VTA,
    A.RESULTADO_CDA                          AS RESULTADO_CDA,
    A.CAMPANIA_RBM                           AS DESCAMPANA,
    A.DECISION_RBM                           AS DECISION,

    COALESCE(S.SEGMENTO,'OTROS')             AS SEGMENTO,
    A.MTO_OFERTA                             AS MTO_OFERTA

  FROM SOLICITUDES_LEAD A
  LEFT JOIN catalog_lhcl_prod_bcp.bcp_edv_rbmper.T57182_CENTRALIZADO C
    ON TRIM(A.NUMSOLICITUD_RBM) = TRIM(C.numsolicitud)
  LEFT JOIN SEGMENTO_CIC S
    ON S.CODMES = A.CODMES
   AND S.CIC    = A.CIC
  WHERE (A.CANAL_RBM='SALEFORCE' AND C.numsolicitud IS NULL)
     OR (A.CANAL_RBM<>'SALEFORCE')
),

BASE_CENTRALIZADO AS (
  SELECT DISTINCT
    CAST(A.CODMES AS INT)                    AS CODMESEVALUACION,
    B.FECEVALUACION                          AS FECEVALUACION,
    TRIM(A.CLIENTE)                          AS CODINTERNOCOMPUTACIONAL,

    'SALEFORCE'                              AS TIPO_CANAL,
    'SALEFORCE'                              AS CANAL_F,
    'CENTRALIZADO'                           AS TIPO_EVALUACION,

    CASE
      WHEN COALESCE(A.MTO_DESEM,0) > 0 THEN '1. Aprobado'
      WHEN A.ESTADO_FIN = 'ACEPTADAS' THEN '1. Aprobado'
      ELSE '2. Denegado'
    END AS RESULTADO_FINAL,

    CASE WHEN A.flg_campana='con_lead' THEN '1. Con lead' ELSE '2. Sin lead' END AS TIPO_LEAD_TMP,

    B.MTOSOLICITADO_SLF                       AS MTOSOLICITADO_SLF,
    COALESCE(A.MTO_DESEM,0)                   AS MTODESEMBOLSADOSOL_VTA,
    B.RESULTADO_CDA                           AS RESULTADO_CDA,
    A.CAMPANA                                 AS DESCAMPANA,
    B.DECISION                                AS DECISION,

    COALESCE(S.SEGMENTO,'OTROS')              AS SEGMENTO,
    NULL                                      AS MTO_OFERTA

  FROM catalog_lhcl_prod_bcp.bcp_edv_rbmper.T57182_CENTRALIZADO A
  LEFT JOIN BASE_DIGITAL B
    ON TRIM(B.CODINTERNOCOMPUTACIONAL)=TRIM(A.CLIENTE)
   AND CAST(A.CODMES AS INT)=B.CODMESEVALUACION
  LEFT JOIN SEGMENTO_CIC S
    ON S.CODMES = CAST(A.CODMES AS INT)
   AND S.CIC    = TRIM(A.CLIENTE)
  WHERE A.CODMES >= '202401'
    AND A.PRODUCTO='CC'
),

BASE AS (
  SELECT * FROM BASE_DIGITAL
  UNION ALL
  SELECT * FROM BASE_CENTRALIZADO
),

-- =========================
-- 10) BASE1 (resultado_final_unico + flags + num_solicitudes) igual notebook
-- =========================
BASE1 AS (
  SELECT
    A.CODMESEVALUACION,
    A.FECEVALUACION,
    A.CODINTERNOCOMPUTACIONAL,

    CASE
      WHEN D.CODINTERNOCOMPUTACIONAL IS NOT NULL THEN '1. Venta'
      WHEN B.CODINTERNOCOMPUTACIONAL IS NULL     THEN '2. Aprobado'
      WHEN C.CODINTERNOCOMPUTACIONAL IS NULL     THEN '3. Denegado'
      ELSE '4. Mix'
    END AS RESULTADO_FINAL_UNICO,

    A.TIPO_EVALUACION,
    A.TIPO_CANAL,
    A.CANAL_F,
    A.TIPO_LEAD_TMP AS TIPO_LEAD,
    A.RESULTADO_FINAL,
    A.SEGMENTO,

    CASE WHEN A.MTOSOLICITADO_SLF IS NULL OR A.MTOSOLICITADO_SLF=0 THEN 0 ELSE 1 END AS FLG_MTOSOLICITUD,
    CASE WHEN A.MTODESEMBOLSADOSOL_VTA IS NULL OR A.MTODESEMBOLSADOSOL_VTA=0 THEN 0 ELSE 1 END AS FLG_MTOVENTA,

    A.RESULTADO_CDA,
    A.DESCAMPANA,
    A.DECISION,
    A.MTODESEMBOLSADOSOL_VTA,
    A.MTOSOLICITADO_SLF,
    A.MTO_OFERTA,

    COUNT(1) OVER (PARTITION BY A.CODINTERNOCOMPUTACIONAL, A.CODMESEVALUACION) AS NUM_SOLICITUDES

  FROM BASE A
  LEFT JOIN (
    SELECT DISTINCT CODMESEVALUACION, CODINTERNOCOMPUTACIONAL
    FROM BASE
    WHERE RESULTADO_FINAL='2. Denegado'
  ) B ON A.CODMESEVALUACION=B.CODMESEVALUACION AND A.CODINTERNOCOMPUTACIONAL=B.CODINTERNOCOMPUTACIONAL

  LEFT JOIN (
    SELECT DISTINCT CODMESEVALUACION, CODINTERNOCOMPUTACIONAL
    FROM BASE
    WHERE RESULTADO_FINAL='1. Aprobado'
  ) C ON A.CODMESEVALUACION=C.CODMESEVALUACION AND A.CODINTERNOCOMPUTACIONAL=C.CODINTERNOCOMPUTACIONAL

  LEFT JOIN (
    SELECT DISTINCT CODMESEVALUACION, CODINTERNOCOMPUTACIONAL
    FROM BASE
    WHERE MTODESEMBOLSADOSOL_VTA > 0
  ) D ON A.CODMESEVALUACION=D.CODMESEVALUACION AND A.CODINTERNOCOMPUTACIONAL=D.CODINTERNOCOMPUTACIONAL
),

-- =========================
-- 11) BASE2 (RK): si hay venta, RK=1; si no, último FECEVALUACION
-- =========================
BASE2 AS (
  SELECT
    A.*,
    1 AS RK
  FROM BASE1 A
  WHERE A.MTODESEMBOLSADOSOL_VTA > 0

  UNION ALL

  SELECT
    A.*,
    ROW_NUMBER() OVER (PARTITION BY A.CODMESEVALUACION, A.CODINTERNOCOMPUTACIONAL ORDER BY A.FECEVALUACION DESC) AS RK
  FROM BASE1 A
  LEFT JOIN (
    SELECT DISTINCT CODMESEVALUACION, CODINTERNOCOMPUTACIONAL
    FROM BASE1
    WHERE MTODESEMBOLSADOSOL_VTA > 0
  ) B
    ON A.CODMESEVALUACION=B.CODMESEVALUACION
   AND A.CODINTERNOCOMPUTACIONAL=B.CODINTERNOCOMPUTACIONAL
  WHERE B.CODINTERNOCOMPUTACIONAL IS NULL
),

-- =========================
-- 12) Reglas: tmp_result (MAX_BY) igual notebook
-- =========================
TMP_RESULT AS (
  SELECT
    RESULTADO,
    MAX_BY(CLASIFICACION, CLASIFICACION) AS CLASIFICACION
  FROM catalog_lhcl_prod_bcp.bcp_edv_rbmper.Reglas
  GROUP BY RESULTADO
),

BASE_RK1 AS (
  SELECT *
  FROM BASE2
  WHERE RK=1
)

-- =========================
-- 13) FINAL: agregado (GROUP BY ALL) + TIPO_LEAD final CON_LEAD/SIN_LEAD + CLASIFICACION
-- =========================
SELECT
  A.CODMESEVALUACION AS CODMES,
  A.CODINTERNOCOMPUTACIONAL,

  CASE
    WHEN TRIM(A.CODINTERNOCOMPUTACIONAL)=TRIM(L.CIC) THEN 'CON_LEAD'
    ELSE 'SIN_LEAD'
  END AS TIPO_LEAD,

  A.TIPO_EVALUACION,
  A.TIPO_CANAL,
  A.CANAL_F,
  A.RESULTADO_FINAL,
  A.RESULTADO_FINAL_UNICO,
  A.SEGMENTO,
  A.FLG_MTOSOLICITUD,
  A.FLG_MTOVENTA,
  A.RESULTADO_CDA,

  CASE
    WHEN A.DECISION IN (
      'No cumple con filtro. Titular está en Archivo Negativo en estado activo reingreso o reiterativo con motivo grave.',
      'No cumple con filtro. Titular está en Archivo Negativo en estado activo\\, reingreso o reiterativo con motivo grave.'
    ) THEN '03.Archivo Neg'
    WHEN A.DECISION='Perfil_SegmentoNoPermitido_Decline_Titular' THEN '08.Perfil Riesgos'
    WHEN A.DECISION='ProdPasivos_BloqueoNoPermitido_Decline_Conyuge' THEN '05.Bloqueo BCP_Pas'
    ELSE COALESCE(R.CLASIFICACION, 'SIN_CLASIFICACION')
  END AS CLASIFICACION,

  A.DESCAMPANA,
  A.DECISION,

  COUNT(1) AS C,
  SUM(A.MTODESEMBOLSADOSOL_VTA) AS MTODESEMBOLSO,
  SUM(A.MTOSOLICITADO_SLF)     AS MTOSOLICITADO,
  SUM(A.NUM_SOLICITUDES)       AS NUM_SOLICITUDES,
  SUM(COALESCE(A.MTO_OFERTA,0)) AS MTO_OFERTA

FROM BASE_RK1 A
LEFT JOIN TMP_RESULT R
  ON A.DECISION = R.RESULTADO
LEFT JOIN LEADS_MES L
  ON A.CODMESEVALUACION = L.CODMES
 AND TRIM(A.CODINTERNOCOMPUTACIONAL)=TRIM(L.CIC)
GROUP BY ALL
;
