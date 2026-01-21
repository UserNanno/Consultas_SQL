BEGIN
  DECLARE v_mes INT DEFAULT 202501;
  DECLARE v_mes_end INT DEFAULT 202512;
  DECLARE v_mes_1 INT;
  DECLARE v_sql STRING;

  bucle: LOOP

    -- calcular mes siguiente (YYYYMM)
    IF MOD(v_mes, 100) = 12 THEN
      SET v_mes_1 = (DIV(v_mes, 100) + 1) * 100 + 1;   -- pasa de YYYY12 a (YYYY+1)01
    ELSE
      SET v_mes_1 = v_mes + 1;
    END IF;

    SET v_sql = '
    INSERT OVERWRITE TABLE catalog_lhcl_prod_bcp.bcp_edv_rbmbdn.T72496_SOLICITUDES_HIST
    PARTITION (CODMES=' || CAST(v_mes AS STRING) || ')
    WITH
    PARAMS AS (
      SELECT ' || CAST(v_mes AS STRING) || ' AS CODMES, ' || CAST(v_mes_1 AS STRING) || ' AS CODMES_1
    ),

    RBM_BASE AS (
      SELECT
        CAST(date_format(A.FECEVALUACION,''yyyyMM'') AS INT)  AS CODMES_EVAL,
        CAST(A.FECEVALUACION AS DATE)                        AS FEC_EVALUACION,
        TRIM(A.CODINTERNOCOMPUTACIONAL)                      AS CIC,
        TRIM(A.NUMSOLICITUDEVALUACION)                       AS NUMSOLICITUD_RBM,
        A.DESCANALVENTARBMPER                                AS CANAL_RBM,
        A.DESTIPDECISIONRESULTADORBM                         AS RESULTADO_CDA,
        A.DESDECISIONEVALUACION                              AS DECISION_RBM,
        A.DESCAMPANIASOLICITUD                               AS CAMPANIA_RBM,
        A.DESTIPEVALUACIONSOLICITUDCREDITO                   AS TIPO_CAMPANIA_RBM,

        CASE
          WHEN A.DESCANALVENTARBMPER=''BANCA MÓVIL'' AND A.DESEVALUACION=''df_Regular'' THEN ''0.No valido''
          WHEN A.DESCAMPANIASOLICITUD=''Reactivo'' THEN ''1.Reactivo''
          ELSE ''2.No Reactivo''
        END AS EVALUACION_REACTIVO,

        CASE
          WHEN A.DESCANALVENTARBMPER=''LOANS'' THEN SUBSTR(TRIM(A.NUMSOLICITUDEVALUACION), 3, 8)
          ELSE TRIM(A.NUMSOLICITUDEVALUACION)
        END AS NUMSOLICITUD_CORTO,

        CASE
          WHEN A.DESTIPDECISIONRESULTADORBM=''Approve'' THEN 1
          WHEN A.DESTIPDECISIONRESULTADORBM=''Decline'' THEN 3
          ELSE 2
        END AS RESULTADO_JERARQ,

        CASE
          WHEN A.DESTIPEVALUACIONSOLICITUDCREDITO IN (''Regular LD'',''Cuotealo'')
            AND A.DESCAMPANIASOLICITUD IN (''100% Aprobado'',''Pre-Aprobado'') THEN ''LD APR''
          WHEN A.DESTIPEVALUACIONSOLICITUDCREDITO IN (''Regular LD'',''Cuotealo'')
            AND A.DESCAMPANIASOLICITUD=''CEF Shield'' THEN ''LD SHD''
          WHEN A.DESTIPEVALUACIONSOLICITUDCREDITO=''Regular LD'' AND A.DESCAMPANIASOLICITUD=''Convenio'' THEN ''LD CONV''
          WHEN A.DESTIPEVALUACIONSOLICITUDCREDITO=''Regular LD'' AND A.DESCAMPANIASOLICITUD=''Invitado'' THEN ''LD INV''

          WHEN A.DESTIPEVALUACIONSOLICITUDCREDITO=''Compra de Deuda''
            AND A.DESCAMPANIASOLICITUD IN (''100% Aprobado'',''Pre-Aprobado'') THEN ''CDD APR''
          WHEN A.DESTIPEVALUACIONSOLICITUDCREDITO=''Compra de Deuda'' AND A.DESCAMPANIASOLICITUD=''Convenio'' THEN ''CDD CONV''

          WHEN A.DESTIPEVALUACIONSOLICITUDCREDITO=''LD + Consolidación''
            AND A.DESCAMPANIASOLICITUD IN (''100% Aprobado'',''Pre-Aprobado'') THEN ''REE APR''
          WHEN A.DESTIPEVALUACIONSOLICITUDCREDITO=''LD + Consolidación'' AND A.DESCAMPANIASOLICITUD=''Convenio'' THEN ''REE CONV''

          WHEN A.DESTIPEVALUACIONSOLICITUDCREDITO=''LD + Compra de Deuda + Consolidación''
            AND A.DESCAMPANIASOLICITUD IN (''100% Aprobado'',''Pre-Aprobado'') THEN ''CONS APR''
          WHEN A.DESTIPEVALUACIONSOLICITUDCREDITO=''LD + Compra de Deuda + Consolidación'' AND A.DESCAMPANIASOLICITUD=''Convenio'' THEN ''CONS CONV''

          ELSE ''REACTIVO''
        END AS LEAD_CEF

      FROM catalog_lhcl_prod_bcp.bcp_ddv_rbmrbmper_modelogestion_vu.md_evaluacionsolicitudcredito A
      CROSS JOIN PARAMS P
      WHERE A.TIPPRODUCTOSOLICITUDRBM=''CC''
        AND A.DESCANALVENTARBMPER NOT IN (''YAPE'',''OTROS'',''SALEFORCE'')
        AND A.CODMESEVALUACION = P.CODMES
    ),

    RBM_UNICOS AS (
      SELECT *
      FROM RBM_BASE
      WHERE EVALUACION_REACTIVO <> ''0.No valido''
      QUALIFY ROW_NUMBER() OVER (
        PARTITION BY CIC, NUMSOLICITUD_RBM
        ORDER BY RESULTADO_JERARQ ASC, FEC_EVALUACION DESC
      ) = 1
    ),

    TC_BASE AS (
      SELECT
        CAST(date_format(A.FECTIPCAMBIO,''yyyyMM'') AS INT) AS CODMES_TC,
        A.FECTIPCAMBIO                                  AS FEC_TC,
        A.CODMONEDAORIGEN                               AS CODMONEDA_ORIGEN,
        A.MTOCAMBIOMONEDAORIGENMONEDADESTINO            AS TC_A_SOL
      FROM catalog_lhcl_prod_bcp.bcp_udv_int_vu.H_TIPOCAMBIO A
      CROSS JOIN PARAMS P
      WHERE A.CODMONEDADESTINO=''0001''
        AND A.CODAPP=''GLM''
        AND CAST(date_format(A.FECTIPCAMBIO,''yyyyMM'') AS INT) BETWEEN P.CODMES AND P.CODMES_1
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

    CLIENTE AS (
      SELECT
        CODCLAVEPARTYCLI,
        TRIM(CODINTERNOCOMPUTACIONAL) AS CIC
      FROM catalog_lhcl_prod_bcp.bcp_udv_int_vu.M_CLIENTE
    ),

    VTA_CUENTA AS (
      SELECT
        CAST(date_format(A.FECAPERTURA,''yyyyMM'') AS INT) AS CODMES_APERTURA,
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
      WHERE CAST(date_format(A.FECAPERTURA,''yyyyMM'') AS INT) BETWEEN P.CODMES AND P.CODMES_1
        AND A.FLGREGELIMINADOFUENTE=''N''
        AND A.CODPRODUCTO IN (''CPEEFM'',''CPECMC'',''CPEDPP'',''CPECEM'',''CPEECV'',''CPEGEN'',''CPEADH'',''CPEFIA'')
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
          WHEN A.CODMONEDA_VTA <> ''0001'' THEN A.MTO_DESEMBOLSADO_VTA * COALESCE(T.TC_A_SOL, 0)
          ELSE A.MTO_DESEMBOLSADO_VTA
        END AS MTO_DESEMBOLSO_SOL_VTA,

        CASE
          WHEN SUBSTR(TRIM(A.CODSOLICITUD_VTA),1,2) IN (''CX'',''DX'') THEN SUBSTR(TRIM(A.CODSOLICITUD_VTA),3,8)
          ELSE TRIM(A.CODSOLICITUD_VTA)
        END AS NUMSOLICITUD_CORTO_VTA,

        CASE
          WHEN A.CODPRODUCTO_VTA=''CPEFIA''                          THEN ''CUOTEALO''
          WHEN SUBSTR(TRIM(A.CODSOLICITUD_VTA),1,2)=''PE''           THEN ''CUOTEALO''
          WHEN SUBSTR(TRIM(A.CODSOLICITUD_VTA),1,2)=''JB''           THEN ''BANCA MÓVIL''
          WHEN SUBSTR(TRIM(A.CODSOLICITUD_VTA),1,2) IN (''CX'',''DX'') THEN ''LOANS''
          ELSE ''OTROS''
        END AS CANAL_VTA

      FROM VTA_CUENTA A
      LEFT JOIN TC_MES T
        ON A.CODMES_APERTURA = T.CODMES_TC
       AND A.CODMONEDA_VTA   = T.CODMONEDA_ORIGEN
      LEFT JOIN CLIENTE C
        ON A.CODCLAVEPARTYCLI = C.CODCLAVEPARTYCLI
    ),

    SOL_CONSUMO AS (
      SELECT
        CAST(date_format(A.FECSOLICITUD,''yyyyMM'') AS INT) AS CODMES_SOL,
        CAST(A.FECSOLICITUD AS DATE)                      AS FEC_SOLICITUD,
        TRIM(A.CODSOLICITUD)                              AS NUMSOLICITUD_RBM,
        TRIM(A.CODINTERNOCOMPUTACIONAL)                   AS CIC,
        A.MTOSOLICITADO                                   AS MTO_SOLICITADO,
        A.MTOAPROBADO                                     AS MTO_APROBADO
      FROM catalog_lhcl_prod_bcp.bcp_udv_int_vu.m_solicitudcreditoconsumo A
      CROSS JOIN PARAMS P
      WHERE CAST(date_format(A.FECSOLICITUD,''yyyyMM'') AS INT) = P.CODMES
    ),

    RBM_VTA_SOL AS (
      SELECT
        A.CODMES_EVAL AS CODMES,
        A.FEC_EVALUACION,
        A.CIC,
        A.NUMSOLICITUD_RBM,
        A.NUMSOLICITUD_CORTO,
        A.CANAL_RBM,
        A.RESULTADO_CDA,
        A.DECISION_RBM,
        A.CAMPANIA_RBM,
        A.TIPO_CAMPANIA_RBM,
        A.LEAD_CEF,
        B.MTO_DESEMBOLSO_SOL_VTA,
        C.FEC_SOLICITUD,
        C.MTO_SOLICITADO,
        C.MTO_APROBADO
      FROM RBM_UNICOS A
      LEFT JOIN VENTAS_CONSOL B
        ON A.NUMSOLICITUD_CORTO = B.NUMSOLICITUD_CORTO_VTA
       AND A.CANAL_RBM          = B.CANAL_VTA
       AND A.CIC                = B.CIC
      LEFT JOIN SOL_CONSUMO C
        ON A.CODMES_EVAL        = C.CODMES_SOL
       AND A.NUMSOLICITUD_RBM   = C.NUMSOLICITUD_RBM
    ),

    LEADS_RAW AS (
      SELECT
        ROUND(A.CODMES,0)               AS CODMES_LEAD,
        TRIM(A.CODINTERNOCOMPUTACIONAL) AS CIC,
        ROUND(A.TIPVENTA,0)             AS TIPVENTA,
        ROUND(A.TIPOFERTA,0)            AS TIPOFERTA,
        A.DESCAMPANA                    AS DESCAMPANA_LEAD,
        A.CODCONDICIONCLIENTE,
        ROUND(A.MTOFINALOFERTADOSOL,1)  AS MTO_OFERTA,
        ROUND(A.NUMPLAZO,0)             AS PLAZO_LEAD,
        A.PCTTASAEFECTIVAANUAL          AS TEA_LEAD,
        A.FECINICIOVIGENCIA,

        CASE
          WHEN A.TIPVENTA=6   AND A.TIPOFERTA=3  AND A.CODCONDICIONCLIENTE IN (''APR'',''PRE'') THEN 1
          WHEN A.TIPVENTA=6   AND A.TIPOFERTA=3  AND A.CODCONDICIONCLIENTE=''OPT''              THEN 2
          WHEN A.TIPVENTA=6   AND A.TIPOFERTA=41 AND A.CODCONDICIONCLIENTE IN (''APR'',''PRE'') THEN 3
          WHEN A.TIPVENTA=6   AND A.TIPOFERTA=38 AND A.CODCONDICIONCLIENTE IN (''APR'',''PRE'') THEN 4
          WHEN A.TIPVENTA=6   AND A.TIPOFERTA IN (89,98)                                       THEN 5
          WHEN A.TIPVENTA=6   AND A.TIPOFERTA=3  AND A.CODCONDICIONCLIENTE=''INV''              THEN 6
          WHEN A.TIPVENTA=6   AND A.TIPOFERTA=187                                               THEN 7
          WHEN A.TIPVENTA=110 AND A.TIPOFERTA=3                                                  THEN 8
          WHEN A.TIPVENTA=110 AND A.TIPOFERTA=41                                                 THEN 9
          WHEN A.TIPVENTA=110 AND A.TIPOFERTA=38                                                 THEN 10
          ELSE 11
        END AS PRIORIDAD_LEAD,

        CASE
          WHEN A.TIPVENTA=6   AND A.TIPOFERTA=3  AND A.CODCONDICIONCLIENTE IN (''APR'',''PRE'') THEN ''LD APR''
          WHEN A.TIPVENTA=6   AND A.TIPOFERTA=3  AND A.CODCONDICIONCLIENTE=''OPT''              THEN ''LD OPT''
          WHEN A.TIPVENTA=6   AND A.TIPOFERTA IN (89,98)                                       THEN ''LD SHD''
          WHEN A.TIPVENTA=6   AND A.TIPOFERTA=41 AND A.CODCONDICIONCLIENTE IN (''APR'',''PRE'') THEN ''CDD APR''
          WHEN A.TIPVENTA=6   AND A.TIPOFERTA=187                                               THEN ''CDD INS''
          WHEN A.TIPVENTA=6   AND A.TIPOFERTA=38 AND A.CODCONDICIONCLIENTE IN (''APR'',''PRE'') THEN ''REE APR''
          WHEN A.TIPVENTA=6   AND A.TIPOFERTA=3  AND A.CODCONDICIONCLIENTE=''INV''              THEN ''LD INV''
          WHEN A.TIPVENTA=110 AND A.TIPOFERTA=3                                                  THEN ''LD CONV''
          WHEN A.TIPVENTA=110 AND A.TIPOFERTA=41                                                 THEN ''CDD CONV''
          WHEN A.TIPVENTA=110 AND A.TIPOFERTA=38                                                 THEN ''REE CONV''
          ELSE ''OTRO''
        END AS LEAD_CEF
      FROM catalog_lhcl_prod_bcp.bcp_edv_rbmper.hm_bdi A
      CROSS JOIN PARAMS P
      WHERE A.CODPAUTARBM <> 41
        AND (A.TIPVENTA = 6 OR A.TIPVENTA = 110)
        AND A.DESCAMPANA NOT IN (''CREDITO PERSONAL, VENTA AMPLIACION PLAZO'', ''VENTA SKIP'')
        AND to_date(A.FECINICIOVIGENCIA,''yyyyMM'') = date_trunc(''MM'', to_date(cast(P.CODMES AS STRING),''yyyyMM''))
    ),

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

    LEADS_MES AS (
      SELECT DISTINCT CODMES_LEAD AS CODMES, CIC
      FROM LEADS_BASE
    ),

    SOLICITUDES_LEAD AS (
      SELECT
        A.*,
        COALESCE(B.MTO_OFERTA, C.MTO_OFERTA) AS MTO_OFERTA_LEAD
      FROM RBM_VTA_SOL A
      LEFT JOIN LEADS_UNICOS B
        ON A.CIC = B.CIC
       AND A.LEAD_CEF = B.LEAD_CEF
      LEFT JOIN LEADS_UNICOS C
        ON A.CIC = C.CIC
    ),

    BASE AS (
      SELECT DISTINCT
        A.CODMES                                 AS CODMESEVALUACION,
        A.FEC_EVALUACION                         AS FECEVALUACION,
        A.CIC                                    AS CODINTERNOCOMPUTACIONAL,
        A.CANAL_RBM                               AS TIPO_CANAL,
        ''DIGITAL''                               AS CANAL_F,
        ''PUNTO DE CONTACTO''                     AS TIPO_EVALUACION,

        CASE
          WHEN COALESCE(A.MTO_DESEMBOLSO_SOL_VTA,0) > 0 THEN ''1. Aprobado''
          WHEN A.RESULTADO_CDA=''Approve''             THEN ''1. Aprobado''
          ELSE ''2. Denegado''
        END AS RESULTADO_FINAL,

        CASE WHEN A.MTO_OFERTA_LEAD IS NOT NULL THEN ''1. Con lead'' ELSE ''2. Sin lead'' END AS TIPO_LEAD_TMP,

        A.MTO_SOLICITADO                          AS MTOSOLICITADO_SLF,
        COALESCE(A.MTO_DESEMBOLSO_SOL_VTA,0)       AS MTODESEMBOLSADOSOL_VTA,
        A.RESULTADO_CDA                           AS RESULTADO_CDA,
        A.CAMPANIA_RBM                            AS DESCAMPANA,
        A.DECISION_RBM                            AS DECISION,
        COALESCE(A.MTO_OFERTA_LEAD,0)             AS MTO_OFERTA
      FROM SOLICITUDES_LEAD A
    ),

    BASE1 AS (
      SELECT
        A.CODMESEVALUACION,
        A.FECEVALUACION,
        A.CODINTERNOCOMPUTACIONAL,

        CASE
          WHEN D.CODINTERNOCOMPUTACIONAL IS NOT NULL THEN ''1. Venta''
          WHEN B.CODINTERNOCOMPUTACIONAL IS NULL     THEN ''2. Aprobado''
          WHEN C.CODINTERNOCOMPUTACIONAL IS NULL     THEN ''3. Denegado''
          ELSE ''4. Mix''
        END AS RESULTADO_FINAL_UNICO,

        A.TIPO_EVALUACION,
        A.TIPO_CANAL,
        A.CANAL_F,
        A.TIPO_LEAD_TMP AS TIPO_LEAD,
        A.RESULTADO_FINAL,

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
        WHERE RESULTADO_FINAL=''2. Denegado''
      ) B ON A.CODMESEVALUACION=B.CODMESEVALUACION AND A.CODINTERNOCOMPUTACIONAL=B.CODINTERNOCOMPUTACIONAL

      LEFT JOIN (
        SELECT DISTINCT CODMESEVALUACION, CODINTERNOCOMPUTACIONAL
        FROM BASE
        WHERE RESULTADO_FINAL=''1. Aprobado''
      ) C ON A.CODMESEVALUACION=C.CODMESEVALUACION AND A.CODINTERNOCOMPUTACIONAL=C.CODINTERNOCOMPUTACIONAL

      LEFT JOIN (
        SELECT DISTINCT CODMESEVALUACION, CODINTERNOCOMPUTACIONAL
        FROM BASE
        WHERE MTODESEMBOLSADOSOL_VTA > 0
      ) D ON A.CODMESEVALUACION=D.CODMESEVALUACION AND A.CODINTERNOCOMPUTACIONAL=D.CODINTERNOCOMPUTACIONAL
    ),

    BASE2 AS (
      SELECT A.*, 1 AS RK
      FROM BASE1 A
      WHERE COALESCE(A.MTODESEMBOLSADOSOL_VTA,0) > 0

      UNION ALL

      SELECT
        A.*,
        ROW_NUMBER() OVER (PARTITION BY A.CODMESEVALUACION, A.CODINTERNOCOMPUTACIONAL ORDER BY A.FECEVALUACION DESC) AS RK
      FROM BASE1 A
      LEFT JOIN (
        SELECT DISTINCT CODMESEVALUACION, CODINTERNOCOMPUTACIONAL
        FROM BASE1
        WHERE COALESCE(MTODESEMBOLSADOSOL_VTA,0) > 0
      ) B
        ON A.CODMESEVALUACION=B.CODMESEVALUACION
       AND A.CODINTERNOCOMPUTACIONAL=B.CODINTERNOCOMPUTACIONAL
      WHERE B.CODINTERNOCOMPUTACIONAL IS NULL
    ),

    BASE_RK1 AS (
      SELECT * FROM BASE2 WHERE RK=1
    )

    SELECT
      ' || CAST(v_mes AS STRING) || ' AS CODMES,
      A.CODINTERNOCOMPUTACIONAL,

      CASE WHEN L.CIC IS NOT NULL THEN ''CON_LEAD'' ELSE ''SIN_LEAD'' END AS TIPO_LEAD,

      A.TIPO_EVALUACION,
      A.TIPO_CANAL,
      A.CANAL_F,
      A.RESULTADO_FINAL,
      A.RESULTADO_FINAL_UNICO,
      A.FLG_MTOSOLICITUD,
      A.FLG_MTOVENTA,
      A.RESULTADO_CDA,
      A.DESCAMPANA,
      A.DECISION,

      COUNT(1)                               AS C,
      SUM(A.MTODESEMBOLSADOSOL_VTA)          AS MTODESEMBOLSO,
      SUM(A.MTOSOLICITADO_SLF)              AS MTOSOLICITADO,
      MAX(A.NUM_SOLICITUDES)                AS NUM_SOLICITUDES,
      SUM(COALESCE(A.MTO_OFERTA,0))         AS MTO_OFERTA

    FROM BASE_RK1 A
    LEFT JOIN LEADS_MES L
      ON L.CODMES = ' || CAST(v_mes AS STRING) || '
     AND TRIM(L.CIC)=TRIM(A.CODINTERNOCOMPUTACIONAL)
    GROUP BY ALL
    ';

    EXECUTE IMMEDIATE v_sql;

    IF v_mes = v_mes_end THEN
      LEAVE bucle;
    END IF;

    -- siguiente mes YYYYMM
    IF MOD(v_mes, 100) = 12 THEN
      SET v_mes = (DIV(v_mes, 100) + 1) * 100 + 1;
    ELSE
      SET v_mes = v_mes + 1;
    END IF;

    ITERATE bucle;

  END LOOP bucle;
END;
