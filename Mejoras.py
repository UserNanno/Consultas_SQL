WITH
-- 1) Filtro temprano y normalizaciones mínimas
sol AS (
  SELECT
    A.codsolicitud,
    A.fecsolicitud,
    A.horsolicitud,
    A.destipevaluacionsolicitud,
    A.codproducto,
    A.destipestadosolicitud,
    A.destipestadoevaluacionanalista,
    A.destipestadoevaluacioncda,
    A.destipetapa,
    A.destipestadojustificacionsolicitud,
    A.destipmotivorechazo,
    A.descomentario,
    A.descampania,
    UPPER(A.descampania) AS descampania_u,  -- para búsquedas "contains" robustas
    A.tipevaluacionriesgo,
    A.nbrmoneda,
    TRIM(A.nbrmoneda) AS nbrmoneda_trim,
    A.mtosolicitado,
    A.mtoaprobado,
    A.feciniciocalif,
    A.horiniciocalif,
    A.fecfincalif,
    A.horfincalif,
    A.fecdesembolso,
    A.codmatriculacolaboradoranalista,
    A.codmatriculacolaboradorvendedor,
    A.codinternocomputacional,
    A.codcolaboradorvendedor,
    A.mtocuotamensualaprobado,
    A.ctdplazosolicitado,
    A.ctdplazoaprobado,
    A.pcttasaefectivaanualsolicitud,
    A.pcttasaefectivaanualaprobada,
    A.mtoingresobrutosolicitud
  FROM catalog_lhcl_prod_bcp.bcp_udv_int_v.m_solicitudcreditoconsumo A
  WHERE A.fecsolicitud >= DATE '2025-10-01'
),
-- 2) Dimensiones/join inputs prefiltradas por rol para reducir cardinalidad
ind_tit AS (
  SELECT
    codsolicitud,
    destiprolsolicitud,
    flgclipagohaberesbcp,
    tiprenta,
    numpuntajeprecalif,
    tippartyidentificacion,
    mtoingresodigitado,
    mtootroingresodigitado,
    mtoingresocalculado,
    mtocem,
    mtocemasesorventa,
    mtocemanalistacredito,
    dessubsegmento,
    mtodeudatotalbcp,
    codsegmentoriesgoscore,
    destipniveleducacional,
    destipocupacion,
    numdependiente,
    destipestcivil
  FROM bcp_udv_int_v.m_solicitudindividuo
  WHERE destiprolsolicitud = 'TITULAR'
),
ind_cony AS (
  SELECT
    codsolicitud,
    destiprolsolicitud,
    tiprenta,
    tippartyidentificacion,
    mtoingresodigitado,
    mtootroingresodigitado,
    mtoingresocalculado,
    codsegmentoriesgoscore,
    destipocupacion
  FROM bcp_udv_int_v.m_solicitudindividuo
  WHERE destiprolsolicitud = 'CONYUGE'
),
pty_tit AS (
  SELECT
    codsolicitud,
    destiprolsolicitud,
    destipclasifriesgocli
  FROM bcp_udv_int_v.m_solicitudparty
  WHERE destiprolsolicitud = 'TITULAR'
),
pty_cony AS (
  SELECT
    codsolicitud,
    destiprolsolicitud,
    destipclasifriesgocli
  FROM bcp_udv_int_v.m_solicitudparty
  WHERE destiprolsolicitud = 'CONYUGE'
),
usr_app AS (
  SELECT
    codusuarioapp,
    desposicioncolaborador
  FROM catalog_lhcl_prod_bcp.bcp_udv_int_v.m_usuarioaplicativo
),
-- 3) Timestamps compuestos (evita alias adelantados)
ts AS (
  SELECT
    s.*,
    TO_TIMESTAMP(
      CONCAT_WS(' ',
        CAST(s.fecsolicitud AS STRING),
        LPAD(CAST(s.horsolicitud AS STRING), 6, '0')
      ),
      'yyyy-MM-dd HHmmss'
    ) AS fecha_registro,
    TO_TIMESTAMP(
      CONCAT_WS(' ',
        CAST(s.feciniciocalif AS STRING),
        LPAD(CAST(s.horiniciocalif AS STRING), 6, '0')
      ),
      'yyyy-MM-dd HHmmss'
    ) AS inicio_evaluacion,
    TO_TIMESTAMP(
      CONCAT_WS(' ',
        CAST(s.fecfincalif AS STRING),
        LPAD(CAST(s.horfincalif AS STRING), 6, '0')
      ),
      'yyyy-MM-dd HHmmss'
    ) AS fin_evaluacion
  FROM sol s
),
-- 4) Join enriquecido (LEFT JOINs preservados)
jx AS (
  SELECT
    t.*,
    it.flgclipagohaberesbcp,
    it.tiprenta                           AS tiprenta_titular,
    it.numpuntajeprecalif,
    it.tippartyidentificacion             AS tipoidc_titular,
    it.mtoingresodigitado                 AS ingreso_bruto_titular,
    it.mtootroingresodigitado             AS otrosingreso_bruto_titular,
    it.mtoingresocalculado                AS ingreso_promedio_titular,
    it.mtocem                              AS cem,
    it.mtocemasesorventa                   AS cem_vendedor,
    it.mtocemanalistacredito               AS cem_analista,
    it.dessubsegmento                      AS segmentotitular,
    it.mtodeudatotalbcp                    AS posicion_consolidada,
    it.codsegmentoriesgoscore              AS segmentoriesgos_titular,
    it.destipniveleducacional              AS grado_instruccion,
    it.destipocupacion                     AS situacion_laboraltitular,
    it.numdependiente                      AS n_dependientestitular,
    it.destipestcivil                      AS estadocivil,

    ic.tiprenta                            AS tiprenta_conyuge,
    ic.tippartyidentificacion              AS tipoidc_conyuge,
    ic.mtoingresodigitado                  AS ingreso_bruto_conyuge,
    ic.mtootroingresodigitado              AS otrosingreso_bruto_conyuge,
    ic.mtoingresocalculado                 AS ingreso_promedio_conyuge,
    ic.codsegmentoriesgoscore              AS segmentoriesgos_conyuge,
    ic.destipocupacion                     AS situacion_laboralconyu ge,

    pt.destipclasifriesgocli               AS clasificacion_sbstitular,
    pc.destipclasifriesgocli               AS clasificacion_sbsconyuge,

    ua.desposicioncolaborador              AS funcionvendedor
  FROM ts t
  LEFT JOIN ind_tit  it ON t.codsolicitud = it.codsolicitud
  LEFT JOIN ind_cony ic ON t.codsolicitud = ic.codsolicitud
  LEFT JOIN pty_tit  pt ON t.codsolicitud = pt.codsolicitud
  LEFT JOIN pty_cony pc ON t.codsolicitud = pc.codsolicitud
  LEFT JOIN usr_app  ua ON t.codcolaboradorvendedor = ua.codusuarioapp
),
-- 5) Estado_F antes de cálculos que lo usan
estado AS (
  SELECT
    jx.*,
    CASE 
      WHEN jx.destipevaluacionsolicitud = 'EVALUACION AUTOMATICA' AND jx.destipestadosolicitud = 'ACEPTADA' THEN 'ACEPTADA'
      WHEN jx.destipevaluacionsolicitud = 'EVALUACION AUTOMATICA' AND jx.destipestadoevaluacioncda = 'Decline' AND jx.destipestadosolicitud = 'DESACTIVADA' THEN 'DENEGADA'
      WHEN jx.destipevaluacionsolicitud = 'EVALUACION AUTOMATICA' AND jx.destipestadosolicitud = 'DESACTIVADA' THEN 'DESACTIVADA'
      WHEN jx.destipevaluacionsolicitud = 'EVALUACION AUTOMATICA' AND jx.destipestadosolicitud = 'EN PROCESO' THEN 'EN PROCESO'
      WHEN jx.destipevaluacionsolicitud = 'EVALUACION AUTOMATICA' AND jx.destipestadosolicitud = 'SOLICITUD CANCELADA' THEN 'SOLICITUD CANCELADA'
      WHEN jx.destipevaluacionsolicitud = 'EVALUACION EN CENTRALIZADO' AND jx.destipestadoevaluacionanalista IN (
           'Aprobado por Analista de créditos','Aprobado por Gerente - Firmas y Desembolso') THEN 'ACEPTADA'
      WHEN jx.destipevaluacionsolicitud = 'EVALUACION EN CENTRALIZADO' AND jx.destipestadoevaluacionanalista IN (
           'Enviado a Analista de créditos','Enviado a Gerente - Documentación Adicional','Enviado a Gerente - Firmas y Desembolso') THEN 'ENVIADO'
      WHEN jx.destipevaluacionsolicitud = 'EVALUACION EN CENTRALIZADO' AND jx.destipestadoevaluacionanalista IN (
           'Rechazado por Gerente - Documentación Adicional','Rechazado por Gerente - Firmas y Desembolso') THEN 'DENEGADA'
      WHEN jx.destipevaluacionsolicitud = 'EVALUACION EN CENTRALIZADO' AND jx.destipestadoevaluacionanalista = 'Iniciado' THEN 'INICIADO'
      ELSE '?????'
    END AS estado_f
  FROM jx
),
-- 6) Conversión de montos (requiere estado_f) + banderas
money AS (
  SELECT
    e.*,
    CASE WHEN e.nbrmoneda_trim = 'DOLAR AMERICANO             EE.UU       ' THEN e.mtosolicitado * 3.81 ELSE e.mtosolicitado END AS montosolicitado_soles,
    CASE WHEN e.nbrmoneda_trim = 'DOLAR AMERICANO             EE.UU       ' THEN e.mtoaprobado   * 3.81 ELSE e.mtoaprobado   END AS montoaprobado_soles,
    -- mto_desembolsado: calc igual al original (si hay fecha de desembolso usa aprobado)
    CASE WHEN e.fecdesembolso IS NOT NULL THEN e.mtoaprobado ELSE NULL END AS mto_desembolsado,
    CASE WHEN e.nbrmoneda_trim = 'DOLAR AMERICANO             EE.UU       ' AND e.estado_f = 'ACEPTADA'
         THEN (CASE WHEN e.fecdesembolso IS NOT NULL THEN e.mtoaprobado ELSE NULL END) * 3.81
         ELSE (CASE WHEN e.fecdesembolso IS NOT NULL THEN e.mtoaprobado ELSE NULL END)
    END AS montodesembolsado_soles,
    CASE WHEN (CASE WHEN e.nbrmoneda_trim = 'DOLAR AMERICANO             EE.UU       ' AND e.estado_f = 'ACEPTADA'
                    THEN (CASE WHEN e.fecdesembolso IS NOT NULL THEN e.mtoaprobado ELSE NULL END) * 3.81
                    ELSE (CASE WHEN e.fecdesembolso IS NOT NULL THEN e.mtoaprobado ELSE NULL END)
               END) > 0
         THEN 'Si' ELSE 'No' END AS flag_montodesembolsado_soles
  FROM estado e
),
-- 7) Rango de score y rango de montos solicitados
clasif AS (
  SELECT
    m.*,
    CASE 
      WHEN m.numpuntajeprecalif IS NULL OR m.numpuntajeprecalif <= 0 THEN 'Sin data' 
      WHEN m.numpuntajeprecalif <= 300 THEN 'A. 0-300' 
      WHEN m.numpuntajeprecalif <= 330 THEN 'B. 300-330' 
      WHEN m.numpuntajeprecalif <= 360 THEN 'C. 330-360' 
      WHEN m.numpuntajeprecalif <= 390 THEN 'D. 360-390' 
      WHEN m.numpuntajeprecalif <= 420 THEN 'E. 390-420' 
      WHEN m.numpuntajeprecalif <= 450 THEN 'F. 420-450'  -- corrige 'F. 42-450'
      ELSE NULL
    END AS rango_score,
    CASE 
      WHEN m.montosolicitado_soles IS NULL OR m.montosolicitado_soles <= 0 THEN 'Sin data' 
      WHEN m.montosolicitado_soles <= 23000 THEN 'A. 0-23k' 
      WHEN m.montosolicitado_soles <= 46000 THEN 'B. 23k-46k' 
      WHEN m.montosolicitado_soles <= 69000 THEN 'C. 46k-69k' 
      WHEN m.montosolicitado_soles <= 92000 THEN 'D. 69k-92k' 
      WHEN m.montosolicitado_soles <= 115000 THEN 'E. 92k-115k' 
      WHEN m.montosolicitado_soles <= 138000 THEN 'F. 115k-138k' 
      WHEN m.montosolicitado_soles <= 160000 THEN 'G. 138k-160k' 
      ELSE 'H. >160k'
    END AS rango_montosolicitado_soles
  FROM money m
),
-- 8) Métricas de tiempo y sus rangos (usar epoch diff)
tcal AS (
  SELECT
    c.*,
    (UNIX_TIMESTAMP(c.fin_evaluacion)   - UNIX_TIMESTAMP(c.fecha_registro))         AS totalsegundo_tiempocliente,
    (UNIX_TIMESTAMP(c.inicio_evaluacion) - UNIX_TIMESTAMP(c.fecha_registro))        AS _tmp_gap_ignorar,
    (UNIX_TIMESTAMP(c.fin_evaluacion)   - UNIX_TIMESTAMP(c.inicio_evaluacion))      AS totalsegundo_tiempoanalista,
    (UNIX_TIMESTAMP(c.fin_evaluacion)   - UNIX_TIMESTAMP(c.fecha_registro)) / 3600.0   AS tiempocliente_hrs,
    (UNIX_TIMESTAMP(c.fin_evaluacion)   - UNIX_TIMESTAMP(c.inicio_evaluacion)) / 3600.0 AS tiempoanalista_hrs,
    CASE 
      WHEN (UNIX_TIMESTAMP(c.fin_evaluacion) - UNIX_TIMESTAMP(c.fecha_registro)) / 3600.0 IS NULL THEN NULL
      WHEN (UNIX_TIMESTAMP(c.fin_evaluacion) - UNIX_TIMESTAMP(c.fecha_registro)) / 3600.0 <= 24 THEN 'Dentro 24hrs'
      ELSE 'Mayor 24hrs' END AS atencion24hrscliente,
    CASE 
      WHEN (UNIX_TIMESTAMP(c.fin_evaluacion) - UNIX_TIMESTAMP(c.fecha_registro)) / 3600.0 IS NULL THEN NULL
      WHEN (UNIX_TIMESTAMP(c.fin_evaluacion) - UNIX_TIMESTAMP(c.fecha_registro)) / 3600.0 <= 12 THEN 'Dentro 12hrs'
      WHEN (UNIX_TIMESTAMP(c.fin_evaluacion) - UNIX_TIMESTAMP(c.fecha_registro)) / 3600.0 <= 24 THEN 'Dentro 24hrs'
      ELSE 'Mayor 24hrs' END AS atencion12hrscliente,
    CASE 
      WHEN (UNIX_TIMESTAMP(c.fin_evaluacion) - UNIX_TIMESTAMP(c.fecha_registro)) / 3600.0 IS NULL THEN NULL
      WHEN (UNIX_TIMESTAMP(c.fin_evaluacion) - UNIX_TIMESTAMP(c.fecha_registro)) / 3600.0 <= 0 THEN 'Sin data' 
      WHEN (UNIX_TIMESTAMP(c.fin_evaluacion) - UNIX_TIMESTAMP(c.fecha_registro)) / 3600.0 <= 3 THEN 'A. 0-3' 
      WHEN (UNIX_TIMESTAMP(c.fin_evaluacion) - UNIX_TIMESTAMP(c.fecha_registro)) / 3600.0 <= 6 THEN 'B. 3-6' 
      WHEN (UNIX_TIMESTAMP(c.fin_evaluacion) - UNIX_TIMESTAMP(c.fecha_registro)) / 3600.0 <= 12 THEN 'C. 6-12' 
      WHEN (UNIX_TIMESTAMP(c.fin_evaluacion) - UNIX_TIMESTAMP(c.fecha_registro)) / 3600.0 <= 18 THEN 'D. 12-18' 
      WHEN (UNIX_TIMESTAMP(c.fin_evaluacion) - UNIX_TIMESTAMP(c.fecha_registro)) / 3600.0 <= 24 THEN 'E. 18-24' 
      ELSE 'F. >24' END AS rangoatencioncliente
  FROM clasif c
),
-- 9) Campañas (mantiene lógica original, usando UPPER + INSTR para "contains")
camp AS (
  SELECT
    t.*,
    CASE WHEN t.descampania IS NOT NULL THEN t.descampania ELSE 'SIN CAMPAÑA' END AS campana,
    CASE
      WHEN INSTR(t.descampania_u,'CONVENIO') > 0 THEN 'CONVENIO'
      WHEN INSTR(t.descampania_u,'SHIELD')   > 0 THEN 'SHIELD'
      WHEN t.tipevaluacionriesgo = 'APR'     THEN '100% APROBADO'
      WHEN t.tipevaluacionriesgo = 'REA'     THEN 'SIN CAMPAÑA'
      WHEN t.tipevaluacionriesgo = 'PRE'     THEN 'PREAPROBADO'
      WHEN t.tipevaluacionriesgo = 'INV'     THEN 'INVITADO'
      WHEN t.tipevaluacionriesgo = 'OPT'     THEN 'OPTIMUS'
      ELSE NULL
    END AS campanacorta2
  FROM tcal t
),
-- 10) Etiquetas de producto y detalle de producto (constantes + mapping)
prod AS (
  SELECT
    c.*,
    'CREDITO CONSUMO' AS tipoproducto,
    'CEF'             AS subproducto,
    CASE
      WHEN c.codproducto = 'EFME' THEN 'LD'
      WHEN c.codproducto = 'EFDP' THEN 'CONVENIO'
      WHEN c.codproducto = 'EFCD' THEN 'CD'
      ELSE NULL
    END AS detproducto
  FROM camp c
)

-- NOTA: se mantiene DISTINCT global como en el SELECT original
SELECT DISTINCT
  p.codsolicitud AS codsolicitud,
  p.fecha_registro AS fecha_registro,
  p.destipevaluacionsolicitud AS tipoevaluacion,
  p.tipoproducto AS tipoproducto,
  p.subproducto AS subproducto,
  p.detproducto AS detproducto,
  p.destipestadosolicitud AS estadosolicitud,
  p.destipestadoevaluacionanalista AS resultadoanalista,
  p.estado_f AS estado_f,
  p.destipestadoevaluacioncda AS resultadomotorcda,
  CAST(NULL AS STRING) AS motivocda,
  p.destipetapa AS etapa,
  COALESCE(p.destipestadojustificacionsolicitud, p.destipmotivorechazo) AS justificacion_analista,
  p.descomentario AS comentarioanalista,
  p.campana AS campana,
  p.campanacorta2 AS campanacorta2,
  p.nbrmoneda AS moneda,
  p.mtosolicitado AS montosolicitado,
  p.mtoaprobado AS montoaprobado,
  p.inicio_evaluacion AS inicio_evaluacion,
  p.fin_evaluacion AS fin_evaluacion,
  p.fecdesembolso AS fecdesembolso,
  p.codmatriculacolaboradoranalista AS matriculaanalista,
  p.codmatriculacolaboradorvendedor AS matriculavendedor,
  p.codinternocomputacional AS codclavecic,

  CASE WHEN p.destiprolsolicitud = 'TITULAR' OR p.flgclipagohaberesbcp IS NOT NULL THEN p.flgclipagohaberesbcp END AS cliente_pdh,
  p.tiprenta_titular AS tipo_rentatitular,
  p.tiprenta_conyuge AS tipo_rentaconyuge,
  p.numpuntajeprecalif AS scoreevaluacion,
  p.tipoidc_titular AS tipoidc_titular,
  p.ingreso_bruto_titular AS ingreso_bruto_titular,
  p.otrosingreso_bruto_titular AS otrosingreso_bruto_titular,
  p.ingreso_promedio_titular AS ingreso_promedio_titular,
  p.tipoidc_conyuge AS tipoidc_conyuge,
  p.ingreso_bruto_conyuge AS ingreso_bruto_conyuge,
  p.otrosingreso_bruto_conyuge AS otrosingreso_bruto_conyuge,
  p.ingreso_promedio_conyuge AS ingreso_promedio_conyuge,

  p.mtocuotamensualaprobado AS cuotamensualaprobado,
  p.ctdplazosolicitado AS plazosolicitado,
  p.ctdplazoaprobado AS plazoaprobado,
  p.pcttasaefectivaanualsolicitud AS tasasolicitada,
  p.pcttasaefectivaanualaprobada AS tasaaprobada,

  p.cem,
  p.cem_vendedor,
  p.cem_analista,
  p.segmentotitular AS segmentotitular,
  p.posicion_consolidada AS posicion_consolidada,
  p.segmentoriesgos_titular AS segmentoriesgosTitular,
  p.segmentoriesgos_conyuge AS segmentoriesgosConyuge,

  p.mto_desembolsado AS mto_desembolsado,
  p.grado_instruccion AS grado_instruccion,
  p.situacion_laboraltitular AS situacion_laboraltitular,
  p.situacion_laboralconyu ge AS situacion_laboralconyu ge,
  p.clasificacion_sbstitular AS clasificacion_sbstitular,
  p.clasificacion_sbsconyuge AS clasificacion_sbsconyuge,
  p.n_dependientestitular AS n_dependientestitular,
  p.estadocivil AS estadocivil,
  p.funcionvendedor AS funcionvendedor,
  p.mtoingresobrutosolicitud AS ingresoverificado,

  DAY(p.fecsolicitud) AS diasolicitud,
  MONTH(p.fecsolicitud) AS messolicitud,
  YEAR(p.fecsolicitud) AS anosolicitud,
  DATE_FORMAT(p.fecsolicitud, 'yyyyMM') AS anomessolicitud,

  p.rango_score AS rango_score,
  p.montosolicitado_soles AS montosolicitado_soles,
  p.montoaprobado_soles AS montoaprobado_soles,
  p.montodesembolsado_soles AS montodesembolsado_soles,
  p.flag_montodesembolsado_soles AS flag_montodesembolsado_soles,
  p.rango_montosolicitado_soles AS rango_montosolicitado_soles,

  p.tiempocliente_hrs AS tiempocliente_hrs,
  p.tiempoanalista_hrs AS tiempoanalista_hrs,
  p.totalsegundo_tiempocliente AS totalsegundo_tiempocliente,
  p.totalsegundo_tiempoanalista AS totalsegundo_tiempoanalista,
  p.atencion24hrscliente AS atencion24hrscliente,
  p.atencion12hrscliente AS atencion12hrscliente,
  p.rangoatencioncliente AS rangoatencioncliente

FROM prod p;