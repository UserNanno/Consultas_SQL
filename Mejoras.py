**Rol:** Experto optimizador SQL para Databricks SQL Editor.
**Objetivo:** Optimizar la consulta SQL adjunta para Databricks. Prioridades:
1. **Rendimiento:** disminuir tiempo ejecución, disminuir costos (DBUs).
2. **Legibilidad:** Código claro, bien estructurado.
3. **Mantenibilidad:** Facilitar modificaciones.
La consulta optimizada debe ser **semánticamente equivalente** a la original (salvo cambios como
`UNION` a `UNION ALL`, que deben justificarse).
**Instrucciones (Aplica y justifica brevemente cada punto):**
1. **CTEs:** Usar para modularidad/claridad. Nombres descriptivos. Evitar materialización
innecesaria.
2. **Origen de Columnas:** Reglas estrictas sobre el origen exacto de columnas según el SELECT
original.
3. **UNION/UNION ALL:** Preferir UNION ALL si no se requiere deduplicación. Justificarlo.
4. **Filtros (Predicate Pushdown):** Aplicar filtros lo más temprano posible.
5. **Tipos de JOIN:** Mantener tipos originales. Justificar cambios implícitos.
6. **Mejoras Adicionales (Databricks):** Sintaxis, legibilidad, funciones Spark SQL.
**Formato de Salida (En español):**
**A. Consulta SQL Optimizada:** (Código SQL listo para Databricks)
**B. Justificación de Cambios:** Para cada una de las 6 directrices:
- Cambios aplicados
- Impacto (rendimiento, legibilidad, mantenibilidad)
- Verificación del origen de columnas (punto 2)
- Confirmar equivalencia semántica
---
Select
DISTINCT (A.codsolicitud) AS codsolicitud,
to_timestamp(concat(substr(cast(A.fecsolicitud AS STRING), 1, 10),' ',concat(lpad(cast(floor(A.horsolicitud / 10000) AS STRING), 2, '0'),':',lpad(cast(floor((A.horsolicitud / 100) % 100) AS STRING), 2, '0'),':',lpad(cast(floor(a.horsolicitud % 100) AS STRING), 2, '0')))) AS  Fecha_Registro,
a.destipevaluacionsolicitud as TipoEvaluacion,
"CREDITO CONSUMO" as TipoProducto,
"CEF" as SubProducto,
CASE
WHEN A.codproducto = 'EFME' THEN 'LD'
WHEN A.codproducto = 'EFDP' THEN 'CONVENIO'
WHEN A.codproducto = 'EFCD' THEN 'CD'
ELSE NULL END as Detproducto,
A.destipestadosolicitud as EstadoSolicitud,
A.destipestadoevaluacionanalista AS ResultadoAnalista,
CASE 
WHEN a.destipevaluacionsolicitud = 'EVALUACION AUTOMATICA' and a.destipestadosolicitud ='ACEPTADA' then 'ACEPTADA'
WHEN a.destipevaluacionsolicitud = 'EVALUACION AUTOMATICA' and A.destipestadoevaluacioncda ='Decline' and A.destipestadosolicitud = 'DESACTIVADA' then 'DENEGADA'
WHEN a.destipevaluacionsolicitud = 'EVALUACION AUTOMATICA' and a.destipestadosolicitud ='DESACTIVADA' then 'DESACTIVADA'
WHEN a.destipevaluacionsolicitud = 'EVALUACION AUTOMATICA' and a.destipestadosolicitud ='EN PROCESO' then 'EN PROCESO'
WHEN a.destipevaluacionsolicitud = 'EVALUACION AUTOMATICA' and a.destipestadosolicitud ='SOLICITUD CANCELADA' then 'SOLICITUD CANCELADA'
WHEN a.destipevaluacionsolicitud = 'EVALUACION EN CENTRALIZADO' and A.destipestadoevaluacionanalista = 'Aprobado por Analista de créditos' THEN 'ACEPTADA'
WHEN a.destipevaluacionsolicitud = 'EVALUACION EN CENTRALIZADO' and A.destipestadoevaluacionanalista = 'Aprobado por Gerente - Firmas y Desembolso' THEN 'ACEPTADA'
WHEN a.destipevaluacionsolicitud = 'EVALUACION EN CENTRALIZADO' and A.destipestadoevaluacionanalista = 'Enviado a Analista de créditos' THEN 'ENVIADO'
WHEN a.destipevaluacionsolicitud = 'EVALUACION EN CENTRALIZADO' and A.destipestadoevaluacionanalista = 'Enviado a Gerente - Documentación Adicional' THEN 'ENVIADO'
WHEN a.destipevaluacionsolicitud = 'EVALUACION EN CENTRALIZADO' and A.destipestadoevaluacionanalista = 'Enviado a Gerente - Firmas y Desembolso' THEN 'ENVIADO'
WHEN a.destipevaluacionsolicitud = 'EVALUACION EN CENTRALIZADO' and A.destipestadoevaluacionanalista = 'Rechazado por Gerente - Documentación Adicional' THEN 'DENEGADA'
WHEN a.destipevaluacionsolicitud = 'EVALUACION EN CENTRALIZADO' and A.destipestadoevaluacionanalista = 'Rechazado por Gerente - Firmas y Desembolso' THEN 'DENEGADA'
WHEN a.destipevaluacionsolicitud = 'EVALUACION EN CENTRALIZADO' and A.destipestadoevaluacionanalista = 'Iniciado' THEN 'INICIADO'
ELSE '?????' END as Estado_F,
A.destipestadoevaluacioncda as ResultadoMotorCDA,

null as MotivoCDA,
a.destipetapa AS Etapa,

CASE WHEN a.destipestadojustificacionsolicitud IS NULL THEN a.destipmotivorechazo ELSE a.destipestadojustificacionsolicitud END as justificacion_analista,
A.descomentario as ComentarioAnalista,  
CASE WHEN a.descampania is not null THEN a.descampania ELSE 'SIN CAMPAÑA'end as Campana,
CASE
WHEN contains(a.descampania,'CONVENIO') THEN 'CONVENIO'
WHEN contains(a.descampania,'SHIELD') THEN 'SHIELD'
WHEN a.tipevaluacionriesgo = 'APR' THEN '100% APROBADO'
WHEN a.tipevaluacionriesgo = 'REA' THEN 'SIN CAMPAÑA'
WHEN a.tipevaluacionriesgo= 'PRE' THEN 'PREAPROBADO'
WHEN a.tipevaluacionriesgo = 'INV' THEN 'INVITADO'
WHEN a.tipevaluacionriesgo = 'OPT' THEN 'OPTIMUS'
ELSE NULL END as CampanaCorta2,
A.nbrmoneda as Moneda,
A.mtosolicitado as MontoSolicitado,
A.mtoaprobado as MontoAprobado,
to_timestamp(concat(substr(cast(a.feciniciocalif AS STRING), 1, 10),' ',concat(lpad(cast(floor(a.horiniciocalif / 10000) AS STRING), 2, '0'),':',lpad(cast(floor((a.horiniciocalif / 100) % 100) AS STRING), 2, '0'),':',lpad(cast(floor(a.horiniciocalif % 100) AS STRING), 2, '0')))) AS Inicio_Evaluacion,
to_timestamp(concat(substr(cast(a.fecfincalif AS STRING), 1, 10),' ',concat(lpad(cast(floor(a.horfincalif / 10000) AS STRING), 2, '0'), ':', lpad(cast(floor((a.horfincalif / 100) % 100) AS STRING), 2, '0'), ':', lpad(cast(floor(a.horfincalif % 100) AS STRING), 2, '0') ) )) AS Fin_Evaluacion,
A.fecdesembolso as fecdesembolso,
A.CODMATRICULACOLABORADORANALISTA AS MatriculaAnalista,
A.CODMATRICULACOLABORADORVENDEDOR as MatriculaVendedor,
a.codinternocomputacional as Codclavecic,
CASE WHEN IND.destiprolsolicitud ='TITULAR' THEN IND.flgclipagohaberesbcp ELSE NULL END AS Cliente_PDH,
CASE WHEN IND.destiprolsolicitud ='TITULAR' THEN IND.tiprenta ELSE NULL END AS Tipo_RentaTitular,
CASE WHEN IND2.destiprolsolicitud ='CONYUGE' THEN IND2.tiprenta ELSE NULL END AS Tipo_RentaConyuge,
CASE WHEN IND.destiprolsolicitud ='TITULAR' THEN IND.numpuntajeprecalif ELSE NULL END AS ScoreEvaluacion,
CASE WHEN IND.destiprolsolicitud ='TITULAR' THEN IND.tippartyidentificacion ELSE NULL END AS TipoIdcTitular,
CASE WHEN IND.destiprolsolicitud ='TITULAR' THEN IND.mtoingresodigitado ELSE NULL END AS Ingreso_Bruto_Titular,
CASE WHEN IND.destiprolsolicitud ='TITULAR' THEN IND.mtootroingresodigitado ELSE NULL END AS OtrosIngreso_Bruto_Titular,
CASE WHEN IND.destiprolsolicitud ='TITULAR' THEN IND.mtoingresocalculado ELSE NULL END AS Ingreso_Promedio_Titular,
CASE WHEN IND2.destiprolsolicitud ='CONYUGE' THEN IND2.tippartyidentificacion ELSE NULL END AS TipoIdc_Conyuge,
CASE WHEN IND2.destiprolsolicitud ='CONYUGE' THEN IND2.mtoingresodigitado ELSE NULL END AS Ingreso_Bruto_Conyuge,
CASE WHEN IND2.destiprolsolicitud ='CONYUGE' THEN IND2.mtootroingresodigitado ELSE NULL END AS OtrosIngreso_Bruto_Conyuge,
CASE WHEN IND2.destiprolsolicitud ='CONYUGE' THEN IND2.mtoingresocalculado ELSE NULL END AS Ingreso_Promedio_Conyuge,
a.mtocuotamensualaprobado as CuotamensualAprobado,
a.ctdplazosolicitado as PlazoSolicitado,
A.ctdplazoaprobado as PlazoAprobado,
a.pcttasaefectivaanualsolicitud as TasaSolicitada,
a.pcttasaefectivaanualaprobada as TasaAprobada,
IND.mtocem as CEM,
IND.mtocemasesorventa as CEM_Vendedor,
IND.mtocemanalistacredito as CEM_Analista,
CASE WHEN IND.destiprolsolicitud ='TITULAR' THEN IND.dessubsegmento ELSE NULL END AS SegmentoTitular,
CASE WHEN IND.destiprolsolicitud ='TITULAR' THEN IND.mtodeudatotalbcp ELSE NULL END AS Posicion_Consolidada,
CASE WHEN IND.destiprolsolicitud ='TITULAR' THEN IND.codsegmentoriesgoscore ELSE NULL END AS SegmentoRiesgosTitular,
CASE WHEN IND2.destiprolsolicitud ='CONYUGE' THEN IND2.codsegmentoriesgoscore ELSE NULL END AS SegmentoRiesgosConyuge,
CASE WHEN a.fecdesembolso IS NOT NULL THEN a.mtoaprobado ELSE NULL END as mto_desembolsado,
CASE WHEN IND.destiprolsolicitud ='TITULAR' THEN IND.destipniveleducacional ELSE NULL END AS Grado_Instruccion,
CASE WHEN IND.destiprolsolicitud ='TITULAR' THEN IND.destipocupacion ELSE NULL END AS Situacion_LaboralTitular,
CASE WHEN IND2.destiprolsolicitud ='CONYUGE' THEN IND2.destipocupacion ELSE NULL END AS Situacion_LaboralConyuge,
CASE WHEN PTY.destiprolsolicitud ='TITULAR' THEN PTY.destipclasifriesgocli ELSE NULL END AS Clasificacion_SbsTitular, 
CASE WHEN PTY2.destiprolsolicitud ='CONYUGE' THEN PTY2.destipclasifriesgocli ELSE NULL END AS Clasificacion_SbsConyuge, 

CASE WHEN IND.destiprolsolicitud ='TITULAR' THEN IND.numdependiente ELSE NULL END AS N_DependientesTitular,
CASE WHEN IND.destiprolsolicitud ='TITULAR' THEN IND.destipestcivil ELSE NULL END AS EstadoCivil,
P1.DESPOSICIONCOLABORADOR as FuncionVendedor,
A.mtoingresobrutosolicitud as IngresoVerificado,
day(a.fecsolicitud) as DiaSolicitud,
month(a.fecsolicitud) as MesSolicitud,
year(a.fecsolicitud) as AnoSolicitud,
date_format(a.fecsolicitud,"yyyyMM")as AnoMesSolicitud,
CASE 
when IND.numpuntajeprecalif <=0 then 'Sin data' 
when IND.numpuntajeprecalif <=300 then 'A. 0-300' 
when IND.numpuntajeprecalif <=330 then 'B. 300-330' 
when IND.numpuntajeprecalif <=360 then 'C. 330-360' 
when IND.numpuntajeprecalif <=390 then 'D. 360-390' 
when IND.numpuntajeprecalif <=420 then 'E. 390-420' 
when IND.numpuntajeprecalif <=450 then 'F. 42-450' 
ELSE NULL END as Rango_Score,
CASE 
WHEN A.nbrmoneda = "DOLAR AMERICANO             EE.UU       " THEN A.mtosolicitado * 3.81
ELSE A.mtosolicitado END as MontoSolicitado_Soles,
CASE 
WHEN A.nbrmoneda = "DOLAR AMERICANO             EE.UU       "  THEN mtoaprobado * 3.81 --Estado_F="ACEPTADA"
ELSE mtoaprobado END as MontoAprobado_Soles,
CASE 
WHEN A.nbrmoneda = "DOLAR AMERICANO             EE.UU       "  AND Estado_F="ACEPTADA" THEN mto_desembolsado * 3.81
ELSE mto_desembolsado END as MontoDesembolsado_Soles,
CASE 
WHEN MontoDesembolsado_Soles>0 THEN "Si"
ELSE "No" END as Flag_MontoDesembolsado_Soles,
CASE 
when MontoSolicitado_Soles <=0 then 'Sin data' 
when MontoSolicitado_Soles <=23000 then 'A. 0-23k' 
when MontoSolicitado_Soles <=46000 then 'B. 23k-46k' 
when MontoSolicitado_Soles <=69000 then 'C. 46k-69k' 
when MontoSolicitado_Soles <=92000 then 'D. 69k-92k' 
when MontoSolicitado_Soles <=115000 then 'E. 92k-115k' 
when MontoSolicitado_Soles <=138000 then 'F. 115k-138k' 
when MontoSolicitado_Soles <=160000 then 'G. 138k-160k' 
ELSE "H. >160k" END as Rango_MontoSolicitado_Soles,
date_diff(second,Fecha_Registro,Fin_Evaluacion)/3600 as TiempoCliente_Hrs,
date_diff(second,Inicio_Evaluacion,Fin_Evaluacion)/3600 as TiempoAnalista_Hrs,
date_diff(second,Fecha_Registro,Fin_Evaluacion) as TotalSegundo_TiempoCliente,
date_diff(second,Inicio_Evaluacion,Fin_Evaluacion) as TotalSegundo_TiempoAnalista,
Case 
when TiempoCliente_Hrs is null then null 
when TiempoCliente_Hrs <= 24 then 'Dentro 24hrs' 
else 'Mayor 24hrs' end as Atencion24HrsCliente,
Case 
when TiempoCliente_Hrs is null then null 
when TiempoCliente_Hrs <= 12 then 'Dentro 12hrs' 
when TiempoCliente_Hrs <= 24 then 'Dentro 24hrs' 
else 'Mayor 24hrs' end as Atencion12HrsCliente,
--
CASE 
when TiempoCliente_Hrs is null then null
when TiempoCliente_Hrs <=0 then 'Sin data' 
when TiempoCliente_Hrs <=3 then 'A. 0-3' 
when TiempoCliente_Hrs <=6 then 'B. 3-6' 
when TiempoCliente_Hrs <=12 then 'C. 6-12' 
when TiempoCliente_Hrs <=18 then 'D. 12-18' 
when TiempoCliente_Hrs <=24 then 'E. 18-24' 
ELSE "F. >24" END as RangoAtencionCliente

from catalog_lhcl_prod_bcp.bcp_udv_int_v.m_solicitudcreditoconsumo A

LEFT JOIN catalog_lhcl_prod_bcp.bcp_udv_int_v.M_USUARIOAPLICATIVO P1
ON A.codcolaboradorvendedor = P1.CODUSUARIOAPP

LEFT JOIN bcp_udv_int_v.m_solicitudindividuo IND
on A.codsolicitud = IND.codsolicitud
and IND.destiprolsolicitud ='TITULAR'

LEFT JOIN bcp_udv_int_v.m_solicitudindividuo IND2
on A.codsolicitud = IND2.codsolicitud
and IND2.destiprolsolicitud ='CONYUGE'

LEFT JOIN bcp_udv_int_v.m_solicitudparty PTY
on A.codsolicitud = PTY.codsolicitud
and PTY.destiprolsolicitud ='TITULAR'

LEFT JOIN bcp_udv_int_v.m_solicitudparty PTY2
on A.codsolicitud = PTY2.codsolicitud
and PTY2.destiprolsolicitud ='CONYUGE'

where a.fecsolicitud >= '2025-10-01'
