--NUEVO TC
Select 
DISTINCT (A.codsolicitud) AS codsolicitud,
--A.tipclasifinternasolicitud AS tipclasifinternasolicitud,
--A.destipclasifinternasolicitud AS destipclasifinternasolicitud,
to_timestamp(concat(substr(cast(A.fecsolicitud AS STRING), 1, 10),' ',concat(lpad(cast(floor(A.horsolicitud / 10000) AS STRING), 2, '0'),':',lpad(cast(floor((A.horsolicitud / 100) % 100) AS STRING), 2, '0'),':',lpad(cast(floor(a.horsolicitud % 100) AS STRING), 2, '0') ) )) AS  Fecha_Registro,
--A.tipevaluacionsolicitud, - TipoEvalaucion
CASE
WHEN A.codmatriculacolaboradoraprobador is not null THEN 'EVALUACION EN CENTRALIZADO'
ELSE 'EVALUACION AUTOMATICA' END as TipoEvaluacion,
CASE
WHEN A.destipflujoventatarjetacredito = 'EP' THEN 'TARJETA CREDITO STOCK'
WHEN A.destipflujoventatarjetacredito = 'SEGUNDA TC' THEN 'TARJETA CREDITO NUEVA'
WHEN A.destipflujoventatarjetacredito = 'TC' THEN 'TARJETA CREDITO NUEVA'
WHEN A.destipflujoventatarjetacredito = 'PTC' THEN 'TARJETA CREDITO STOCK'
WHEN A.destipflujoventatarjetacredito = 'BT' THEN 'TARJETA CREDITO STOCK'
WHEN A.destipflujoventatarjetacredito = 'AMPLIACION' THEN 'TARJETA CREDITO STOCK'
WHEN A.destipflujoventatarjetacredito = 'UPGRADE' THEN 'TARJETA CREDITO STOCK'
WHEN A.destipflujoventatarjetacredito = 'ADICIONAL' THEN 'TARJETA CREDITO STOCK'
WHEN contains(A.descampania,'PRESTAMO TAR') THEN 'TARJETA CREDITO STOCK'
WHEN contains(A.descampania,'PRÉSTAMO TAR') THEN 'TARJETA CREDITO STOCK'
WHEN contains(A.descampania,'PTC') THEN 'TARJETA CREDITO STOCK'
WHEN contains(A.descampania,'EP') THEN 'TARJETA CREDITO STOCK'
WHEN contains(A.descampania,'BALANCE TRA') THEN 'TARJETA CREDITO STOCK'
WHEN contains(A.descampania,'TC+BT') THEN 'TARJETA CREDITO STOCK'
WHEN contains(A.descampania,'BT') THEN 'TARJETA CREDITO STOCK'
WHEN contains(A.descampania,'AMPLIACION') THEN 'TARJETA CREDITO STOCK'
WHEN contains(A.descampania,'AMPLIACIÓN') THEN 'TARJETA CREDITO STOCK'
WHEN contains(A.descampania,'UPGRADE') THEN 'TARJETA CREDITO STOCK'
WHEN contains(A.descampania,'UPG') THEN 'TARJETA CREDITO STOCK'
WHEN contains(A.descampania,'ADICIONAL') THEN 'TARJETA CREDITO STOCK'
ELSE 'Sin data' END as TipoProducto,
--A.destipflujoventatarjetacredito AS TipoVariante,
CASE
WHEN A.destipflujoventatarjetacredito = 'EP' THEN 'PTC'
WHEN A.destipflujoventatarjetacredito = 'SEGUNDA TC' THEN 'TC'
WHEN A.destipflujoventatarjetacredito IS NOT NULL THEN A.destipflujoventatarjetacredito
WHEN contains(A.descampania,'PRESTAMO TAR') THEN 'PTC'
WHEN contains(A.descampania,'PRÉSTAMO TAR') THEN 'PTC'
WHEN contains(A.descampania,'PTC') THEN 'PTC'
WHEN contains(A.descampania,'EP') THEN 'PTC'
WHEN contains(A.descampania,'BALANCE TRA') THEN 'BT'
WHEN contains(A.descampania,'TC+BT') THEN 'BT'
WHEN contains(A.descampania,'BT') THEN 'BT'
WHEN contains(A.descampania,'AMPLIACION') THEN 'AMPLIACION'
WHEN contains(A.descampania,'AMPLIACIÓN') THEN 'AMPLIACION'
WHEN contains(A.descampania,'UPGRADE') THEN 'UPGRADE'
WHEN contains(A.descampania,'UPG') THEN 'UPGRADE'
WHEN contains(A.descampania,'ADICIONAL') THEN 'ADICIONAL'
ELSE 'Sin data' END as SubProducto,
--A.codproducto AS Codproducto,
A.desproducto AS Detproducto,
--A.destipetapa as destipestadosolicitud,
--A.tipestadosolicitud as tipestadosolicitud,
CASE
WHEN A.tipestadosolicitud ='ACPD' THEN 'ACEPTADA'
WHEN A.tipestadosolicitud ='DCLD' THEN 'DENEGADA'
WHEN A.tipestadosolicitud ='ENPR' THEN 'EN PROCESO'
WHEN A.tipestadosolicitud ='IRFC' THEN 'INGRESADO'
WHEN A.tipestadosolicitud ='EEEL' THEN 'EN PROCESO DE CALIFICACION'
WHEN A.tipestadosolicitud ='DIGI' THEN 'DIGITADO'
ELSE 'Sin data' END as EstadoSolicitud,
EstadoSolicitud AS ResultadoAnalista,
CASE
WHEN A.tipestadosolicitud ='ACPD' and A.destipetapa = 'CERRADA' THEN 'ACEPTADA'
WHEN A.tipestadosolicitud ='DCLD' and A.destipetapa = 'RECHAZADA' THEN 'DENEGADA'
WHEN A.tipestadosolicitud ='DCLD' and A.destipetapa = 'DESESTIMADA' THEN 'DESESTIMADA'
WHEN A.tipestadosolicitud ='DCLD' and A.destipetapa = 'PERDIDA' THEN 'DESESTIMADA'
WHEN A.tipestadosolicitud ='ENPR' THEN 'EN PROCESO'
WHEN A.tipestadosolicitud ='IRFC' THEN 'INGRESADO'
WHEN A.tipestadosolicitud ='EEEL' THEN 'EN PROCESO DE CALIFICACION'
WHEN A.tipestadosolicitud ='DIGI' THEN 'DIGITADO'
else 'SIn data'end AS Estado_F,

--B.bcp_cdaresponse__c AS Resultado_CDA, --falta migrar a Udv
/*CASE 
when B.bcp_cdaresponse__c = 'Approve' then 'Aceptada' 
when B.bcp_cdaresponse__c = 'Investigate' then 'En Centralizado' 
when B.bcp_cdaresponse__c = 'Decline' then 'Denegado' 
when B.bcp_cdaresponse__c = 'Contingency' then 'Contingencia' 
when B.bcp_cdaresponse__c = 'No Decision' then 'No Decision' 
when B.bcp_cdaresponse__c = 'Decline_exception' then 'Denegado' 
when B.bcp_cdaresponse__c = 'Massive Contingency' then 'Contingencia' 
ELSE B.bcp_cdaresponse__c END as ResultadoMotorCDA,*/
null as ResultadoMotorCDA,  --no exite en TC

--XX.motivo_submotivos,   ---------NUEVO
A.desmotivocda as MotivoCDA,
--A.tipetapa as tipetapa,
A.destipetapa as Etapa,
--
/*Case 
when b.bcp_acceptancereason__c is not null then b.bcp_acceptancereason__c
when A.destipetapa in ('DESESTIMADA','CERRADA','RECHAZADA') then b.bcp_rejectedreason__c
ELSE null END as justificacion_analista, */                                                 --NO HAY EN UDV
null as justificacion_analista,
A.descomentario AS ComentarioAnalista,
--A.desiniciocomentarioevaluacion,
--A.desfincomentarioevaluacion,
--B.BCP_ACCEPTANCEREASON__C --MOTIVO ACEPTACION
/*case when A.destipetapa='CERRADA' then b.bcp_acceptancereason__c
--when  B.bcp_cdaresponse__c in ('Desestimada', 'Rechazada', 'Perdida') then b.bcp_rejectedreason__c ELSE NULL END as destipestadojustificacionsolicitud,  */
--
--A.destipevaluacionsolicitud, --Campaña
A.descampania AS Campana,
CASE
WHEN contains(A.descampania,'100%') THEN '100% APROBADO'
WHEN contains(A.descampania,'PREAPR') THEN 'PREAPROBADO'
WHEN contains(A.descampania,'SIN CAMP') THEN 'SIN CAMPAÑA'
WHEN contains(A.descampania,'INVITA') THEN 'INVITADO'
WHEN A.destipflujoventatarjetacredito = 'EP' THEN 'PTC'
WHEN A.destipflujoventatarjetacredito = 'SEGUNDA TC' THEN 'TC'
WHEN A.destipflujoventatarjetacredito IS NOT NULL THEN A.destipflujoventatarjetacredito
WHEN contains(A.descampania,'PRESTAMO TAR') THEN 'PTC'
WHEN contains(A.descampania,'PRÉSTAMO TAR') THEN 'PTC'
WHEN contains(A.descampania,'PTC') THEN 'PTC'
WHEN contains(A.descampania,'EP') THEN 'PTC'
WHEN contains(A.descampania,'BALANCE TRA') THEN 'BT'
WHEN contains(A.descampania,'TC+BT') THEN 'BT'
WHEN contains(A.descampania,'BT') THEN 'BT'
WHEN contains(A.descampania,'AMPLIACION') THEN 'AMPLIACION'
WHEN contains(A.descampania,'AMPLIACIÓN') THEN 'AMPLIACION'
WHEN contains(A.descampania,'UPGRADE') THEN 'UPGRADE'
WHEN contains(A.descampania,'UPG') THEN 'UPGRADE'
WHEN contains(A.descampania,'ADICIONAL') THEN 'ADICIONAL'
ELSE 'Sin data' END as CampanaCorta2,
A.nbrmoneda AS Moneda,
A.mtosolicitado AS MontoSolicitado,
A.mtoaprobado AS MontoAprobado,
to_timestamp(concat(substr(cast(A.fecinicioevaluacion AS STRING), 1, 10),' ',concat(lpad(cast(floor(A.horinicioevaluacion / 10000) AS STRING), 2, '0'),':',lpad(cast(floor((A.horinicioevaluacion / 100) % 100) AS STRING), 2, '0'),':',lpad(cast(floor(a.horinicioevaluacion % 100) AS STRING), 2, '0')))) AS  Inicio_Evaluacion,
to_timestamp(concat(substr(cast(A.fecfinevaluacion AS STRING), 1, 10),' ', concat(lpad(cast(floor(A.horfinevaluacion / 10000) AS STRING), 2, '0'), ':', lpad(cast(floor((A.horfinevaluacion / 100) % 100) AS STRING), 2, '0'),':',lpad(cast(floor(a.horfinevaluacion % 100) AS STRING), 2, '0')))) AS  Fin_Evaluacion,
--case when B.stagename='Cerrada' THEN B.laststagechangedate ELSE NULL END AS fecdesembolso,
A.fecemisiontarjetacredito AS fecdesembolso,                                       --Esta es fec emision tarjetacredito
--case when A.destipetapa='Cerrada' THEN A.fecemisiontarjetacredito ELSE NULL END AS fecemision,
--to_timestamp(cast(regexp_replace(concat(substr(cast(A.fecultmodificacion as string),1,10),A.horultmodificacion), '(\\d{4})-(\\d{2})-(\\d{2})(\\d{2})(\\d{2})(\\d{2})', '$1-$2-$3 $4:$5:$6') as timestamp)) AS fechorultmodificacion,
--A.fecultmodificacion as fecha_cierre_evaluacion,
--case when A.destipetapa='CERRADA' THEN 'S' ELSE 'N' END as flgdesembolsosolicitud,
--
A.codmatriculacolaboradoraprobador AS MatriculaAnalista,
A.codmatriculacolaboradorvendedor AS MatriculaVendedor,
A.codinternocomputacional AS Codclavecic,
--A.codmatriculagerenteareacolaboradoraprobador,
--
CASE WHEN IND.destiprolsolicitud ='TITULAR' THEN IND.flgclipagohaberesbcp ELSE NULL END AS Cliente_PDH, --tabla m_solicitudindividuo trae vacio
--CASE WHEN A1.bcp_persontype__c='T' THEN A1.bcp_pdhclient__c ELSE NULL END AS Cliente_PDH, -- FALTA MIGRAR A UDV
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
null as PlazoSolicitado,
null as PlazoAprobado,
A.pcttasaefectivaanualsolicitud AS TasaSolicitada,
null as TasaAprobada,
--A.tipsolicitud AS tipsolicitud,
--A.destipsolicitud AS destipsolicitud,
--A.codapp AS codapp,
IND.mtocem as CEM,
IND.mtocemasesorventa as CEM_Vendedor,
IND.mtocemanalistacredito as CEM_Analista,
CASE WHEN IND.destiprolsolicitud ='TITULAR' THEN IND.dessubsegmento ELSE NULL END AS SegmentoTitular,
null as Posicion_Consolidada,
--CASE WHEN A1.bcp_persontype__c='T'THEN A1.BCP_calculatedDebtAmount__c ELSE NULL END AS Monto_CEM, --Posicion_Consolidada
CASE WHEN IND.destiprolsolicitud ='TITULAR' THEN IND.codsegmentoriesgoscore ELSE NULL END AS SegmentoRiesgosTitular,
CASE WHEN IND2.destiprolsolicitud ='CONYUGE' THEN IND2.mtoingresocalculado ELSE NULL END AS SegmentoRiesgosConyuge,
null as mto_desembolsado,
CASE WHEN IND.destiprolsolicitud ='TITULAR' THEN IND.destipniveleducacional ELSE NULL END AS Grado_Instruccion,
CASE WHEN IND.destiprolsolicitud ='TITULAR' THEN IND.destipocupacion ELSE NULL END AS Situacion_LaboralTitular,
CASE WHEN IND2.destiprolsolicitud ='CONYUGE' THEN IND2.destipocupacion ELSE NULL END AS Situacion_LaboralConyuge,
--CASE WHEN A1.bcp_persontype__c='T' THEN A1.BCP_SBSRISKCLASSIFICATION__C ELSE NULL END AS Clasificacion_SbsTitular, --Falta migrar a Udv
--CASE WHEN A2.bcp_persontype__c='C' then A2.BCP_SBSRISKCLASSIFICATION__C ELSE NULL END AS Clasificacion_SbsConyuge, --Falta migrar a Udv
CASE WHEN PTY.destiprolsolicitud ='TITULAR' THEN PTY.destipclasifriesgocli ELSE NULL END AS Clasificacion_SbsTitular, 
CASE WHEN PTY2.destiprolsolicitud ='CONYUGE' THEN PTY2.destipclasifriesgocli ELSE NULL END AS Clasificacion_SbsConyuge, 

CASE WHEN IND.destiprolsolicitud ='TITULAR' THEN IND.numdependiente ELSE NULL END AS N_DependientesTitular,
CASE WHEN IND.destiprolsolicitud ='TITULAR' THEN IND.destipestcivil ELSE NULL END AS EstadoCivil,
P1.DESPOSICIONCOLABORADOR as FuncionVendedor,
--A.destipregapp,
--b.BCP_VerifiedRevenue__c as IngresoVerificado,
A.mtoingresobrutosolicitud as IngresoVerificado,

day(A.fecsolicitud) as DiaSolicitud,
month(A.fecsolicitud) as MesSolicitud,
year(A.fecsolicitud) as AnoSolicitud,
date_format(A.fecsolicitud,"yyyyMM")as AnoMesSolicitud,
--concat(AnoSolicitud,MesSolicitud) as AnoMesSolicitud,
CASE 
when IND.numpuntajeprecalif <=0 then 'Sin data' 
when IND.numpuntajeprecalif <=300 then 'A. 0-300' 
when IND.numpuntajeprecalif <=330 then 'B. 300-330' 
when IND.numpuntajeprecalif <=360 then 'C. 330-360' 
when IND.numpuntajeprecalif <=390 then 'D. 360-390' 
when IND.numpuntajeprecalif <=420 then 'E. 390-420' 
when IND.numpuntajeprecalif <=450 then 'F. 42-450' 
ELSE NULL END as Rango_Score,

--
CASE 
WHEN A.nbrmoneda = "DOLAR AMERICANO             EE.UU       " THEN A.mtosolicitado * 3.81
ELSE A.mtosolicitado END as MontoSolicitado_Soles,
--
CASE 
WHEN A.nbrmoneda = "DOLAR AMERICANO             EE.UU       "  THEN mtoaprobado * 3.81 --Estado_F="ACEPTADA"
ELSE mtoaprobado END as MontoAprobado_Soles,
--
null as MontoDesembolsado_Soles,
--
null as Flag_MontoDesembolsado_Soles,
--
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
--
date_diff(second,Fecha_Registro,Fin_Evaluacion)/3600 as TiempoCliente_Hrs,
date_diff(second,Inicio_Evaluacion,Fin_Evaluacion)/3600 as TiempoAnalista_Hrs,
date_diff(second,Fecha_Registro,Fin_Evaluacion) as TotalSegundo_TiempoCliente,
date_diff(second,Inicio_Evaluacion,Fin_Evaluacion) as TotalSegundo_TiempoAnalista,
--Case when TiempoCliente_Hrs < 24 then 'Dentro 24hrs' else 'Mayor 24hrs' end as Atencion24HrsCliente,
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
--A.desmotivoaprocda --Justificacion _ bcp_acceptancereason__c
--b.bcp_acceptancereason__c,
--bcp_rejectedreason__c 


----
from catalog_lhcl_prod_bcp.bcp_udv_int_v.m_solicitudtarjetacredito A
--LEFT JOIN bcp_udv_int_v.m_subsegmento D               
--on A1.BCP_CustomerSegment__c = D.codexternosegmento   

LEFT JOIN catalog_lhcl_prod_bcp.bcp_udv_int_v.M_USUARIOAPLICATIVO P1
ON A.codcolaboradorvendedor = P1.CODUSUARIOAPP
-------------
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
/*-----------
--------------------NUEVO motivo_submotivos
LEFT JOIN (with table_motivos as (
select
bcp_opportunity__c as codsolicitud,
--name as motivo,
name ||'-'||bcp_modulestatus__c AS motivo,
bcp_rulesdetail__c as submotivo
 from (select*,
row_number() over(partition by bcp_opportunity__c, name order by fecrutinahost desc) rn
from bcp_rdv_crmo_v.scrm_evaluation_v
WHERE bcp_modulestatus__c not like '%APROBADO%')
 
where rn = 1)
 
select
  codsolicitud,
  collect_list(
    named_struct('motivos', motivo, 'submotivo', submotivos_array)
 
) as motivo_submotivos
from (
  select
    codsolicitud,
    motivo,
    collect_list(submotivo) as submotivos_array
    from table_motivos
    group by codsolicitud, motivo
) as motivos_con_submotivos
group by codsolicitud ) XX
 
ON XX.CODSOLICITUD = B.ID
-----------
-----------*/

WHERE A.codtipregapp IN ('0126O000001xsIYQAY','0126O000001h0WaQAI','0126O000001h0WbQAI')
AND A.fecsolicitud >='2025-10-01'

--AND A.fecsolicitud between '2025-09-21' and '2025-09-28'
--and A.codsolicitud ='O0015633951'
--and destipevaluacionsolicitud = 'EVALUACION EN CENTRALIZADO'
--AND A.codsolicitud IN ('O0008469302','O0008880201','O0008571263','O0008880349')
/*WHERE A.codsolicitud in ('O0016398772',
'O0016394223',
'O0016946084',
'O0016944189',
'O0016938957' )

*/



















--NUEVO CEF
Select
DISTINCT (A.codsolicitud) AS codsolicitud,
--A.tipclasifinternasolicitud,
--A.destipclasifinternasolicitud,
to_timestamp(concat(substr(cast(A.fecsolicitud AS STRING), 1, 10),' ',concat(lpad(cast(floor(A.horsolicitud / 10000) AS STRING), 2, '0'),':',lpad(cast(floor((A.horsolicitud / 100) % 100) AS STRING), 2, '0'),':',lpad(cast(floor(a.horsolicitud % 100) AS STRING), 2, '0')))) AS  Fecha_Registro,
a.destipevaluacionsolicitud as TipoEvaluacion,
"CREDITO CONSUMO" as TipoProducto,
"CEF" as SubProducto,
--A.codproducto, 
CASE
WHEN A.codproducto = 'EFME' THEN 'LD'
WHEN A.codproducto = 'EFDP' THEN 'CONVENIO'
WHEN A.codproducto = 'EFCD' THEN 'CD'
ELSE NULL END as Detproducto,
--a.desproducto,
A.destipestadosolicitud as EstadoSolicitud,
--B.bcp_approvalstatus__c AS ResultadoAnalista,  --falta migrar a Udv
/*CASE
WHEN B.bcp_approvalstatus__c = 'Aprobado por Analista de créditos' THEN 'ACEPTADA'                      --ACEPTADA --RDV
WHEN B.bcp_approvalstatus__c = 'Aprobado por Gerente - Firmas y Desembolso' THEN 'ACEPTADA'             --ACEPTADA
WHEN B.bcp_approvalstatus__c = 'Enviado a Analista de créditos' THEN 'ENVIADO AL ANALISTA'              --DENEGADA
WHEN B.bcp_approvalstatus__c = 'Enviado a Gerente - Documentación Adicional' THEN 'ENVIADO AL GERENTE'  --DENEGADA
WHEN B.bcp_approvalstatus__c = 'Enviado a Gerente - Firmas y Desembolso' THEN 'ENVIADO AL GERENTE'      --DENEGADA
WHEN B.bcp_approvalstatus__c = 'Rechazado por Gerente - Documentación Adicional' THEN 'DENEGADA'        --DENEGADA
WHEN B.bcp_approvalstatus__c = 'Rechazado por Gerente - Firmas y Desembolso' THEN 'DENEGADA'            --DENEGADA
WHEN B.bcp_approvalstatus__c = 'Iniciado' THEN 'INICIADO'                                               --DENEGADA
ELSE NULL END as ResultadoAnalista,*/
A.destipestadoevaluacionanalista AS ResultadoAnalista, --UDV PORNER CASE
CASE 
WHEN a.destipevaluacionsolicitud = 'EVALUACION AUTOMATICA' and a.destipestadosolicitud ='ACEPTADA' then 'ACEPTADA'
WHEN a.destipevaluacionsolicitud = 'EVALUACION AUTOMATICA' and A.destipestadoevaluacioncda ='Decline' and A.destipestadosolicitud = 'DESACTIVADA' then 'DENEGADA'
WHEN a.destipevaluacionsolicitud = 'EVALUACION AUTOMATICA' and a.destipestadosolicitud ='DESACTIVADA' then 'DESACTIVADA'
WHEN a.destipevaluacionsolicitud = 'EVALUACION AUTOMATICA' and a.destipestadosolicitud ='EN PROCESO' then 'EN PROCESO'
WHEN a.destipevaluacionsolicitud = 'EVALUACION AUTOMATICA' and a.destipestadosolicitud ='SOLICITUD CANCELADA' then 'SOLICITUD CANCELADA'
--WHEN a.destipevaluacionsolicitud = 'EVALUACION AUTOMATICA' and a.destipestadosolicitud ='DENEGADA' then 'DENEGADA'
--WHEN a.destipevaluacionsolicitud = 'EVALUACION AUTOMATICA' and a.destipestadosolicitud ='ENVIADO AL CENTRALIZADO' then 'ENVIADO AL CENTRALIZADO'
WHEN a.destipevaluacionsolicitud = 'EVALUACION EN CENTRALIZADO' and A.destipestadoevaluacionanalista = 'Aprobado por Analista de créditos' THEN 'ACEPTADA'
WHEN a.destipevaluacionsolicitud = 'EVALUACION EN CENTRALIZADO' and A.destipestadoevaluacionanalista = 'Aprobado por Gerente - Firmas y Desembolso' THEN 'ACEPTADA'
WHEN a.destipevaluacionsolicitud = 'EVALUACION EN CENTRALIZADO' and A.destipestadoevaluacionanalista = 'Enviado a Analista de créditos' THEN 'ENVIADO'
WHEN a.destipevaluacionsolicitud = 'EVALUACION EN CENTRALIZADO' and A.destipestadoevaluacionanalista = 'Enviado a Gerente - Documentación Adicional' THEN 'ENVIADO'
WHEN a.destipevaluacionsolicitud = 'EVALUACION EN CENTRALIZADO' and A.destipestadoevaluacionanalista = 'Enviado a Gerente - Firmas y Desembolso' THEN 'ENVIADO'
WHEN a.destipevaluacionsolicitud = 'EVALUACION EN CENTRALIZADO' and A.destipestadoevaluacionanalista = 'Rechazado por Gerente - Documentación Adicional' THEN 'DENEGADA'
WHEN a.destipevaluacionsolicitud = 'EVALUACION EN CENTRALIZADO' and A.destipestadoevaluacionanalista = 'Rechazado por Gerente - Firmas y Desembolso' THEN 'DENEGADA'
WHEN a.destipevaluacionsolicitud = 'EVALUACION EN CENTRALIZADO' and A.destipestadoevaluacionanalista = 'Iniciado' THEN 'INICIADO'
ELSE '?????' END as Estado_F,       --revisar ???
--B.bcp_cdaresponse__c AS ResultadoCDA,  --falta migrar a Udv
/*CASE 
when B.bcp_cdaresponse__c = 'Approve' then 'Aceptada' 
when B.bcp_cdaresponse__c = 'Investigate' then 'En Centralizado' 
when B.bcp_cdaresponse__c = 'Decline' then 'Denegado' 
when B.bcp_cdaresponse__c = 'Contingency' then 'Contingencia' 
when B.bcp_cdaresponse__c = 'No Decision' then 'No Decision' 
when B.bcp_cdaresponse__c = 'Decline_exception' then 'Denegado' 
when B.bcp_cdaresponse__c = 'Massive Contingency' then 'Contingencia' 
ELSE B.bcp_cdaresponse__c END as ResultadoMotorCDA,*/
A.destipestadoevaluacioncda as ResultadoMotorCDA,  --UDV PORNER CASE

null as MotivoCDA,  --se solicito migracion a Udv, validar en tabla Udv
a.destipetapa AS Etapa,            --etapa
--
--A.destipestadojustificacionsolicitud,
--A.destipmotivorechazo,
CASE WHEN a.destipestadojustificacionsolicitud IS NULL THEN a.destipmotivorechazo ELSE a.destipestadojustificacionsolicitud END as justificacion_analista,
--b.BCP_CONFIDENTIALCOMMENT__C as ComentarioAnalista,   --falta migrar a Udv
A.descomentario as ComentarioAnalista,  
--
--a.descampania,            --Campana
--a.destipevaluacionriesgo, --TipoCampana
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
/*CASE
WHEN contains(a.descampania,'CONVENIO') THEN 'CONVENIO'
WHEN contains(a.descampania,'SUSTENTO DE INGRESOS') THEN 'INVITADO'
WHEN contains(a.descampania,'SHIELD') THEN 'SHIELD'
WHEN CampanaCorta = 'SIN CAMPAÑA' THEN 'SIN CAMPAÑA'
WHEN contains(a.descampania,'LOOK ALIKE') THEN 'LOOK ALIKE'
WHEN contains(a.descampania,'ABANDONO') THEN 'ABANDONO'
WHEN contains(a.descampania,'Cliente Alto Valor') THEN 'Cliente Alto Valor'
WHEN contains(a.descampania,'Nueva Oferta') THEN 'Nueva Oferta'
WHEN contains(a.descampania,'Oferta Mejorada') THEN 'Oferta Mejorada'
WHEN contains(a.descampania,'Nueva Oferta') THEN 'Nueva Oferta'
WHEN contains(a.descampania,'TOP INGRESOS') THEN 'TOP INGRESOS'
WHEN contains(a.descampania,'FRONTERA') THEN 'FRONTERA'
WHEN contains(a.descampania,'Oferta mejorada por tasa') THEN 'Oferta mejorada por tasa'
WHEN contains(a.descampania,'Lead Nuevo') THEN 'Lead Nuevo'
WHEN contains(a.descampania,'Sueldo VIP') THEN 'Sueldo VIP'
ELSE '???' END as CampanaCorta3,*/
A.nbrmoneda as Moneda,
A.mtosolicitado as MontoSolicitado,
A.mtoaprobado as MontoAprobado,
to_timestamp(concat(substr(cast(a.feciniciocalif AS STRING), 1, 10),' ',concat(lpad(cast(floor(a.horiniciocalif / 10000) AS STRING), 2, '0'),':',lpad(cast(floor((a.horiniciocalif / 100) % 100) AS STRING), 2, '0'),':',lpad(cast(floor(a.horiniciocalif % 100) AS STRING), 2, '0')))) AS Inicio_Evaluacion,
to_timestamp(concat(substr(cast(a.fecfincalif AS STRING), 1, 10),' ',concat(lpad(cast(floor(a.horfincalif / 10000) AS STRING), 2, '0'), ':', lpad(cast(floor((a.horfincalif / 100) % 100) AS STRING), 2, '0'), ':', lpad(cast(floor(a.horfincalif % 100) AS STRING), 2, '0') ) )) AS Fin_Evaluacion,
A.fecdesembolso as fecdesembolso,
--
A.CODMATRICULACOLABORADORANALISTA AS MatriculaAnalista,
--A.CODCOLABORADORANALISTA AS CodAnalista,
---A.CODMATRICULACOLABORADORAPROBADOR AS CodAnalistaAprobador,
--A.destipsolicitud,
---A.CODMATRICULACOLABORADOREXCEPTUADOR codmatriculaexceptuador,
--A.CODCOLABORADOREXCEPTUADOR codexceptuador,
A.CODMATRICULACOLABORADORVENDEDOR as MatriculaVendedor,
--b.BCP_BusinessManagerNumber__c as Mat_Manager,
a.codinternocomputacional as Codclavecic,
--A.nbrtipexcep,pdh
--A.destipventacda,
--
CASE WHEN IND.destiprolsolicitud ='TITULAR' THEN IND.flgclipagohaberesbcp ELSE NULL END AS Cliente_PDH, --tabla m_solicitudindividuo trae vacio
--CASE WHEN C.bcp_persontype__c='T' THEN C.bcp_pdhclient__c ELSE NULL END AS Cliente_PDH,
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
--b.bcp_potencialcampaignamount__c as MontoCampana,  --falta migrar a Udv
a.ctdplazosolicitado as PlazoSolicitado,
A.ctdplazoaprobado as PlazoAprobado,
a.pcttasaefectivaanualsolicitud as TasaSolicitada,
a.pcttasaefectivaanualaprobada as TasaAprobada,
IND.mtocem as CEM,
--b.bcp_digitedcem__c as CEM_Digitado,
IND.mtocemasesorventa as CEM_Vendedor,
IND.mtocemanalistacredito as CEM_Analista,
CASE WHEN IND.destiprolsolicitud ='TITULAR' THEN IND.dessubsegmento ELSE NULL END AS SegmentoTitular,
--CASE WHEN IND.destiprolsolicitud ='TITULAR' THEN IND.codsubsegmento ELSE NULL END AS SubsegmentoTitular,
CASE WHEN IND.destiprolsolicitud ='TITULAR' THEN IND.mtodeudatotalbcp ELSE NULL END AS Posicion_Consolidada,
CASE WHEN IND.destiprolsolicitud ='TITULAR' THEN IND.codsegmentoriesgoscore ELSE NULL END AS SegmentoRiesgosTitular,
CASE WHEN IND2.destiprolsolicitud ='CONYUGE' THEN IND2.codsegmentoriesgoscore ELSE NULL END AS SegmentoRiesgosConyuge,
CASE WHEN a.fecdesembolso IS NOT NULL THEN a.mtoaprobado ELSE NULL END as mto_desembolsado,
CASE WHEN IND.destiprolsolicitud ='TITULAR' THEN IND.destipniveleducacional ELSE NULL END AS Grado_Instruccion,
CASE WHEN IND.destiprolsolicitud ='TITULAR' THEN IND.destipocupacion ELSE NULL END AS Situacion_LaboralTitular,
CASE WHEN IND2.destiprolsolicitud ='CONYUGE' THEN IND2.destipocupacion ELSE NULL END AS Situacion_LaboralConyuge,

--CASE WHEN C.bcp_persontype__c='T' THEN C.BCP_SBSRISKCLASSIFICATION__C ELSE NULL END AS Clasificacion_SbsTitular, --Falta migrar a Udv
--CASE WHEN C2.bcp_persontype__c='C' then C2.BCP_SBSRISKCLASSIFICATION__C ELSE NULL END AS Clasificacion_SbsConyuge, --Falta migrar a Udv
CASE WHEN PTY.destiprolsolicitud ='TITULAR' THEN PTY.destipclasifriesgocli ELSE NULL END AS Clasificacion_SbsTitular, 
CASE WHEN PTY2.destiprolsolicitud ='CONYUGE' THEN PTY2.destipclasifriesgocli ELSE NULL END AS Clasificacion_SbsConyuge, 

CASE WHEN IND.destiprolsolicitud ='TITULAR' THEN IND.numdependiente ELSE NULL END AS N_DependientesTitular,
CASE WHEN IND.destiprolsolicitud ='TITULAR' THEN IND.destipestcivil ELSE NULL END AS EstadoCivil,

--A.destipevaluacionriesgo,
--E.BCP_UnidadNegocio__c as AreaVendedor,
--E.BCP_Region__c as RegionVendedor,
--B.BCP_TITLE__C as FuncionVendedor,
--E.BCP_Puesto_Salesforce__c as FuncionVendedor,
P1.DESPOSICIONCOLABORADOR as FuncionVendedor,
--E.BCP_User_Profile__c AS Perfil_Vendedor,
--P1.DESTIPPERFILUSUARIOAPP as Perfil_Vendedor
--P1.DESTIPUNIDADNEGAPP as AreaVendedor,
--P1.NBRAGE as AgenciaVendedor
--E.BCP_BusinessManagerNumber__c as SupervisorVendedor,
--E.BCP_UserCategory__c as CategoriaVendedor
--B.BCP_ListView__c,
--B.BCP_CampaignDescription__c
--'Sin data' as Motivo_CDA, --motivos de derivacion
--b.BCP_VerifiedRevenue__c as IngresoVerificado,
A.mtoingresobrutosolicitud as IngresoVerificado,
----NUEVOS CAMPOS
day(a.fecsolicitud) as DiaSolicitud,
month(a.fecsolicitud) as MesSolicitud,
year(a.fecsolicitud) as AnoSolicitud,
date_format(a.fecsolicitud,"yyyyMM")as AnoMesSolicitud,
--concat(AnoSolicitud,MesSolicitud) as AnoMesSolicitud,
--CASE WHEN IND.destiprolsolicitud ='TITULAR' THEN IND.numpuntajeprecalif ELSE NULL END AS ScoreEvaluacion,
CASE 
when IND.numpuntajeprecalif <=0 then 'Sin data' 
when IND.numpuntajeprecalif <=300 then 'A. 0-300' 
when IND.numpuntajeprecalif <=330 then 'B. 300-330' 
when IND.numpuntajeprecalif <=360 then 'C. 330-360' 
when IND.numpuntajeprecalif <=390 then 'D. 360-390' 
when IND.numpuntajeprecalif <=420 then 'E. 390-420' 
when IND.numpuntajeprecalif <=450 then 'F. 42-450' 
ELSE NULL END as Rango_Score,
--
CASE 
WHEN A.nbrmoneda = "DOLAR AMERICANO             EE.UU       " THEN A.mtosolicitado * 3.81
ELSE A.mtosolicitado END as MontoSolicitado_Soles,
--
CASE 
WHEN A.nbrmoneda = "DOLAR AMERICANO             EE.UU       "  THEN mtoaprobado * 3.81 --Estado_F="ACEPTADA"
ELSE mtoaprobado END as MontoAprobado_Soles,
--
CASE 
WHEN A.nbrmoneda = "DOLAR AMERICANO             EE.UU       "  AND Estado_F="ACEPTADA" THEN mto_desembolsado * 3.81
ELSE mto_desembolsado END as MontoDesembolsado_Soles,
--
CASE 
WHEN MontoDesembolsado_Soles>0 THEN "Si"
ELSE "No" END as Flag_MontoDesembolsado_Soles,
--
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
--
date_diff(second,Fecha_Registro,Fin_Evaluacion)/3600 as TiempoCliente_Hrs,
date_diff(second,Inicio_Evaluacion,Fin_Evaluacion)/3600 as TiempoAnalista_Hrs,
date_diff(second,Fecha_Registro,Fin_Evaluacion) as TotalSegundo_TiempoCliente,
date_diff(second,Inicio_Evaluacion,Fin_Evaluacion) as TotalSegundo_TiempoAnalista,
--Case when TiempoCliente_Hrs < 24 then 'Dentro 24hrs' else 'Mayor 24hrs' end as Atencion24HrsCliente,
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
-------------------
-------------------



/*CASE
WHEN a.tipevaluacionriesgo = 'APR' THEN '100% APROBADO'
WHEN a.tipevaluacionriesgo = 'REA' THEN 'SIN CAMPAÑA'
WHEN a.tipevaluacionriesgo= 'PRE' THEN 'PREAPROBADO'
WHEN a.tipevaluacionriesgo = 'INV' THEN 'INVITADO'
WHEN a.tipevaluacionriesgo = 'OPT' THEN 'OPTIMUS'
ELSE NULL END as CampanaCorta*/
--
/*CASE
WHEN contains(a.descampania,'COMPRA DE DEUDA') THEN 'CD'
WHEN contains(a.descampania,'CEF LD -') THEN 'LD'
WHEN contains(a.descampania,'CEF-LD:') THEN 'LD'
WHEN contains(a.descampania,'CEF LD:') THEN 'LD'
WHEN contains(a.descampania,'LD SFCP') THEN 'LD'
ELSE '???' END as TipoVariantecampana2,*/

----
from catalog_lhcl_prod_bcp.bcp_udv_int_v.m_solicitudcreditoconsumo A

LEFT JOIN catalog_lhcl_prod_bcp.bcp_udv_int_v.M_USUARIOAPLICATIVO P1
ON A.codcolaboradorvendedor = P1.CODUSUARIOAPP

-------
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
---------
---------


where a.fecsolicitud >= '2025-10-01'
--and a.fecsolicitud <= '2025-10-28'

--and B.bcp_approvalstatus__c <> 'Iniciado'
--Where A.codsolicitud ='O0009833476'
--where A.codsolicitud in ('O0011986087','O0010834434')
--in ('O0014728931','O0014714058','O0014733439','O0014706731','O0014710469','O0014718229','O0015836413','O0015855431','O0015728752','O0015459704','O0015735227','O0015658217','O0015525493')
--Where A.codsolicitud in ('O0015398334','O0015843609')
--Where A.codsolicitud in ('O0012621711','O0012563606','O0009871666','O0012677019')
--Where A.codsolicitud in ('O0015836413','O0015855431','O0015728752','O0015459704','O0015735227','O0015658217','O0015525493')
--Where A.codsolicitud in ('O0015728752')
--where A.codsolicitud ='O0012816043'



