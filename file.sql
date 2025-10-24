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
