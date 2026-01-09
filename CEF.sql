Cuando ocurre estos casos en el menu de opciones que hay slae asi
<div id="Menu">
  <ul class="horizontal">
    
      <li>
        
        
        
          <span>Consolidado</span>
        
      </li>
    
      <li>
        
        
        
          <span>Por Tipo de Credito</span>
        
      </li>
    
      <li>
        
          <a id="idOp2" href="#" title="Información Histórica">Historica</a>
        
        
        
      </li>
    
      <li>
        
        
        
          <span>Adicional</span>
        
      </li>
    
      <li>
        
        
        
          <span>Detallada</span>
        
      </li>
    
      <li>
        
        
        
          <span>Al Cliente</span>
        
      </li>
    
      <li>
        
          <a id="idOp6" href="#" title="Información referida a otros reportes">Otros Reportes</a>
        
        
        
      </li>
    
      <li>
        
        
        
          <span>RIO</span>
        
      </li>
    
  </ul>
</div>



Solo esta ahbilitado Historico y Otros reportes

Entonces en el caso de este alert para eso.
Por ello debemos tomar captura a esta pagina que sale de historico que en html es:
<body>
    <!-- CABECERA -->
    
      
      
      
      
      
      
      
      
      
    







<link href="style/extranet.css" rel="stylesheet">
<div id="overDiv" style="position: absolute; visibility: hidden; z-index: 1;"></div>
<script type="text/javascript" async="" src="https://ssl.google-analytics.com/ga.js"></script><script type="text/javascript" src="Jscript/Print.js"></script>
<script type="text/javascript" src="Jscript/Tagover.js"></script>
<!-- PROYECTO GTI 6.5.9 | 2022 - INICIO -->
<!-- <script type="text/javascript" src="Jscript/jquery.js"></script> -->
<script type="text/javascript" src="Jscript/jquery-3.6.1.min.js"></script>
<!-- PROYECTO GTI 6.5.9 | 2022 - FIN -->
<script type="text/javascript" src="Jscript/jquery.blockUI.js?v2.25"></script>
<script type="text/javascript" src="Jscript/ga.js"></script>




  <script type="text/javascript" src="Jscript/extjscr.js"></script>


<script type="text/javascript">
  window.onbeforeprint = removeelements;
  window.onafterprint = revertback;

  $(document).ready(function() {
	$("form").submit(function() {
	  $("#sCodAsociado").val("");
	  $("#sNomAsociado").val("");
	  
	  if (checkForm()) {
		$.blockUI({
		  message : "<h1>Por favor, espere...</h1>",
		  overlayCSS : {
			backgroundColor : '#C5EBFA'
		  },
		  css : {
			border : 'none',
			padding : '10px',
			backgroundColor : '#336699',
			'-webkit-border-radius' : '10px',
			'-moz-border-radius' : '10px',
			opacity : .5,
			color : '#fff'
		  }
		});

		return true;
	  }

	  return false;
	});

	$("select[name=as_tipo_doc]").val("11");

	$(".lnkImprimir").click(function(event) {
	  event.preventDefault();
	  Imprimir();
	});

	$("#lnkManual").click(function(event) {
	  event.preventDefault();
	  WManual('00002');
	});

	$("#lnkConsultas").click(function(event) {
	  event.preventDefault();
	  WConsultas('00002');
	});

	$("#lnkGlosario").click(function(event) {
	  event.preventDefault();
	  WGlosario('00002');
	});

	$(".lnkRegresar").click(function() {
	  // PROYECTO GTI 6.5.9 | 2022 - INICIO
	  // jsRegresar('', '');
	  jsRegresar('aj4vAYR1CRX5SR5rjQbE8Ei0MhdMR+IdPK6DE0lZjRs=', '');
	  // PROYECTO GTI 6.5.9 | 2022 - FIN
	});
  });
</script>


  <script type="text/javascript">
	$(document).ready(function() {
	  $("#idOp0").click(function() {
		eval("verConsolidado(-224709996715837781)");
	  });
	});
  </script>

  <script type="text/javascript">
	$(document).ready(function() {
	  $("#idOp1").click(function() {
		eval("verxTipodeCredito(88175962766540838)");
	  });
	});
  </script>

  <script type="text/javascript">
	$(document).ready(function() {
	  $("#idOp2").click(function() {
		eval("verxHistorico(-9195143277111294426)");
	  });
	});
  </script>

  <script type="text/javascript">
	$(document).ready(function() {
	  $("#idOp3").click(function() {
		eval("verAdicional(2161885552363733595)");
	  });
	});
  </script>

  <script type="text/javascript">
	$(document).ready(function() {
	  $("#idOp4").click(function() {
		eval("verDetallada(-5738630263942389890)");
	  });
	});
  </script>

  <script type="text/javascript">
	$(document).ready(function() {
	  $("#idOp5").click(function() {
		eval("verPau(-8843363938902678467)");
	  });
	});
  </script>

  <script type="text/javascript">
	$(document).ready(function() {
	  $("#idOp6").click(function() {
		eval("verOtrosReportes(-8079937720100670460)");
	  });
	});
  </script>

  <script type="text/javascript">
	$(document).ready(function() {
	  $("#idOp7").click(function() {
		eval("verRio(-338827749954007030)");
	  });
	});
  </script>




<div id="Cabecera">
  <ul class="horizontal">
    <li class="manual"><a href="#" id="lnkManual">Manual</a></li>

    <li><a href="#" id="lnkConsultas">Consultas</a></li>

    <li><a href="#" id="lnkGlosario">Glosario</a></li>

    <li><a href="/criesgos/logout?c_c_producto=00002">Salir</a></li>
  </ul>

  <div id="AppName">Central de Riesgos</div>

  <div id="UserName">Luis Felipe Pachas Lachira</div>
</div>

<div id="Menu">
  <ul class="horizontal">
    
      <li>
        
        
        
          <span>Consolidado</span>
        
      </li>
    
      <li>
        
        
        
          <span>Por Tipo de Credito</span>
        
      </li>
    
      <li>
        
          <a id="idOp2" href="#" title="Información Histórica">Historica</a>
        
        
        
      </li>
    
      <li>
        
        
        
          <span>Adicional</span>
        
      </li>
    
      <li>
        
        
        
          <span>Detallada</span>
        
      </li>
    
      <li>
        
        
        
          <span>Al Cliente</span>
        
      </li>
    
      <li>
        
          <a id="idOp6" href="#" title="Información referida a otros reportes">Otros Reportes</a>
        
        
        
      </li>
    
      <li>
        
        
        
          <span>RIO</span>
        
      </li>
    
  </ul>
</div>

<div id="FormConsulta">
  <table style="width: 100%">
    <tbody><tr>
      <td style="width: 30%">
        <span> ::
          <!-- TITULO -->
          
          
          
            Posición Histórica
           &gt;
          
          <!-- PERIODO -->
          
        </span>
      </td>
      <td style="width: 70%; text-align: right;">
        
          <form method="POST" action="buscarposicionconsolidada" style="margin: 0">
            <input type="hidden" name="c_c_page_back" value="03">
            
            <span>Nueva Consulta&nbsp;:</span>
            
            <select name="as_tipo_doc" id="as_tipo_doc">
              <option value="-1">Tipo de Documento</option>
              <option value="11">1. LE/DNI</option>
              <option value="12">2. Carnet de Extranjería</option>
              <option value="13">3. Carnet de Identidad FFPP</option>
              <option value="14">4. Carnet de Identidad FFAA</option>
              <option value="15">5. Pasaporte</option>
              <option value="21">6. RUC</option>
              
              <option value="17">7. Carnet de Identidad emitido por el Ministerio de Relaciones Exteriores</option>
              <option value="18">8. Carnet de Permiso Temporal de Permanencia</option>
              <option value="00">9. Código SBS</option>
              
              <option value="1A">A. Cédula de identidad o documentos análogos</option>
              <option value="1B">B. Carnet del refugiado</option>
              <option value="1C">C. Documento expedido por la CEPR del Ministerio de Relaciones Exteriores que
                acredita que la solicitud de refugiado se encuentra en trámite.</option>
            </select>
            
            <input type="text" size="14" maxlength="16" onkeypress="return OnKeyPressValidInput(event)" onchange="ValInputValid(this)" class="input-upper" id="as_doc_iden" name="as_doc_iden" value="76500660">
            
            <input type="Submit" name="Submit" value="Consultar" id="btnConsultar" class="boton">
            <!-- PROYECTO GTI 6.5.9 | 2022 - INICIO -->
            <input type="hidden" name="validation" id="validation" value="4KRTHR1r2EIwZvuuQpYNMK3APqZsclznn0uAb8uq0p8=">
            
            <input type="hidden" name="pt" value="">
            <!-- PROYECTO GTI 6.5.9 | 2022 - FIN -->
            <input type="hidden" name="c_c_producto" value="00002">
            <input type="hidden" name="ah_cod_sbs" value="bdSOVWoon12k+Qwh3QzOPZX7ChAeCKPMSSCoAvd2wf0=">
            <input type="hidden" name="ah_barra" value="001000100">
            <input type="hidden" name="as_nuevo" value="NUEVO">
            <input type="hidden" name="ah_completa" value="S">
            <input type="hidden" name="sCodAsociado" id="sCodAsociado" value="">
            <input type="hidden" name="sNomAsociado" id="sNomAsociado" value="">
            <input type="hidden" name="sAction" id="sAction" value="">
            <input type="hidden" name="ah_periodo_consulta" value="">
            <input type="hidden" name="hdnTodosPeriodos" value="">
            <input type="hidden" name="hdnArbolPeriodos" value="">
          </form>
        
      </td>
    </tr>
  </tbody></table>
</div>

  <!-- TITULO -->
  <div id="TituloConsulta">
    <table style="width: 100%;">
      <tbody><tr>
        <td class="Izq"><span class="Dt">Posición Histórica</span></td>
        <td class="Der">
          <b class="Dz" style="color: #0099CC">
            Lima,
            
            9 de enero de 2026
          </b>
        </td>
      </tr>

      <tr>
        <td>&nbsp;</td>
        <td class="Der">
          <ul id="Imprimir" class="horizontal">
            <li><a href="#" class="lnkImprimir" title="Imprimir"><img src="images/icon_print.gif"></a></li>

            <li><a href="#" class="lnkImprimir" title="Imprimir">Imprimir</a></li>

            
          </ul>
        </td>
      </tr>
    </tbody></table>
  </div>

    
    <div id="Contenido">
      <!-- DATOS DE IDENTIFICACION -->
      
        
      











<!-- DATOS DEL DEUDOR -->
<table class="Crw">
  <colgroup><col width="20%">
  <col width="18%">
  <col width="15%">
  <col width="15%">
  <col width="17%">
  <col width="15%">
  </colgroup><thead>
    <tr>
      <td class="Izq" colspan="6">
        <b class="F">Datos del Deudor</b>
      </td>
    </tr>
  </thead>
  <tbody>
    <tr class="Def">
      <td class="Izq">
        <b class="Dz">Documento</b>
      </td>
      <td class="Izq Inf">
        <span class="Dz">
          DNI
        </span>
      </td>
      <td class="Izq">
        <b class="Dz">Número</b>
      </td>
      <td class="Izq Inf">
        <span class="Dz">
          76500660
        </span>
      </td>
      <td class="Izq">
        <b class="Dz">Persona</b>
      </td>
      <td class="Izq Inf">
        <span class="Dz">
          Natural
        </span>
      </td>
    </tr>
    <tr class="Def">
      <td class="Izq">
        <b class="Dz">
          Apellido Paterno
        </b>
      </td>
      <td class="Izq Inf" colspan="3">
        <span class="Dz">
          QUISPE
        </span>
      </td>
      
      
      <td>&nbsp;</td>
      <td class="Inf">&nbsp;</td>
      
      
    </tr>
    
    <tr class="Def">
      <td class="Izq">
        <b class="Dz">
          Apellido Materno
        </b>
      </td>
      <td class="Izq Inf">
        <span class="Dz">
          
          TUANAMA
          
        </span>
      </td>
      <td class="Izq">
        <b class="Dz">
          Ape. de Casada
        </b>
      </td>
      <td class="Izq Inf">
        <span class="Dz">
          
           
          
        </span>
      </td>
      <td class="Izq">
        
      </td>
      <td class="Izq Inf">
        
      </td>
    </tr>
    <tr class="Def">
      <td class="Izq">
        <b class="Dz">
          Nombres
        </b>
      </td>
      <td class="Izq Inf" colspan="3">
        <span class="Dz">
          
          ALMENDRA ESMERALDA
          
        </span>
      </td>
      <td class="Izq">
        <b class="Dz">Código SBS</b>
      </td>
      <td class="Izq Inf">
        <span class="Dz">
          221981707
        </span>
      </td>
    </tr>
    
    
    
    
    
  </tbody>
</table>

       
      
      <!-- INFORMACION HISTORICA -->
      <table class="Crw">
        <thead>
          <tr>
            <td class="Izq" colspan="7">
              <span class="F">Información Histórica</span>
            </td>
          </tr>
        </thead>
         
        <tbody>
          <tr class="Str">
            <td class="Izq">
              <b class="Dz">SALDOS</b>
            </td>
            <!-- DESCRIPCION DE PERIODOS -->
            
            <td align="center" width="12%">
              
              <img src="images/cuad.gif" alt="">
              
               
              
              <span class="Dz"> 
                Nov-2024
              </span>
              
            </td>
            
            <td align="center" width="12%">
              
              <img src="images/cuad.gif" alt="">
              
               
              
              <span class="Dz"> 
                Mar-2025
              </span>
              
            </td>
            
            <td align="center" width="12%">
              
              <img src="images/cuad.gif" alt="">
              
               
              
              <span class="Dz"> 
                Jun-2025
              </span>
              
            </td>
            
            <td align="center" width="12%">
              
              <img src="images/cuad.gif" alt="">
              
               
              
              <span class="Dz"> 
                Set-2025
              </span>
              
            </td>
            
            <td align="center" width="12%">
              
              <a onclick="verPosicionxPeriodo('202510', '7258931841614892459'); return false;" href="#"><img src="images/mas.gif" alt="Posición Consolidada a Oct-2025"></a>
              
               
              
              <a onclick="verPosicionxPeriodo('202510', '-1119479934827764598'); return false;" href="#"><b class="Dz">Oct-2025</b></a>
              
            </td>
            
            <td align="center" width="12%">
              
              <img src="images/cuad.gif" alt="">
              
               
              
              <span class="Dz"> 
                Nov-2025
              </span>
              
            </td>
            
          </tr>
          <!-- SALDOS -->
          
          <tr>
            <td class="Izq" bgcolor="#EAF8FD">
              <span class="Dz">
                Vigente
              </span>
            </td>
            <!-- SALDOS POR PERIODO -->
            
            <td class="Der" bgcolor="#F0F0F0">
              <span class="Dz">
                
               </span>
            </td>
            
            <td class="Der" bgcolor="#F0F0F0">
              <span class="Dz">
                
               </span>
            </td>
            
            <td class="Der" bgcolor="#F0F0F0">
              <span class="Dz">
                
               </span>
            </td>
            
            <td class="Der" bgcolor="#F0F0F0">
              <span class="Dz">
                
               </span>
            </td>
            
            <td class="Der" bgcolor="#F0F0F0">
              <span class="Dz">
                
                  150
                 
               </span>
            </td>
            
            <td class="Der" bgcolor="#F0F0F0">
              <span class="Dz">
                
               </span>
            </td>
            
          </tr>
          
          <tr>
            <td class="Izq" bgcolor="#C5EBFA">
              <span class="Dz">
                TOTAL DEUDA DIRECTA
              </span>
            </td>
            <!-- SALDOS POR PERIODO -->
            
            <td class="Der" bgcolor="#C5EBFA">
              <span class="Dz">
                
               </span>
            </td>
            
            <td class="Der" bgcolor="#C5EBFA">
              <span class="Dz">
                
               </span>
            </td>
            
            <td class="Der" bgcolor="#C5EBFA">
              <span class="Dz">
                
               </span>
            </td>
            
            <td class="Der" bgcolor="#C5EBFA">
              <span class="Dz">
                
               </span>
            </td>
            
            <td class="Der" bgcolor="#C5EBFA">
              <span class="Dz">
                
                  150
                 
               </span>
            </td>
            
            <td class="Der" bgcolor="#C5EBFA">
              <span class="Dz">
                
               </span>
            </td>
            
          </tr>
          
          <tr>
            <td class="Izq" bgcolor="#62c7ef">
              <span class="Dz">
                TOTAL DEUDA
              </span>
            </td>
            <!-- SALDOS POR PERIODO -->
            
            <td class="Der" bgcolor="#62c7ef">
              <span class="Dz">
                
               </span>
            </td>
            
            <td class="Der" bgcolor="#62c7ef">
              <span class="Dz">
                
               </span>
            </td>
            
            <td class="Der" bgcolor="#62c7ef">
              <span class="Dz">
                
               </span>
            </td>
            
            <td class="Der" bgcolor="#62c7ef">
              <span class="Dz">
                
               </span>
            </td>
            
            <td class="Der" bgcolor="#62c7ef">
              <span class="Dz">
                
                  150
                 
               </span>
            </td>
            
            <td class="Der" bgcolor="#62c7ef">
              <span class="Dz">
                
               </span>
            </td>
            
          </tr>
          
          <tr class="Str">
            <td class="Izq">
              <b class="Dz">Nro Emp. que adeuda</b>
            </td>
            
            <td class="Der">
              
            </td>
            
            <td class="Der">
              
            </td>
            
            <td class="Der">
              
            </td>
            
            <td class="Der">
              
            </td>
            
            <td class="Der">
              
              <a onclick="verDetallexPeriodo('202510', '-3324916950926820461'); return false;" href="#"><img src="images/mas.gif" alt=""></a>
                
              <a onclick="verDetallexPeriodo('202510', '-1021968254783170058'); return false;" href="#"><span class="Dz">1</span></a>
              
            </td>
            
            <td class="Der">
              
            </td>
            
          </tr>
        </tbody>
      </table>
      
       
      
       
      <!-- PIE DE PAGINA -->
       
      
        
      


<p class="Dz Izq">(*) Montos Expresados en Soles</p>


<p>
  <img src="images/logo-sbs.jpg" alt="SBS - Central de Riesgos">
</p>
    </div>
  
</body>
