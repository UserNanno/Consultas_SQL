Body de "Otros Reportes" cuando sale la segunda tabla de "Otros reportes" pero no existe nada para desglosar y la primera tabla "Datos del Deudor"
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
	  jsRegresar('GQtolvR9R5ifXekjH00nQH11JuFSEiRyWlD/7XgbWF0=', '');
	  // PROYECTO GTI 6.5.9 | 2022 - FIN
	});
  });
</script>


  <script type="text/javascript">
	$(document).ready(function() {
	  $("#idOp0").click(function() {
		eval("verConsolidado(6672445374915720263)");
	  });
	});
  </script>

  <script type="text/javascript">
	$(document).ready(function() {
	  $("#idOp1").click(function() {
		eval("verxTipodeCredito(7067583700095226355)");
	  });
	});
  </script>

  <script type="text/javascript">
	$(document).ready(function() {
	  $("#idOp2").click(function() {
		eval("verxHistorico(-5033036592699094217)");
	  });
	});
  </script>

  <script type="text/javascript">
	$(document).ready(function() {
	  $("#idOp3").click(function() {
		eval("verAdicional(-9123728983113122231)");
	  });
	});
  </script>

  <script type="text/javascript">
	$(document).ready(function() {
	  $("#idOp4").click(function() {
		eval("verDetallada(-3785996328051991119)");
	  });
	});
  </script>

  <script type="text/javascript">
	$(document).ready(function() {
	  $("#idOp5").click(function() {
		eval("verPau(2032258925741021373)");
	  });
	});
  </script>

  <script type="text/javascript">
	$(document).ready(function() {
	  $("#idOp6").click(function() {
		eval("verOtrosReportes(1240617577417089239)");
	  });
	});
  </script>

  <script type="text/javascript">
	$(document).ready(function() {
	  $("#idOp7").click(function() {
		eval("verRio(4745412027546058193)");
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
        
          <a id="idOp0" href="#" title="Posición Consolidada del Deudor">Consolidado</a>
        
        
        
      </li>
    
      <li>
        
          <a id="idOp1" href="#" title="Posición del Deudor por Tipo de Crédito">Por Tipo de Credito</a>
        
        
        
      </li>
    
      <li>
        
          <a id="idOp2" href="#" title="Información Histórica">Historica</a>
        
        
        
      </li>
    
      <li>
        
          <a id="idOp3" href="#" title="Información Adicional">Adicional</a>
        
        
        
      </li>
    
      <li>
        
          <a id="idOp4" href="#" title="Información Detallada">Detallada</a>
        
        
        
      </li>
    
      <li>
        
          <a id="idOp5" href="#" title="Reporte preparado para el Cliente">Al Cliente</a>
        
        
        
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
          
          
          
            Otros Reportes
           &gt;
          
          <!-- PERIODO -->
          
        </span>
      </td>
      <td style="width: 70%; text-align: right;">
        
          <form method="POST" action="buscarposicionconsolidada" style="margin: 0">
            <input type="hidden" name="c_c_page_back" value="07">
            
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
            
            <input type="text" size="14" maxlength="16" onkeypress="return OnKeyPressValidInput(event)" onchange="ValInputValid(this)" class="input-upper" id="as_doc_iden" name="as_doc_iden" value="78801600">
            
            <input type="Submit" name="Submit" value="Consultar" id="btnConsultar" class="boton">
            <!-- PROYECTO GTI 6.5.9 | 2022 - INICIO -->
            <input type="hidden" name="validation" id="validation" value="L7rulh1u5qHCgzPkt/KTTcQakCLaiyM4Wq5kwQnD14M=">
            
            <input type="hidden" name="pt" value="">
            <!-- PROYECTO GTI 6.5.9 | 2022 - FIN -->
            <input type="hidden" name="c_c_producto" value="00002">
            <input type="hidden" name="ah_cod_sbs" value="VgiO1Ze5zHQDFrYo4+mkVvoxwAZtghQvbaGXmWBMRfI=">
            <input type="hidden" name="ah_barra" value="111111100">
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
        <td class="Izq"><span class="Dt">Otros Reportes</span></td>
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
          78801600
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
          CANECILLAS
        </span>
      </td>
      
      
      <td class="Izq">
        <b class="Dz">N° Entidades</b>
      </td>
      <td class="Izq Inf">        
        <span class="Dz">
          1
        </span>
        
      </td>
      
      
    </tr>
    
    <tr class="Def">
      <td class="Izq">
        <b class="Dz">
          Apellido Materno
        </b>
      </td>
      <td class="Izq Inf">
        <span class="Dz">
          
          CONTRERAS
          
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
        
        <b class="Dz">Fecha Reporte</b>
        
      </td>
      <td class="Izq Inf">
        
        <b id="FecReporte" class="Dz Red">
          30/11/2025
        </b>
        
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
          
          JUAN MARIANO
          
        </span>
      </td>
      <td class="Izq">
        <b class="Dz">Código SBS</b>
      </td>
      <td class="Izq Inf">
        <span class="Dz">
          226092366
        </span>
      </td>
    </tr>
    
    
    
    <!-- DISTRIBUCION -->
    <tr>
      <td class="Inf" colspan="6">
        <table class="EE">
          <tbody><tr>
            <td class="Izq">
              <b class="Dz">
                Distribución Porcentual según Clasificación
                <sup>1</sup>
                :
              </b>
            </td>
            <td height="18" width="70"><font class="Dz">0 :100%</font></td><td height="18" width="70"><font class="Dz">1 :0%</font></td><td height="18" width="70"><font class="Dz">2 :0%</font></td><td height="18" width="70"><font class="Dz">3 :0%</font></td><td height="18" width="70"><font class="Dz">4 :0%</font></td>
          </tr>
        </tbody></table>
      </td>
    </tr>
    
    
    
  </tbody>
</table>

      
      <!-- OTROS REPORTES -->
      <table class="Crw">
        <thead>
          <tr>
            <td class="Izq">
              <span class="F">Otros Reportes</span>
            </td>
          </tr>
        </thead>
         
        <tbody>
          <tr class="Def">
            <td class="Izq">
              
               
              No existe información en Otros Reportes
               
              
            </td>
          </tr>
        </tbody>
      </table>
      
      <div class="Aviso">
        <b class="Dz">Aviso Importante</b>
        <p class="Dz" align="justify">
          La información mostrada en este reporte ha sido proporcionada directamente por las entidades supervisadas. Por consiguiente, la Superintendencia de Banca, Seguros y Administradoras Privadas de Fondos de Pensiones no es responsable por la veracidad y corrección de los montos y calificaciones de las deudas informadas.
        </p>
      </div>
      
      <!-- PIE DE PAGINA -->
      
        
        
      



<p class="Dz Izq">1: A partir de la información de septiembre-2005, la distribución porcentual de las obligaciones de acuerdo a las clasificaciones incluye los saldos de los créditos castigados</p>

<p>
  <img src="images/logo-sbs.jpg" alt="SBS - Central de Riesgos">
</p>
    </div>
  
</body>



En el otro caso (out) cuando en segunda tabla "Otros reportes" si figura para desglosar

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
	  jsRegresar('izo4pk2ZAwvNBa2BcnZXhVeXtp1GIaD9F8pz+v3i6A4=', '');
	  // PROYECTO GTI 6.5.9 | 2022 - FIN
	});
  });
</script>


  <script type="text/javascript">
	$(document).ready(function() {
	  $("#idOp0").click(function() {
		eval("verConsolidado(-8658983566831504252)");
	  });
	});
  </script>

  <script type="text/javascript">
	$(document).ready(function() {
	  $("#idOp1").click(function() {
		eval("verxTipodeCredito(-7843143492855223231)");
	  });
	});
  </script>

  <script type="text/javascript">
	$(document).ready(function() {
	  $("#idOp2").click(function() {
		eval("verxHistorico(5788700361388580024)");
	  });
	});
  </script>

  <script type="text/javascript">
	$(document).ready(function() {
	  $("#idOp3").click(function() {
		eval("verAdicional(6332931711338771330)");
	  });
	});
  </script>

  <script type="text/javascript">
	$(document).ready(function() {
	  $("#idOp4").click(function() {
		eval("verDetallada(-2589224193783332156)");
	  });
	});
  </script>

  <script type="text/javascript">
	$(document).ready(function() {
	  $("#idOp5").click(function() {
		eval("verPau(6657268598983056544)");
	  });
	});
  </script>

  <script type="text/javascript">
	$(document).ready(function() {
	  $("#idOp6").click(function() {
		eval("verOtrosReportes(2451957141955347733)");
	  });
	});
  </script>

  <script type="text/javascript">
	$(document).ready(function() {
	  $("#idOp7").click(function() {
		eval("verRio(-8242256905460551965)");
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
        
          <a id="idOp0" href="#" title="Posición Consolidada del Deudor">Consolidado</a>
        
        
        
      </li>
    
      <li>
        
          <a id="idOp1" href="#" title="Posición del Deudor por Tipo de Crédito">Por Tipo de Credito</a>
        
        
        
      </li>
    
      <li>
        
          <a id="idOp2" href="#" title="Información Histórica">Historica</a>
        
        
        
      </li>
    
      <li>
        
          <a id="idOp3" href="#" title="Información Adicional">Adicional</a>
        
        
        
      </li>
    
      <li>
        
          <a id="idOp4" href="#" title="Información Detallada">Detallada</a>
        
        
        
      </li>
    
      <li>
        
          <a id="idOp5" href="#" title="Reporte preparado para el Cliente">Al Cliente</a>
        
        
        
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
          
          
          
            Otros Reportes
           &gt;
          
          <!-- PERIODO -->
          
        </span>
      </td>
      <td style="width: 70%; text-align: right;">
        
          <form method="POST" action="buscarposicionconsolidada" style="margin: 0">
            <input type="hidden" name="c_c_page_back" value="07">
            
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
            
            <input type="text" size="14" maxlength="16" onkeypress="return OnKeyPressValidInput(event)" onchange="ValInputValid(this)" class="input-upper" id="as_doc_iden" name="as_doc_iden" value="72811352">
            
            <input type="Submit" name="Submit" value="Consultar" id="btnConsultar" class="boton">
            <!-- PROYECTO GTI 6.5.9 | 2022 - INICIO -->
            <input type="hidden" name="validation" id="validation" value="L7rulh1u5qHCgzPkt/KTTcQakCLaiyM4Wq5kwQnD14M=">
            
            <input type="hidden" name="pt" value="">
            <!-- PROYECTO GTI 6.5.9 | 2022 - FIN -->
            <input type="hidden" name="c_c_producto" value="00002">
            <input type="hidden" name="ah_cod_sbs" value="o0Rdy+km4mj6b/WwaY4BgQjedJWWeEAlbZBjofOcuLw=">
            <input type="hidden" name="ah_barra" value="111111100">
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
        <td class="Izq"><span class="Dt">Otros Reportes</span></td>
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
          72811352
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
          VASQUEZ
        </span>
      </td>
      
      
      <td class="Izq">
        <b class="Dz">N° Entidades</b>
      </td>
      <td class="Izq Inf">        
        <span class="Dz">
          1
        </span>
        
      </td>
      
      
    </tr>
    
    <tr class="Def">
      <td class="Izq">
        <b class="Dz">
          Apellido Materno
        </b>
      </td>
      <td class="Izq Inf">
        <span class="Dz">
          
          SARANGO
          
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
        
        <b class="Dz">Fecha Reporte</b>
        
      </td>
      <td class="Izq Inf">
        
        <b id="FecReporte" class="Dz Red">
          30/11/2025
        </b>
        
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
          
          GUIDO MARTIN
          
        </span>
      </td>
      <td class="Izq">
        <b class="Dz">Código SBS</b>
      </td>
      <td class="Izq Inf">
        <span class="Dz">
          170696476
        </span>
      </td>
    </tr>
    
    
    
    <!-- DISTRIBUCION -->
    <tr>
      <td class="Inf" colspan="6">
        <table class="EE">
          <tbody><tr>
            <td class="Izq">
              <b class="Dz">
                Distribución Porcentual según Clasificación
                <sup>1</sup>
                :
              </b>
            </td>
            <td height="18" width="70"><font class="Dz">0 :100%</font></td><td height="18" width="70"><font class="Dz">1 :0%</font></td><td height="18" width="70"><font class="Dz">2 :0%</font></td><td height="18" width="70"><font class="Dz">3 :0%</font></td><td height="18" width="70"><font class="Dz">4 :0%</font></td>
          </tr>
        </tbody></table>
      </td>
    </tr>
    
    
    
  </tbody>
</table>

      
      <!-- OTROS REPORTES -->
      <table class="Crw">
        <thead>
          <tr>
            <td class="Izq">
              <span class="F">Otros Reportes</span>
            </td>
          </tr>
        </thead>
         
        <tbody>
          <tr class="Def">
            <td class="Izq">
              
              <ul id="OtrosReportes" class="horizontal">
                
                 
                
                <li>
                  <a onclick="jsVerCT('141313988563468330'); return false;" href="#">
                    <img src="images/mas.gif" alt="Carteras Transferidas"></a>
                </li>
                 
                <li>
                  <a onclick="jsVerCT('8911663787186198077'); return false;" href="#">Carteras Transferidas</a>
                </li>
                
                 
                
                
                
              </ul>
              
            </td>
          </tr>
        </tbody>
      </table>
      
      <div class="Aviso">
        <b class="Dz">Aviso Importante</b>
        <p class="Dz" align="justify">
          La información mostrada en este reporte ha sido proporcionada directamente por las entidades supervisadas. Por consiguiente, la Superintendencia de Banca, Seguros y Administradoras Privadas de Fondos de Pensiones no es responsable por la veracidad y corrección de los montos y calificaciones de las deudas informadas.
        </p>
      </div>
      
      <!-- PIE DE PAGINA -->
      
        
        
      



<p class="Dz Izq">1: A partir de la información de septiembre-2005, la distribución porcentual de las obligaciones de acuerdo a las clasificaciones incluye los saldos de los créditos castigados</p>

<p>
  <img src="images/logo-sbs.jpg" alt="SBS - Central de Riesgos">
</p>
    </div>
  
</body>


Asi es mi pages/sbs/riesgos_page.py
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select, WebDriverWait
from selenium.common.exceptions import TimeoutException, NoSuchElementException
import logging

from pages.base_page import BasePage


class RiesgosPage(BasePage):
    MENU = (By.ID, "Menu")

    # Tabs (cuando están habilitados son <a id="...">)
    TAB_HISTORICA = (By.ID, "idOp2")
    TAB_DETALLADA = (By.ID, "idOp4")
    TAB_OTROS_REPORTES = (By.ID, "idOp6")

    # Versión "deshabilitada" (cuando sale como <span>Detallada</span>)
    SPAN_DETALLADA = (By.XPATH, "//div[@id='Menu']//span[normalize-space()='Detallada']")
    SPAN_HISTORICA = (By.XPATH, "//div[@id='Menu']//span[normalize-space()='Historica' or normalize-space()='Histórica']")
    SPAN_OTROS_REPORTES = (By.XPATH, "//div[@id='Menu']//span[normalize-space()='Otros Reportes']")

    CONTENIDO = (By.ID, "Contenido")

    # Otros Reportes
    OTROS_LIST = (By.ID, "OtrosReportes")
    LNK_CARTERAS_TRANSFERIDAS = (
        By.XPATH,
        "//ul[@id='OtrosReportes']//a[normalize-space()='Carteras Transferidas']"
    )

    # Tabla contenedora "Otros Reportes"
    TBL_OTROS_REPORTES = (
        By.XPATH,
        "//table[contains(@class,'Crw')][.//span[normalize-space()='Otros Reportes']]"
    )

    # Carteras
    TBL_CARTERAS = (By.CSS_SELECTOR, "table#expand.Crw")
    ARROWS = (By.CSS_SELECTOR, "table#expand div.arrow[title*='Rectificaciones']")

    LINK_MODULO = (
        By.CSS_SELECTOR,
        "a.descripcion[onclick*=\"/criesgos/criesgos/criesgos.jsp\"]"
    )

    SELECT_TIPO_DOC = (By.ID, "as_tipo_doc")
    INPUT_DOC = (By.CSS_SELECTOR, "input[name='as_doc_iden']")
    BTN_CONSULTAR = (By.ID, "btnConsultar")

    TBL_DATOS_DEUDOR = (
        By.XPATH,
        "//table[contains(@class,'Crw')][.//b[contains(@class,'F') and contains(normalize-space(.),'Datos del Deudor')]]"
    )
    TBL_POSICION = (
        By.XPATH,
        "//table[contains(@class,'Crw')][.//span[contains(@class,'F') and contains(normalize-space(.),'Posición Consolidada del Deudor')]]"
    )

    LINK_SALIR_CRIESGOS = (By.CSS_SELECTOR, "a[href*='/criesgos/logout?c_c_producto=00002']")
    BTN_SALIR_PORTAL = (By.CSS_SELECTOR, "a[onclick*=\"goTo('logout')\"]")

    # ---------------- navegación básica ----------------

    def open_modulo_deuda(self):
        link = self.wait.until(EC.element_to_be_clickable(self.LINK_MODULO))
        link.click()

    def consultar_por_dni(self, dni: str):
        sel = self.wait.until(EC.presence_of_element_located(self.SELECT_TIPO_DOC))
        Select(sel).select_by_value("11")

        inp = self.wait.until(EC.element_to_be_clickable(self.INPUT_DOC))
        inp.click()
        inp.clear()
        inp.send_keys(dni)

        btn = self.wait.until(EC.element_to_be_clickable(self.BTN_CONSULTAR))
        btn.click()

        # ✅ si SBS muestra un JS alert, aceptarlo y continuar
        alert_txt = self.accept_alert_if_present(timeout=2.5)
        if alert_txt:
            logging.warning("[SBS] Alert al consultar DNI=%s: %s", dni, alert_txt)

    # ---------------- detección de modo ----------------

    def detallada_habilitada(self) -> bool:
        """
        'Detallada' está habilitada si existe <a id="idOp4">.
        Si aparece como <span>Detallada</span>, está deshabilitada.
        """
        try:
            return len(self.driver.find_elements(*self.TAB_DETALLADA)) > 0
        except Exception:
            return False

    def historica_habilitada(self) -> bool:
        """Histórica habilitada si existe <a id="idOp2">."""
        try:
            return len(self.driver.find_elements(*self.TAB_HISTORICA)) > 0
        except Exception:
            return False

    # ---------------- tabs ----------------

    def go_historica(self):
        self.wait.until(EC.presence_of_element_located(self.MENU))
        self.wait.until(EC.element_to_be_clickable(self.TAB_HISTORICA)).click()
        self._wait_tab_loaded()
        WebDriverWait(self.driver, 6).until(EC.presence_of_element_located(self.CONTENIDO))

    def go_detallada(self):
        self.wait.until(EC.presence_of_element_located(self.MENU))
        self.wait.until(EC.element_to_be_clickable(self.TAB_DETALLADA)).click()
        self._wait_tab_loaded()

    def go_otros_reportes(self):
        self.wait.until(EC.presence_of_element_located(self.MENU))
        self.wait.until(EC.element_to_be_clickable(self.TAB_OTROS_REPORTES)).click()

        # Espera corta: el contenido/tablas aparecen rápido
        short_wait = WebDriverWait(self.driver, 6)
        short_wait.until(EC.presence_of_element_located(self.CONTENIDO))
        short_wait.until(EC.presence_of_element_located(self.TBL_OTROS_REPORTES))

    # ===================== CLAVE: no demorar si no hay info =====================

    def otros_reportes_disponible(self) -> bool:
        """
        Determina si existen opciones en "Otros Reportes" SIN usar waits largos.
        - Si el texto contiene 'No existe información...' => False inmediato
        - Si existe UL#OtrosReportes => True
        """
        short_wait = WebDriverWait(self.driver, 3)

        try:
            tbl = short_wait.until(EC.presence_of_element_located(self.TBL_OTROS_REPORTES))
        except TimeoutException:
            return False

        txt = " ".join((tbl.text or "").split()).lower()
        if "no existe información en otros reportes" in txt:
            return False

        return len(tbl.find_elements(By.ID, "OtrosReportes")) > 0

    def click_carteras_transferidas(self) -> bool:
        """
        No bloqueante y SIN waits largos.
        - Si no hay info => return False inmediato
        - Si hay lista, intenta click con wait corto y valida carga con wait corto
        """
        short_wait = WebDriverWait(self.driver, 3)

        try:
            tbl = short_wait.until(EC.presence_of_element_located(self.TBL_OTROS_REPORTES))
        except TimeoutException:
            return False

        txt = " ".join((tbl.text or "").split()).lower()
        if "no existe información en otros reportes" in txt:
            return False

        ul_list = tbl.find_elements(By.ID, "OtrosReportes")
        if not ul_list:
            return False

        ul = ul_list[0]
        links = ul.find_elements(By.XPATH, ".//a[normalize-space()='Carteras Transferidas']")
        if not links:
            return False

        try:
            links[0].click()
        except Exception:
            self.driver.execute_script("arguments[0].click();", links[0])

        short_wait2 = WebDriverWait(self.driver, 5)

        def _loaded(d):
            try:
                if d.find_elements(*self.TBL_CARTERAS):
                    return True
            except Exception:
                pass
            try:
                return "buscarinfocarterastransferidas" in (d.current_url or "")
            except Exception:
                return False

        try:
            short_wait2.until(_loaded)
            short_wait2.until(EC.presence_of_element_located(self.CONTENIDO))
            return True
        except TimeoutException:
            return False

    def has_carteras_table(self) -> bool:
        try:
            self.driver.find_element(*self.TBL_CARTERAS)
            return True
        except NoSuchElementException:
            return False

    def expand_all_rectificaciones(self, expected: int = 2):
        arrows = self.driver.find_elements(*self.ARROWS)
        if not arrows:
            return

        for arrow in arrows[:expected]:
            try:
                self.driver.execute_script("arguments[0].scrollIntoView({block:'center'});", arrow)
                try:
                    arrow.click()
                except Exception:
                    self.driver.execute_script("arguments[0].click();", arrow)

                def _expanded(d):
                    try:
                        master_tr = arrow.find_element(By.XPATH, "./ancestor::tr[contains(@class,'master')]")
                        detail_tr = master_tr.find_element(
                            By.XPATH, "following-sibling::tr[contains(@class,'Vde')][1]"
                        )
                        style = (detail_tr.get_attribute("style") or "").lower()
                        return "display: none" not in style
                    except Exception:
                        return True

                try:
                    WebDriverWait(self.driver, 3).until(_expanded)
                except Exception:
                    pass
            except Exception:
                continue

    def screenshot_contenido(self, out_path):
        short_wait = WebDriverWait(self.driver, 4)
        try:
            el = short_wait.until(EC.presence_of_element_located(self.CONTENIDO))
            self.driver.execute_script("arguments[0].scrollIntoView({block:'start'});", el)
            try:
                el.screenshot(str(out_path))
            except Exception:
                self.driver.save_screenshot(str(out_path))
        except TimeoutException:
            self.driver.save_screenshot(str(out_path))

    def _wait_tab_loaded(self):
        self.wait.until(lambda d: d.execute_script("return document.readyState") == "complete")

    # ---------------- extracción ----------------

    def extract_datos_deudor(self) -> dict:
        alert_txt = self.accept_alert_if_present(timeout=0.6)
        if alert_txt:
            logging.warning("[SBS] Alert previo a extraer Datos del Deudor: %s", alert_txt)

        tbl = self.wait.until(EC.presence_of_element_located(self.TBL_DATOS_DEUDOR))
        rows = tbl.find_elements(By.CSS_SELECTOR, "tbody tr")
        data = {}
        for r in rows:
            tds = r.find_elements(By.CSS_SELECTOR, "td")
            if len(tds) < 2:
                continue
            i = 0
            while i < len(tds) - 1:
                label_el = None
                try:
                    label_el = tds[i].find_element(By.CSS_SELECTOR, "b.Dz")
                except Exception:
                    pass
                label = (label_el.text.strip() if label_el else tds[i].text.strip())
                value_text = tds[i + 1].text.strip()
                if label:
                    label = " ".join(label.split())
                    value_text = " ".join(value_text.split())
                    data[label] = value_text
                i += 2
        return data

    def extract_posicion_consolidada(self) -> list:
        alert_txt = self.accept_alert_if_present(timeout=0.6)
        if alert_txt:
            logging.warning("[SBS] Alert previo a extraer Posición Consolidada: %s", alert_txt)

        tbl = self.wait.until(EC.presence_of_element_located(self.TBL_POSICION))
        trs = tbl.find_elements(By.CSS_SELECTOR, "tbody tr")
        out = []
        for tr in trs:
            tds = tr.find_elements(By.CSS_SELECTOR, "td")
            if len(tds) != 4:
                continue
            row = [" ".join(td.text.split()) for td in tds]
            if row[0].upper() == "SALDOS":
                continue
            out.append(row)
        return out

    # ---------------- logout ----------------

    def logout_modulo(self):
        link = self.wait.until(EC.element_to_be_clickable(self.LINK_SALIR_CRIESGOS))
        link.click()
        self.wait.until(EC.presence_of_element_located(self.BTN_SALIR_PORTAL))

    def logout_portal(self):
        btn = self.wait.until(EC.element_to_be_clickable(self.BTN_SALIR_PORTAL))
        btn.click()



services/sbs_flow.py
from pathlib import Path
import logging

from pages.sbs.cerrar_sesiones_page import CerrarSesionesPage
from pages.sbs.login_page import LoginPage
from pages.sbs.riesgos_page import RiesgosPage
from pages.copilot_page import CopilotPage
from services.copilot_service import CopilotService
from config.settings import URL_LOGIN


class SbsFlow:
    # Pre-step debe correr SOLO una vez por ejecución del proceso
    _prestep_done: bool = False

    def __init__(self, driver, usuario: str, clave: str):
        self.driver = driver
        self.usuario = usuario
        self.clave = clave

    def _pre_cerrar_sesion_activa(self):
        logging.info("[SBS] Pre-step: cerrar sesión activa (cerrarSesiones.jsf)")

        try:
            self.driver.delete_all_cookies()
        except Exception:
            pass

        page = CerrarSesionesPage(self.driver)
        page.open()
        outcome = page.cerrar_sesion(self.usuario, self.clave)

        if outcome == "CERRADA":
            logging.info("[SBS] Pre-step OK: sesión activa cerrada")
        else:
            logging.info("[SBS] Pre-step OK: no existían sesiones activas (continuar)")

    def run(
        self,
        dni: str,
        captcha_img_path: Path,
        detallada_img_path: Path,
        otros_img_path: Path,
    ) -> dict:
        # ==========================================================
        # 0) PRE-STEP: Cerrar sesión activa (SOLO 1 VEZ POR EJECUCIÓN)
        # ==========================================================
        if not SbsFlow._prestep_done:
            try:
                self._pre_cerrar_sesion_activa()
                SbsFlow._prestep_done = True
            except Exception as e:
                logging.warning("[SBS] Pre-step cerrar sesión falló, se continúa igual. Detalle=%r", e)
        else:
            logging.info("[SBS] Pre-step omitido (ya se ejecutó en esta corrida)")

        # ==========================================================
        # 1) Flujo SBS normal
        # ==========================================================
        logging.info("[SBS] Ir a login")
        self.driver.get(URL_LOGIN)

        login_page = LoginPage(self.driver)

        logging.info("[SBS] Capturar captcha")
        login_page.capture_image(captcha_img_path)

        # --- Copilot en nueva pestaña (y luego cerrarla) ---
        original_handle = self.driver.current_window_handle
        self.driver.switch_to.new_window("tab")
        copilot = CopilotService(CopilotPage(self.driver))

        logging.info("[SBS] Resolver captcha con Copilot")
        captcha = copilot.resolve_captcha(captcha_img_path)

        try:
            self.driver.close()
        except Exception:
            pass
        self.driver.switch_to.window(original_handle)

        logging.info("[SBS] Login (usuario=%s) + ingresar captcha", self.usuario)
        login_page.fill_form(self.usuario, self.clave, captcha)

        riesgos = RiesgosPage(self.driver)

        logging.info("[SBS] Abrir módulo deuda")
        riesgos.open_modulo_deuda()

        logging.info("[SBS] Consultar DNI=%s", dni)
        riesgos.consultar_por_dni(dni)

        # ==========================================================
        # 1.A) CAMINO "HISTORICA" (Detallada NO habilitada)
        # ==========================================================
        if (not riesgos.detallada_habilitada()) and riesgos.historica_habilitada():
            logging.warning("[SBS] Modo HISTORICA: Detallada no está habilitada. Se captura evidencia y se continúa sin Posición Consolidada.")

            # Asegurar que estamos en Histórica
            try:
                riesgos.go_historica()
            except Exception:
                pass

            # En Histórica sí existe "Datos del Deudor" -> lo extraemos si se puede
            datos_deudor = {}
            try:
                datos_deudor = riesgos.extract_datos_deudor()
            except Exception as e:
                logging.warning("[SBS] No se pudo extraer Datos del Deudor en Histórica: %r", e)

            # Evidencia principal (guardamos en detallada_img_path aunque sea Histórica)
            try:
                riesgos.screenshot_contenido(str(detallada_img_path))
            except Exception:
                try:
                    self.driver.save_screenshot(str(detallada_img_path))
                except Exception:
                    pass

            # Intentar Otros Reportes (puede estar habilitado)
            try:
                riesgos.go_otros_reportes()
                # misma lógica no bloqueante
                disponible = riesgos.otros_reportes_disponible()
                logging.info("[SBS] Otros Reportes disponible=%s (modo Histórica)", disponible)

                loaded = riesgos.click_carteras_transferidas()
                logging.info("[SBS] Carteras Transferidas loaded=%s (modo Histórica)", loaded)

                if loaded and riesgos.has_carteras_table():
                    riesgos.expand_all_rectificaciones(expected=2)

                riesgos.screenshot_contenido(str(otros_img_path))
            except Exception as e:
                logging.warning("[SBS] Otros Reportes falló en modo Histórica: %r", e)
                try:
                    riesgos.screenshot_contenido(str(otros_img_path))
                except Exception:
                    pass

            # Logout y retorno parcial
            try:
                logging.info("[SBS] Logout módulo")
                riesgos.logout_modulo()
                logging.info("[SBS] Logout portal")
                riesgos.logout_portal()
            except Exception:
                pass

            logging.info("[SBS] Fin flujo OK (modo HISTORICA)")
            return {
                "datos_deudor": datos_deudor,
                "posicion": [],          # no hay posición consolidada
                "modo": "HISTORICA",
            }

        # ==========================================================
        # 1.B) CAMINO NORMAL (Consolidado)
        # ==========================================================
        logging.info("[SBS] Extraer datos (modo normal)")
        datos_deudor = riesgos.extract_datos_deudor()
        posicion = riesgos.extract_posicion_consolidada()

        logging.info("[SBS] Ir a Detallada + screenshot")
        riesgos.go_detallada()
        self.driver.save_screenshot(str(detallada_img_path))

        logging.info("[SBS] Ir a Otros Reportes")
        riesgos.go_otros_reportes()

        logging.info("[SBS] Intentar Carteras Transferidas (no bloqueante)")
        try:
            disponible = riesgos.otros_reportes_disponible()
            logging.info("[SBS] Otros Reportes disponible=%s", disponible)

            loaded = riesgos.click_carteras_transferidas()
            logging.info("[SBS] Carteras Transferidas loaded=%s", loaded)

            if loaded and riesgos.has_carteras_table():
                logging.info("[SBS] Expandir rectificaciones (si hay)")
                riesgos.expand_all_rectificaciones(expected=2)

            logging.info("[SBS] Screenshot contenido (rápido + fallback)")
            riesgos.screenshot_contenido(str(otros_img_path))

        except Exception as e:
            logging.exception("[SBS] Error en Otros Reportes/Carteras: %r", e)
            riesgos.screenshot_contenido(str(otros_img_path))

        logging.info("[SBS] Logout módulo")
        riesgos.logout_modulo()

        logging.info("[SBS] Logout portal")
        riesgos.logout_portal()

        logging.info("[SBS] Fin flujo OK (modo normal)")
        return {
            "datos_deudor": datos_deudor,
            "posicion": posicion,
            "modo": "NORMAL",
        }


main.py
from __future__ import annotations
from pathlib import Path
import os
import sys
import tempfile
import logging
from typing import Optional, Tuple

from config.settings import *
from config.credentials_store import load_sbs_credentials
from config.analyst_store import load_matanalista

from infrastructure.edge_debug import EdgeDebugLauncher
from infrastructure.selenium_driver import SeleniumDriverFactory
from services.sbs_flow import SbsFlow
from services.sunat_flow import SunatFlow
from services.rbm_flow import RbmFlow
from services.xlsm_session_writer import XlsmSessionWriter
from utils.logging_utils import setup_logging
from utils.decorators import log_exceptions


def _get_app_dir() -> Path:
    """Carpeta del exe (PyInstaller) o del proyecto (script)."""
    if getattr(sys, "frozen", False):
        return Path(sys.executable).resolve().parent
    return Path(__file__).resolve().parent


def _pick_results_dir(app_dir: Path) -> Path:
    """
    Intenta crear ./results al lado del exe/script.
    Si falla por permisos, usa %LOCALAPPDATA%/PrismaProject/results (o TEMP).
    """
    primary = app_dir / "results"
    try:
        primary.mkdir(parents=True, exist_ok=True)
        test_file = primary / ".write_test"
        test_file.write_text("ok", encoding="utf-8")
        test_file.unlink(missing_ok=True)
        return primary
    except Exception:
        base = Path(os.environ.get("LOCALAPPDATA", tempfile.gettempdir()))
        fallback = base / "PrismaProject" / "results"
        fallback.mkdir(parents=True, exist_ok=True)
        return fallback


def _safe_filename_part(s: str) -> str:
    """
    Limpia caracteres problemáticos para nombre de archivo (Windows).
    """
    s = (s or "").strip()
    invalid = '<>:"/\\|?*'
    for ch in invalid:
        s = s.replace(ch, "")
    # reduce espacios
    s = s.replace(" ", "")
    return s


@log_exceptions
def run_app(
    dni_titular: str,
    dni_conyuge: Optional[str] = None,
    numoportunidad: Optional[str] = None,
    producto: Optional[str] = None,
    desproducto: Optional[str] = None,
) -> Tuple[Path, Path]:
    app_dir = _get_app_dir()
    setup_logging(app_dir)

    logging.info("=== INICIO EJECUCION ===")
    logging.info("DNI_TITULAR=%s | DNI_CONYUGE=%s", dni_titular, dni_conyuge or "")
    logging.info(
        "NUMOPORTUNIDAD=%s | PRODUCTO=%s | DESPRODUCTO=%s",
        numoportunidad or "",
        producto or "",
        desproducto or "",
    )
    logging.info("APP_DIR=%s", app_dir)

    # Aunque UI/Controller validen, protegemos el motor
    if not (numoportunidad or "").strip():
        raise ValueError("NUMOPORTUNIDAD es obligatorio.")
    if not (producto or "").strip():
        raise ValueError("PRODUCTO es obligatorio.")
    if not (desproducto or "").strip():
        raise ValueError("DESPRODUCTO es obligatorio.")

    # ====== MATANALISTA persistente (obligatorio) ======
    matanalista = load_matanalista("").strip()
    if not matanalista:
        raise ValueError("No hay MATANALISTA configurado. Ve al botón 'MATANALISTA' y guárdalo.")
    logging.info("MATANALISTA runtime=%s", matanalista)

    # ====== CREDENCIALES SBS DESDE GUI (LOCALAPPDATA) ======
    sbs_user, sbs_pass = load_sbs_credentials("", "")
    if not sbs_user or not sbs_pass:
        raise ValueError("No hay credenciales SBS configuradas. Ve a 'Credenciales SBS' y guárdalas.")
    logging.info("SBS user runtime=%s", sbs_user)

    launcher = EdgeDebugLauncher()
    launcher.ensure_running()
    driver = SeleniumDriverFactory.create()

    try:
        macro_path = app_dir / "Macro.xlsm"
        if not macro_path.exists():
            raise FileNotFoundError(f"No se encontró Macro.xlsm junto al ejecutable: {macro_path}")

        results_dir = _pick_results_dir(app_dir)

        dni_conyuge = (dni_conyuge or "").strip() or None

        safe_numop = _safe_filename_part(numoportunidad)
        safe_mat = _safe_filename_part(matanalista)
        out_xlsm = results_dir / f"{safe_numop}_{safe_mat}.xlsm"

        captcha_img_path = results_dir / "captura.png"
        detallada_img_path = results_dir / "detallada.png"
        otros_img_path = results_dir / "otros_reportes.png"

        captcha_img_cony_path = results_dir / "sbs_captura_conyuge.png"
        detallada_img_cony_path = results_dir / "sbs_detallada_conyuge.png"
        otros_img_cony_path = results_dir / "sbs_otros_reportes_conyuge.png"

        sunat_img_path = results_dir / "sunat_panel.png"

        rbm_consumos_img_path = results_dir / "rbm_consumos.png"
        rbm_cem_img_path = results_dir / "rbm_cem.png"

        rbm_consumos_cony_path = results_dir / "rbm_consumos_conyuge.png"
        rbm_cem_cony_path = results_dir / "rbm_cem_conyuge.png"

        logging.info("RESULTS_DIR=%s", results_dir)
        logging.info("OUTPUT_XLSM=%s", out_xlsm)

        # ==========================================================
        # 1) SBS TITULAR
        # ==========================================================
        logging.info("== FLUJO SBS (TITULAR) INICIO ==")
        _ = SbsFlow(driver, sbs_user, sbs_pass).run(
            dni=dni_titular,
            captcha_img_path=captcha_img_path,
            detallada_img_path=detallada_img_path,
            otros_img_path=otros_img_path,
        )
        logging.info("== FLUJO SBS (TITULAR) FIN ==")

        # ==========================================================
        # 1.1) SBS CONYUGE (opcional)
        # ==========================================================
        if dni_conyuge:
            logging.info("== FLUJO SBS (CONYUGE) INICIO ==")
            _ = SbsFlow(driver, sbs_user, sbs_pass).run(
                dni=dni_conyuge,
                captcha_img_path=captcha_img_cony_path,
                detallada_img_path=detallada_img_cony_path,
                otros_img_path=otros_img_cony_path,
            )
            logging.info("== FLUJO SBS (CONYUGE) FIN ==")

        # ==========================================================
        # 2) SUNAT TITULAR
        # ==========================================================
        logging.info("== FLUJO SUNAT (TITULAR) INICIO ==")
        try:
            driver.delete_all_cookies()
        except Exception:
            pass
        SunatFlow(driver).run(dni=dni_titular, out_img_path=sunat_img_path)
        logging.info("== FLUJO SUNAT (TITULAR) FIN ==")

        # ==========================================================
        # 3) RBM TITULAR
        # ==========================================================
        logging.info("== FLUJO RBM (TITULAR) INICIO ==")
        rbm_titular = RbmFlow(driver).run(
            dni=dni_titular,
            consumos_img_path=rbm_consumos_img_path,
            cem_img_path=rbm_cem_img_path,
            numoportunidad=numoportunidad,
            producto=producto,
            desproducto=desproducto,
        )
        rbm_inicio_tit = rbm_titular.get("inicio", {}) if isinstance(rbm_titular, dict) else {}
        rbm_cem_tit = rbm_titular.get("cem", {}) if isinstance(rbm_titular, dict) else {}
        rbm_scores_tit = rbm_titular.get("scores", {}) if isinstance(rbm_titular, dict) else {}
        logging.info("RBM titular inicio=%s", rbm_inicio_tit)
        logging.info("RBM titular cem=%s", rbm_cem_tit)
        logging.info("RBM titular scores=%s", rbm_scores_tit)
        logging.info("== FLUJO RBM (TITULAR) FIN ==")

        # ==========================================================
        # 4) RBM CONYUGE (opcional, en nueva pestaña)
        # ==========================================================
        rbm_inicio_cony = {}
        rbm_cem_cony = {}
        if dni_conyuge:
            logging.info("== FLUJO RBM (CONYUGE) INICIO ==")
            original_handle_rbm = driver.current_window_handle
            rbm_conyuge = {}
            try:
                driver.switch_to.new_window("tab")
                rbm_conyuge = RbmFlow(driver).run(
                    dni=dni_conyuge,
                    consumos_img_path=rbm_consumos_cony_path,
                    cem_img_path=rbm_cem_cony_path,
                    numoportunidad=numoportunidad,
                    producto=producto,
                    desproducto=desproducto,
                )
            finally:
                try:
                    driver.close()
                except Exception:
                    pass
                try:
                    driver.switch_to.window(original_handle_rbm)
                except Exception:
                    pass

            rbm_inicio_cony = rbm_conyuge.get("inicio", {}) if isinstance(rbm_conyuge, dict) else {}
            rbm_cem_cony = rbm_conyuge.get("cem", {}) if isinstance(rbm_conyuge, dict) else {}
            logging.info("RBM conyuge inicio=%s", rbm_inicio_cony)
            logging.info("RBM conyuge cem=%s", rbm_cem_cony)
            logging.info("== FLUJO RBM (CONYUGE) FIN ==")

        # ==========================================================
        # 5) Escribir todo en XLSM
        #    (AHORA SÍ escribimos Inicio!C4 + scores C14/C83 según producto)
        # ==========================================================
        logging.info("== ESCRITURA XLSM INICIO ==")

        cem_row_map = [
            ("hipotecario", 26),
            ("cef", 27),
            ("vehicular", 28),
            ("pyme", 29),
            ("comercial", 30),
            ("deuda_indirecta", 31),
            ("tarjeta", 32),
            ("linea_no_utilizada", 33),
        ]

        # Mapeo GUI PRODUCTO -> texto exacto en Inicio!C4
        producto_excel_c4 = None
        p = (producto or "").strip().upper()
        if p == "CREDITO EFECTIVO":
            producto_excel_c4 = "Credito Efectivo"
        elif p == "TARJETA DE CREDITO":
            producto_excel_c4 = "Tarjeta de Credito"

        with XlsmSessionWriter(macro_path) as writer:
            # NUEVO: C4 "selección" en plantilla
            if producto_excel_c4:
                writer.write_cell("Inicio", "C4", producto_excel_c4)

            # Campos Inicio existentes
            writer.write_cell("Inicio", "C11", rbm_inicio_tit.get("segmento"))
            writer.write_cell("Inicio", "C12", rbm_inicio_tit.get("segmento_riesgo"))
            writer.write_cell("Inicio", "C13", rbm_inicio_tit.get("pdh"))
            writer.write_cell("Inicio", "C15", rbm_inicio_tit.get("score_rcc"))

            # NUEVO: scores RBM a Inicio!C14 y C83
            if rbm_scores_tit.get("inicio_c14") is not None:
                writer.write_cell("Inicio", "C14", rbm_scores_tit.get("inicio_c14"))
            if rbm_scores_tit.get("inicio_c83") is not None:
                writer.write_cell("Inicio", "C83", rbm_scores_tit.get("inicio_c83"))

            # CEM titular
            for key, row in cem_row_map:
                item = rbm_cem_tit.get(key, {}) or {}
                writer.write_cell("Inicio", f"C{row}", item.get("cuota_bcp", 0))
                writer.write_cell("Inicio", f"D{row}", item.get("cuota_sbs", 0))
                writer.write_cell("Inicio", f"E{row}", item.get("saldo_sbs", 0))

            # Conyuge (si aplica)
            if dni_conyuge:
                writer.write_cell("Inicio", "D11", rbm_inicio_cony.get("segmento"))
                writer.write_cell("Inicio", "D12", rbm_inicio_cony.get("segmento_riesgo"))
                writer.write_cell("Inicio", "D15", rbm_inicio_cony.get("score_rcc"))

                for key, row in cem_row_map:
                    item = rbm_cem_cony.get(key, {}) or {}
                    writer.write_cell("Inicio", f"G{row}", item.get("cuota_bcp", 0))
                    writer.write_cell("Inicio", f"H{row}", item.get("cuota_sbs", 0))
                    writer.write_cell("Inicio", f"I{row}", item.get("saldo_sbs", 0))

            # Imágenes
            writer.add_image_to_range("SBS", detallada_img_path, "C64", "Z110")
            writer.add_image_to_range("SBS", otros_img_path, "C5", "Z50")
            if dni_conyuge:
                writer.add_image_to_range("SBS", detallada_img_cony_path, "AI64", "AY110")
                writer.add_image_to_range("SBS", otros_img_cony_path, "AI5", "AY50")

            writer.add_image_to_range("SUNAT", sunat_img_path, "C5", "O51")

            writer.add_image_to_range("RBM", rbm_consumos_img_path, "C5", "Z50")
            writer.add_image_to_range("RBM", rbm_cem_img_path, "C64", "Z106")
            if dni_conyuge:
                writer.add_image_to_range("RBM", rbm_consumos_cony_path, "AI5", "AY50")
                writer.add_image_to_range("RBM", rbm_cem_cony_path, "AI64", "AY106")

            writer.save(out_xlsm)

        logging.info("== ESCRITURA XLSM FIN ==")
        logging.info("XLSM final generado: %s", out_xlsm.resolve())
        logging.info("Evidencias guardadas en: %s", results_dir.resolve())
        print(f"XLSM final generado: {out_xlsm.resolve()}")
        print(f"Evidencias guardadas en: {results_dir.resolve()}")

        return out_xlsm, results_dir

    finally:
        try:
            driver.quit()
        except Exception:
            pass
        try:
            launcher.close()
        except Exception:
            pass

        logging.info("=== FIN EJECUCION ===")
        logging.shutdown()


@log_exceptions
def main():
    run_app(DNI_CONSULTA, DNI_CONYUGE_CONSULTA)


if __name__ == "__main__":
    main()
