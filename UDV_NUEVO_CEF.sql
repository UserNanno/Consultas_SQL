<body>
  
  	
	<div class="container">
		<div class="header hidden" id="divHeader">
			<div class="col-md-10 col-md-offset-1">
	        	<img src="/a/imagenes/logo_2015.png" class="imgLogo">
			</div>
	    </div>
	    <div class="row">	
	        <div class="col-md-10 col-md-offset-1">
	        	<div>
	        		<h1>Consulta RUC</h1>
	        	</div>
	        	<div class="divBotoneraArriba text-center">				 
	        		 
					<button type="button" class="btn btn-danger btnNuevaConsulta">Volver</button>
	        	</div> 
			    <div class="panel panel-primary">
				  <div class="panel-heading">Relación de contribuyentes</div>
				  <div class="list-group">
					  
					  
						
						
						
									<a href="#" class="list-group-item clearfix aRucs" data-ruc="10788016005">
									    <h4 class="list-group-item-heading">RUC: 10788016005</h4>
									    <h4 class="list-group-item-heading">CANECILLAS CONTRERAS JUAN MARIANO</h4>
									    <p class="list-group-item-text">Ubicación: LIMA</p>
									    
									    
									    <p class="list-group-item-text">Estado: <strong><span class="text-success">ACTIVO</span></strong></p>
									    
									    <span class="glyphicon glyphicon-chevron-right pull-right" aria-hidden="true"></span>
									    
									  </a>
						
						
					
					  
					</div>
				  
				   
				  
				  
				  <div class="panel-footer text-center">
				  	<small>Fecha consulta: 08/01/2026 16:09</small>
				  </div><!-- fin footer del panel -->
				</div><!--fin panel-->
				
				
				<div class="text-center divBotonera">	        		 
	        		  <a href="FrameCriterioBusquedaWeb.jsp" class="hidden" id="aNuevaConsulta">Volver</a>					 
					<button type="button" class="btn btn-danger btnNuevaConsulta">Volver</button>
	        	</div>
				
	         </div><!--fin col-->
	    </div><!--fin row-->
		<footer class="footer text-center">
			<div class="col-md-10 col-md-offset-1">
				<p><small>© 1997 - 2026 SUNAT Derechos Reservados</small></p>
			</div>
		</footer>
	</div><!--fin container--> 
	
	
	
  
	<form action="/cl-ti-itmrconsruc/jcrS00Alias" method="post" name="selecXNroRuc">
		<input type="hidden" name="accion" value="consPorRuc">
		<input type="hidden" name="actReturn" value="1">
		<input type="hidden" name="nroRuc" value="">
		<input type="hidden" name="numRnd" value="1413385332">			
		<input type="hidden" name="modo" value="1">
	</form>	    
	


    <script src="/a/js/libs/jquery/1.11.1/jquery.min.js"></script>
    <script src="/a/js/libs/bootstrap/3.3.1/js/bootstrap.min.js"></script>
    <script>
    	$( document ).ready(function() {
    		$.ajaxSetup({ scriptCharset: "utf-8" , contentType: "application/json; charset=utf-8"});
    	    jQuery.support.cors = true;

    	    iniciaVariables();
    	    iniciaBotones();   
    	});


    	
    	 
    	 
    	/*Funciones usadas por la aplicación*/
    	function iniciaBotones(){
    	    //resetea estado inicial del formulario
    	   

    	    aRucs.bind('click',function(event){
				var dataRuc=$(this).attr("data-ruc");
				sendNroRuc(dataRuc);
				
				
				event.preventDefault();
                event.stopImmediatePropagation();
                return false;
    	    });    

    	    $(".btnNuevaConsulta").bind('click',function(event){
    	    	regresa();        		
        		
        		event.preventDefault();
        		event.stopImmediatePropagation();
        		return false;
        	});  
    	    
    	}
    	
    	function regresa(){
    		var href = $("#aNuevaConsulta").attr('href');
    		window.location.href = href;
    	}
    	function irHome(){
    		regresa();
    	}
    	
    	function sendNroRuc(cc){
    	    document.selecXNroRuc.nroRuc.value = cc;
    		document.selecXNroRuc.submit();
    	}
    	

    	/*Variables del app*/

    	
    	/*Declarar variables*/
    	var aRucs=null;
    	

    	/*Iniciar variables*/
    	function iniciaVariables(){
    		aRucs=$(".aRucs");
    	}
    	
    	
    	
    	
    	
	  
	</script>
	
    <script src="/a/js/apps/workspace/ws.js"></script>
  
</body>
