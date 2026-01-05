Todo salio perfecto.

Aca tengo otro detalle, la pagina que estoy consultando actualmente es solo 1 de 3.

Deberiamos reorganizar nuestro codigo, archivos? 

Quiero ahora que entre a otra pagina que es: https://e-consultaruc.sunat.gob.pe/cl-ti-itmrconsruc/FrameCriterioBusquedaWeb.jsp y dentro dar click al boton btnPorDocumento

<div role="group" class="btn-group">
						      <button id="btnPorRuc" class="btn btn-default btnPor active" type="button">Por RUC</button>
						      <button id="btnPorDocumento" class="btn btn-default btnPor" type="button">Por Documento</button>
						      <button id="btnPorRazonSocial" class="btn btn-default btnPor" type="button">Por Nomb./Raz.Soc.</button>
						    </div>

Donde se mostrará otras opciones como: 

<div class="col-sm-8 col-sm-offset-2 divConsultaCampo" id="divTipoDoc1">
	                            	<select name="tipdoc" id="cmbTipoDoc" class="form-control cmbTipo">				
										 <option value="1" selected="">Documento Nacional de Identidad</option>
										 <option value="4">Carnet de Extranjeria</option>					
										 <option value="7">Pasaporte</option>
										 <option value="A">Ced. Diplomática de Identidad</option>
									</select> 
	                            </div>


Donde tenemos que escoger Documento Nacional de Identiddad (DNI). Posterior digitar el DNI en 
<div class="col-sm-8 col-sm-offset-2 divConsultaCampo" id="divTipoDoc2">
	                             	<input type="text" class="form-control" id="txtNumeroDocumento" name="search2" placeholder="Ingrese Número documento" tabindex="1"><!-- maxlength="16" -->
	                            </div>

Para luego dar click en el boton Buscar:
<div class="col-sm-8 col-sm-offset-2 divConsultaCampo" id="divTipoDoc2">
	                             	<input type="text" class="form-control" id="txtNumeroDocumento" name="search2" placeholder="Ingrese Número documento" tabindex="1"><!-- maxlength="16" -->
	                            </div>


Una vez dado buscar, se cambiará la URL de la pagina a: https://e-consultaruc.sunat.gob.pe/cl-ti-itmrconsruc/jcrS00Alias

Aca podremos visularizar esto:

<div class="list-group">
					  
					  
						
						
						
									<a href="#" class="list-group-item clearfix aRucs" data-ruc="10788016005">
									    <h4 class="list-group-item-heading">RUC: 10788016005</h4>
									    <h4 class="list-group-item-heading">CANECILLAS CONTRERAS JUAN MARIANO</h4>
									    <p class="list-group-item-text">Ubicación: LIMA</p>
									    
									    
									    <p class="list-group-item-text">Estado: <strong><span class="text-success">ACTIVO</span></strong></p>
									    
									    <span class="glyphicon glyphicon-chevron-right pull-right" aria-hidden="true"></span>
									    
									  </a>
						
						
					
					  
					</div>


Donde le daremos click para que se muestre el contenido que queremos: 

Que es este:

<div class="panel panel-primary">
				  <div class="panel-heading">Resultado de la Búsqueda</div>
				  <div class="list-group">
				  	<!-- Inicio filas de datos -->
				  	 
				    <div class="list-group-item">
				    	<div class="row">
				    		<div class="col-sm-5">
	                         	<h4 class="list-group-item-heading">Número de RUC:</h4>
	                         </div>
	                         <div class="col-sm-7">
	                         	<h4 class="list-group-item-heading">10788016005 - CANECILLAS CONTRERAS JUAN MARIANO</h4>
	                         </div>
				    	</div>  
			        </div>
			        <div class="list-group-item">
				    	<div class="row">
				    		<div class="col-sm-5">
	                         	<h4 class="list-group-item-heading">Tipo Contribuyente:</h4>
	                         </div>
	                         <div class="col-sm-7">
	                         	<p class="list-group-item-text">PERSONA NATURAL SIN NEGOCIO</p>
	                         </div>
				    	</div>  
			        </div>
			        
			          <div class="list-group-item">
					    	<div class="row">
					    		<div class="col-sm-5">
		                         	<h4 class="list-group-item-heading">Tipo de Documento:</h4>
		                         </div>
		                         <div class="col-sm-7">
		                         	<p class="list-group-item-text">DNI  78801600 
						            
						            	 - CANECILLAS CONTRERAS, JUAN MARIANO
						            	
						            </p>
		                         </div>
					    	</div>  
				        </div>
			        
			        <div class="list-group-item">
				    	<div class="row">
				    		<div class="col-sm-5">
	                         	<h4 class="list-group-item-heading">Nombre Comercial:</h4>
	                         </div>
	                         <div class="col-sm-7">
	                         	<p class="list-group-item-text">-
					              
					            </p>
	                         </div>
				    	</div>  
			        </div>
			         
			        <div class="list-group-item">
				    	<div class="row">
				    		<div class="col-sm-3">
	                         	<h4 class="list-group-item-heading">Fecha de Inscripción:</h4>
	                         </div>
	                         <div class="col-sm-3">
	                         	<p class="list-group-item-text">02/12/2023</p>
	                         </div>
	                         
	                         <div class="col-sm-3">
	                         	<h4 class="list-group-item-heading">Fecha de Inicio de Actividades:</h4>
	                         </div>
	                         <div class="col-sm-3">
	                         	<p class="list-group-item-text">02/12/2023</p>
	                         </div>
				    	</div>  
			        </div>
			        
			         
			        <div class="list-group-item list-group-item-success">
				    	<div class="row">
				    		<div class="col-sm-5">
	                         	<h4 class="list-group-item-heading">Estado del Contribuyente:</h4>
	                         </div>
	                         <div class="col-sm-7">
	                         	
	                         	<p class="list-group-item-text">ACTIVO
	                         	
					                 
					                
	                         	</p>
	                         </div>
				    	</div>  
			        </div>
			        
			        
			        
			        <div class="list-group-item list-group-item-success">
				    	<div class="row">
				    		<div class="col-sm-5">
	                         	<h4 class="list-group-item-heading">Condición del Contribuyente:</h4>
	                         </div>
	                         <div class="col-sm-7">
	                         	
	                         	<p class="list-group-item-text">
		                         	
						              	HABIDO
						              

								</p>
	                         </div>
				    	</div>  
			        </div>
			        
			        
			        <div class="list-group-item">
				    	<div class="row">
				    		<div class="col-sm-5">
	                         	<h4 class="list-group-item-heading">Domicilio Fiscal:</h4>
	                         </div>
	                         <div class="col-sm-7">
	                         	<p class="list-group-item-text">-</p>
	                         </div>
				    	</div>  
			        </div>
			        
			         
			        <div class="list-group-item">
				    	<div class="row">
				    		<div class="col-sm-3">
	                         	<h4 class="list-group-item-heading">Sistema Emisión de Comprobante:</h4>
	                         </div>
	                         <div class="col-sm-3">
	                         	<p class="list-group-item-text">MANUAL</p>
	                         </div>
	                         
	                         <div class="col-sm-3">
	                         	<h4 class="list-group-item-heading">Actividad Comercio Exterior:</h4>
	                         </div>
	                         <div class="col-sm-3">
	                         	<p class="list-group-item-text">SIN ACTIVIDAD</p>
	                         </div>
				    	</div>  
			        </div>
			        
			        <div class="list-group-item">
				    	<div class="row">
				    		<div class="col-sm-5">
	                         	<h4 class="list-group-item-heading">Sistema Contabilidad:</h4>
	                         </div>
	                         <div class="col-sm-7">
	                         	<p class="list-group-item-text">MANUAL</p>
	                         </div>
				    	</div>  
			        </div>
			        
			        
			        
			        <div class="list-group-item">
				    	<div class="row">
				    		<div class="col-sm-5">
	                         	<h4 class="list-group-item-heading">Actividad(es) Económica(s):</h4>
	                         </div>
	                         <div class="col-sm-7">
	                         	<table class="table tblResultado">
	                                <tbody>
	                    
		                <tr><td>Principal    - 9609 - OTRAS ACTIVIDADES DE SERVICIOS PERSONALES N.C.P.</td></tr>
		                <!--SC003-2015 Inicio-->
		                
		                <!--SC003-2015 Fin-->
		                
		                <!--SC003-2015 Fin-->
		                
		                
									</tbody>
	                            </table>
	                         </div>
				    	</div>  
			        </div>
			        
			        
			        
			        <div class="list-group-item">
				    	<div class="row">
				    		<div class="col-sm-5">
	                         	<h4 class="list-group-item-heading">Comprobantes de Pago c/aut. de impresión (F. 806 u 816):</h4>
	                         </div>
	                         <div class="col-sm-7">
	                         	<table class="table tblResultado">
	                                <tbody>
	                               	 	
						                
						                	<tr><td>NINGUNO</td></tr>
						                
									</tbody>
	                            </table>
	                         </div>
				    	</div>  
			        </div>
			        
			        <div class="list-group-item">
				    	<div class="row">
				    		<div class="col-sm-5">
	                         	<h4 class="list-group-item-heading">Sistema de Emisión Electrónica:</h4>
	                         </div>
	                         <div class="col-sm-7">
	                         	
				                
						                <table class="table tblResultado">
	                                		<tbody>
						                
						                
						                
						                		<tr><td>RECIBOS POR HONORARIOS     AFILIADO DESDE 29/02/2024</td></tr>
						                
								             </tbody>
			                            </table>
				                
	                         </div>
				    	</div>  
			        </div>
			        
			        <div class="list-group-item">
				    	<div class="row">
				    		<div class="col-sm-5">
	                         	<h4 class="list-group-item-heading">Emisor electrónico desde:</h4>
	                         </div>
	                         <div class="col-sm-7">
	                         	<p class="list-group-item-text">29/02/2024</p>
	                         </div>
				    	</div>  
			        </div>
			        
			        <div class="list-group-item">
				    	<div class="row">
				    		<div class="col-sm-5">
	                         	<h4 class="list-group-item-heading">Comprobantes Electrónicos:</h4>
	                         </div>
	                         <div class="col-sm-7">
	                         	<p class="list-group-item-text">RECIBO POR HONORARIO (desde 29/02/2024)</p>
	                         </div>
				    	</div>  
			        </div>
			        
			        <div class="list-group-item">
				    	<div class="row">
				    		<div class="col-sm-5">
	                         	<h4 class="list-group-item-heading">Afiliado al PLE desde:</h4>
	                         </div>
	                         <div class="col-sm-7">
	                         	<p class="list-group-item-text">-</p>
	                         </div>
				    	</div>  
			        </div>
			        
			        <div class="list-group-item">
				    	<div class="row">
				    		<div class="col-sm-5">
	                         	<h4 class="list-group-item-heading">Padrones:</h4>
	                         </div>
	                         <div class="col-sm-7">
	                         	 <table class="table tblResultado">
                               		<tbody>
						                
				                       		
						                
						                
						                
						
										<!-- JRR - 20/09/2010 - Se añade cambio de Igor -->
										
						                
						                
						                
											<tr><td>NINGUNO</td></tr>
						                
					                </tbody>
	                            </table>
	                         </div>
				    	</div>  
			        </div>
			        
			        
			        <!-- <div class="list-group-item">
				    	<div class="row">
				    		<div class="col-sm-5">
	                         	<h4 class="list-group-item-heading">Razón Social:</h4>
	                         </div>
	                         <div class="col-sm-7">
	                         	<p class="list-group-item-text">eeee</p>
	                         </div>
				    	</div>  
			        </div> -->
			        
			        
			        
				  </div><!-- fin list-group -->
				  
				  
				  <div class="panel-footer text-center">
				  	<small>Fecha consulta: 05/01/2026 15:14</small>
				  </div><!-- fin footer del panel -->
				</div>




Entonces tomaremos captura a este elemento y todo dentro

<div class="panel panel-primary">

Y luego lo pegaremos en el excel pero en la hoja que se llama SUNAT en el rango C5:O51


