Todo me ejecuta perfectamente :) 

Ahora quiero añadir otra pagina que es: https://suitebancapersonas.lima.bcp.com.pe:444/Consumos/FiltroClienteConsumo

Dentro existe un selector donde escogeremos DNI

<div class="editor-field col-md-4">
<select class="form-control" id="CodTipoDocumento" name="CodTipoDocumento"><option value="1">D.N.I.</option>
<option value="3">C.E.</option>
</select>
</div>

Luego digitaremos el DNI en:
<div class="editor-field col-md-4">
<input class="form-control" data-val="true" data-val-length="Ingresar 8 caracteres como minimo" data-val-length-max="30" data-val-length-min="8" id="CodDocumento" name="CodDocumento" required="True" type="text" value="" maxlength="8">
</div>

Luego haremos la consulta con el siguiente boton:
<div class="col-md-4">
<button id="btnConsultar" type="submit" class="btn btn-primary">
<i class="fa fa-search"></i>
                                            Consultar
</button>
</div>



Posterior tomaremos captura a la pantalla que salga ahi, todo lo que está dentro de <div class="panel-body"> y lo pegaremos en la hoja RBM en C5:Z50

Posterior daremos click en:
<button class="nav-link" id="CEM-tab" data-toggle="tab" data-target="#CEM" type="button" role="tab" aria-controls="nav-CEM" aria-selected="false" style="font-weight: bold;">Módulo Capacidad Endeudamiento</button>

Que nos cargará la pagina otra sección donde tambien tomaremos captura a todo lo que esté a dentro de <div class="panel-body"> y lo pegaremos en la hoja RBM en C64:Z106

No olvides agregar que se cierre el edge cuando finalice el flujo

