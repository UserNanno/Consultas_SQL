Deberá ingresar acá:
https://extranet.sbs.gob.pe/CambioClave/pages/cerrarSesiones.jsf

Y colocar el usuario aqui:

<td><input id="Formulario:txtCodUsuario" name="Formulario:txtCodUsuario" type="text" maxlength="16" onblur="value=value.toUpperCase()" aria-required="true" class="ui-inputfield ui-inputtext ui-widget ui-state-default ui-corner-all" role="textbox" aria-disabled="false" aria-readonly="false"></td>


Y la contraseña primero debes dar click en el input, 
    
<td><input id="Formulario:txtClave" name="Formulario:txtClave" type="password" maxlength="16" class="ui-inputfield ui-keyboard-input ui-widget ui-state-default ui-corner-all hasKeypad" aria-required="true" readonly="readonly" role="textbox" aria-disabled="false" aria-readonly="true"></td>


luego se aperturará unas opciones de dar click los numeros

<div id="keypad-div" style="position: absolute; top: 476.4px; width: auto; z-index: 1001; left: 346.2px; display: block;" class="ui-widget ui-widget-content ui-corner-all ui-shadow"><div class="keypad-row"><button type="button" class="keypad-key ui-state-default" title="" role="button" aria-disabled="false">1</button><button type="button" class="keypad-key ui-state-default" title="" role="button" aria-disabled="false">2</button><button type="button" class="keypad-key ui-state-default" title="" role="button" aria-disabled="false">3</button><button type="button" class="keypad-key ui-state-default keypad-close" title="Close the keypad" role="button" aria-disabled="false">Cerrar</button></div><div class="keypad-row"><button type="button" class="keypad-key ui-state-default" title="" role="button" aria-disabled="false">4</button><button type="button" class="keypad-key ui-state-default" title="" role="button" aria-disabled="false">5</button><button type="button" class="keypad-key ui-state-default" title="" role="button" aria-disabled="false">6</button><button type="button" class="keypad-key ui-state-default keypad-clear" title="Erase all the text" role="button" aria-disabled="false">Limpiar</button></div><div class="keypad-row"><button type="button" class="keypad-key ui-state-default" title="" role="button" aria-disabled="false">7</button><button type="button" class="keypad-key ui-state-default" title="" role="button" aria-disabled="false">8</button><button type="button" class="keypad-key ui-state-default" title="" role="button" aria-disabled="false">9</button><button type="button" class="keypad-key ui-state-default keypad-back" title="Erase the previous character" role="button" aria-disabled="false">Back</button></div><div class="keypad-row"><div class="keypad-space"></div><button type="button" class="keypad-key ui-state-default" title="" role="button" aria-disabled="false">0</button></div><div style="clear: both;"></div></div>


Y luego deberá dar click en el boton de continuar

<td><button id="Formulario:btnContinuar" name="Formulario:btnContinuar" class="ui-button ui-widget ui-state-default ui-corner-all ui-button-text-only" onclick="PrimeFaces.ab({s:&quot;Formulario:btnContinuar&quot;,u:&quot;Formulario&quot;});return false;" type="submit" role="button" aria-disabled="false"><span class="ui-button-text ui-c">Continuar</span></button></td>


Cargará esta pagina: https://extranet.sbs.gob.pe/CambioClave/pages/cerrarSesiones.jsf

<body>
	<div class="cuerpoPagina">
		<div class="cuerpoOpcion">
		<h1>Ha cerrado la sesión activa satisfactoriamente</h1>

		<div style="text-align: center;">
			<a href="http://extranet.sbs.gob.pe/app/login.jsp">Ir al Portal del Supervisado</a>
		</div>
		</div>
		<table id="pie">
			<tbody><tr>
				<td>
					Superintendencia de Banca, Seguros y AFP - Todos los derechos reservados -
					2026
				</td>
			</tr>
			<tr>
				<td>
					Contáctenos a: <a href="mailto:mesa-ayuda@sbs.gob.pe">mesa-ayuda@sbs.gob.pe</a>
				</td>
			</tr>
		</tbody></table>
	</div></body>


Donde indica que ya se cerró la sesión y luego continuamos con nuestro flujo normal. Esto debe hacerse cada que inicia el flujo desde 0
