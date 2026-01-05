Perfecto, todo salio bien. Ahora aca tengo un tema, para ciertas busquedas de ciertos DNI cuando vamos a la sección de otros reportes se muestra una tabla adicional que tiene esta estructura

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
                  <a onclick="jsVerCT('3436435861224265094'); return false;" href="#">
                    <img src="images/mas.gif" alt="Carteras Transferidas"></a>
                </li>
                <li>
                  <a onclick="jsVerCT('7190420682310492127'); return false;" href="#">Carteras Transferidas</a>
                </li>
              </ul>
            </td>
          </tr>
        </tbody>
      </table>


Al darle click ahi en el a onclick se desglosa otra seccion pero he notado que la pagina se pasa a otra porque antes de hacer click la ruta es: https://extranet.sbs.gob.pe/criesgos/criesgos/buscarotrosreportes?r=6644719673685279000
pero despues de darle click la ruta es: https://extranet.sbs.gob.pe/criesgos/criesgos/buscarinfocarterastransferidas?r=7190420682310492127

Dentro de esta nueva ruta se muestra la mimsa primera tabla pero la segunda tabla adicional tiene la siguiente estructura

<table id="expand" class="Crw">
		<thead>
			<tr>
				<td class="Izq" colspan="10">
					<span class="F">Información de Carteras Transferidas</span>
				</td>
			</tr>
		</thead>

		<tbody>
			<tr class="Str">
				<td width="4%">&nbsp;</td>
				<td width="5%" align="center">
					<b class="Dz">N°</b>
				</td>
				<td width="8%" align="center">
					<b class="Dz">Fecha Cartera</b>
				</td>
				<td class="Izq">
					<b class="Dz">Entidad Vendedora</b>
				</td>
				<td width="12%" class="Izq">
					<b class="Dz">Clasificación</b>
				</td>
				<td width="16%" align="center">
					<b class="Dz">Tipo de Crédito</b>
				</td>
				<td width="10%" align="center">
					<b class="Dz">Capital</b>
				</td>
				<td width="10%" align="center">
					<b class="Dz">Interés y Comisiones</b>
				</td>
				<td width="10%" align="center">
					<b class="Dz">Saldo</b>
				</td>
				<td width="8%" align="center">
					<b class="Dz">Días de Atraso</b>
				</td>
			</tr>
			
				<tr class="master Def">
					<td align="center">
						<div class="arrow" title="Mostrar Rectificaciones"></div>
					</td>
					<td align="center">
						<span class="Dz">1</span>
					</td>
					<td align="center">
						<span class="Dz">03/2023</span>
					</td>
					<td class="Izq">
						<span class="Dz">BBVA</span>
						
							<span class="Dz" style="background-color: red; padding: 2px; color: white;">RAC</span>
						
						
					</td>
					<td class="Izq">
						<span class="Dz">Deficiente</span>
					</td>
					<td class="Izq">
						<span class="Dz">Créditos de Consumo revolventes</span>
					</td>
					<td class="Der">
						<span class="Dz">0</span>
					</td>
					<td class="Der">
						<span class="Dz">0</span>
					</td>
					<td class="Der">
						<span class="Dz">0</span>
					</td>
					<td class="Der">
						<span class="Dz">0</span>
					</td>
				</tr>
				<tr class="Vde" style="display: none;">
					<td>&nbsp;</td>
					<td colspan="9">
						<table class="Verde">
							<thead>
								<tr>
									<td class="Izq" colspan="9">
										<b class="Dz">Rectificaciones RIC</b>
									</td>
								</tr>
							</thead>

							<tbody>
								<tr class="Str">
									<td width="5%" align="center">
										<b class="Dz">N°</b>
									</td>
									<td width="12%" align="center">
										<b class="Dz">Concepto</b>
									</td>
									<td width="16%" align="center">
										<b class="Dz">Tipo de crédito</b>
									</td>
									<td align="center">
										<b class="Dz">Cuenta</b>
									</td>
									<td width="14%" align="center">
										<b class="Dz">Cond. días</b>
									</td>
									<td width="14%" align="center">
										<b class="Dz">Clasif.</b>
									</td>
									<td width="14%" align="center">
										<b class="Dz">Saldo</b>
									</td>
									<td width="12%" align="center">
										<b class="Dz">F. Registro</b>
									</td>
								</tr>
								
								
									<tr class="Def">
										<td class="Izq" colspan="8">
											<span class="Dz">No se han encontrado rectificaciones.</span>
										</td>
									</tr>
								
							</tbody>
						</table>

						<table class="Verde" style="margin-top: 5px;">
							<thead>
								<tr>
									<td class="Izq" colspan="9">
										<b class="Dz">Rectificaciones RAC</b>
									</td>
								</tr>
							</thead>

							<tbody>
								<tr class="Str">
									<td width="5%" align="center">
										<b class="Dz">N°</b>
									</td>
									<td align="center">
										<b class="Dz">Entidad Reportante</b>
									</td>
									<td width="15%" align="center">
										<b class="Dz">Concepto</b>
									</td>
									<td width="15%" align="center">
										<b class="Dz">Cond. días</b>
									</td>
									<td width="15%" align="center">
										<b class="Dz">Saldo</b>
									</td>
									<td width="17%" align="center">
										<b class="Dz">F. Registro</b>
									</td>
								</tr>
								
									<tr class="Def">
										<td align="center" rowspan="2">
											<span class="Dz">1</span>
										</td>
										<td class="Izq" rowspan="2">
											<span class="Dz">BBVA</span>
										</td>
										<td class="Izq">
											<span class="Dz">Dice</span>
										</td>
										<td class="Der">
											<span class="Dz">56</span>
										</td>
										<td class="Der">
											<span class="Dz">35,565.92</span>
										</td>
										<td align="center" rowspan="2">
											<span class="Dz">10/05/2024 02:34 PM</span>
										</td>
									</tr>
									<tr class="Def">
										<td class="Izq">
											<span class="Dz">Debe decir</span>
										</td>
										<td class="Der">
											<span class="Dz">0</span>
										</td>
										<td class="Der">
											<span class="Dz">0.00</span>
										</td>
									</tr>
								
									<tr class="Def">
										<td align="center" rowspan="2">
											<span class="Dz">2</span>
										</td>
										<td class="Izq" rowspan="2">
											<span class="Dz">BBVA</span>
										</td>
										<td class="Izq">
											<span class="Dz">Dice</span>
										</td>
										<td class="Der">
											<span class="Dz">37</span>
										</td>
										<td class="Der">
											<span class="Dz">71,637.54</span>
										</td>
										<td align="center" rowspan="2">
											<span class="Dz">10/05/2024 02:34 PM</span>
										</td>
									</tr>
									<tr class="Def">
										<td class="Izq">
											<span class="Dz">Debe decir</span>
										</td>
										<td class="Der">
											<span class="Dz">0</span>
										</td>
										<td class="Der">
											<span class="Dz">0.00</span>
										</td>
									</tr>
								
								
							</tbody>
						</table>
					</td>
				</tr>
			
				<tr class="master Def">
					<td align="center">
						<div class="arrow" title="Mostrar Rectificaciones"></div>
					</td>
					<td align="center">
						<span class="Dz">2</span>
					</td>
					<td align="center">
						<span class="Dz">03/2023</span>
					</td>
					<td class="Izq">
						<span class="Dz">BBVA</span>
						
							<span class="Dz" style="background-color: red; padding: 2px; color: white;">RAC</span>
						
						
					</td>
					<td class="Izq">
						<span class="Dz">Deficiente</span>
					</td>
					<td class="Izq">
						<span class="Dz">Créditos de Consumo no revolventes</span>
					</td>
					<td class="Der">
						<span class="Dz">0</span>
					</td>
					<td class="Der">
						<span class="Dz">0</span>
					</td>
					<td class="Der">
						<span class="Dz">0</span>
					</td>
					<td class="Der">
						<span class="Dz">0</span>
					</td>
				</tr>
				<tr class="Vde" style="display: none;">
					<td>&nbsp;</td>
					<td colspan="9">
						<table class="Verde">
							<thead>
								<tr>
									<td class="Izq" colspan="9">
										<b class="Dz">Rectificaciones RIC</b>
									</td>
								</tr>
							</thead>

							<tbody>
								<tr class="Str">
									<td width="5%" align="center">
										<b class="Dz">N°</b>
									</td>
									<td width="12%" align="center">
										<b class="Dz">Concepto</b>
									</td>
									<td width="16%" align="center">
										<b class="Dz">Tipo de crédito</b>
									</td>
									<td align="center">
										<b class="Dz">Cuenta</b>
									</td>
									<td width="14%" align="center">
										<b class="Dz">Cond. días</b>
									</td>
									<td width="14%" align="center">
										<b class="Dz">Clasif.</b>
									</td>
									<td width="14%" align="center">
										<b class="Dz">Saldo</b>
									</td>
									<td width="12%" align="center">
										<b class="Dz">F. Registro</b>
									</td>
								</tr>
								
								
									<tr class="Def">
										<td class="Izq" colspan="8">
											<span class="Dz">No se han encontrado rectificaciones.</span>
										</td>
									</tr>
								
							</tbody>
						</table>

						<table class="Verde" style="margin-top: 5px;">
							<thead>
								<tr>
									<td class="Izq" colspan="9">
										<b class="Dz">Rectificaciones RAC</b>
									</td>
								</tr>
							</thead>

							<tbody>
								<tr class="Str">
									<td width="5%" align="center">
										<b class="Dz">N°</b>
									</td>
									<td align="center">
										<b class="Dz">Entidad Reportante</b>
									</td>
									<td width="15%" align="center">
										<b class="Dz">Concepto</b>
									</td>
									<td width="15%" align="center">
										<b class="Dz">Cond. días</b>
									</td>
									<td width="15%" align="center">
										<b class="Dz">Saldo</b>
									</td>
									<td width="17%" align="center">
										<b class="Dz">F. Registro</b>
									</td>
								</tr>
								
									<tr class="Def">
										<td align="center" rowspan="2">
											<span class="Dz">1</span>
										</td>
										<td class="Izq" rowspan="2">
											<span class="Dz">BBVA</span>
										</td>
										<td class="Izq">
											<span class="Dz">Dice</span>
										</td>
										<td class="Der">
											<span class="Dz">56</span>
										</td>
										<td class="Der">
											<span class="Dz">35,565.92</span>
										</td>
										<td align="center" rowspan="2">
											<span class="Dz">10/05/2024 02:34 PM</span>
										</td>
									</tr>
									<tr class="Def">
										<td class="Izq">
											<span class="Dz">Debe decir</span>
										</td>
										<td class="Der">
											<span class="Dz">0</span>
										</td>
										<td class="Der">
											<span class="Dz">0.00</span>
										</td>
									</tr>
								
									<tr class="Def">
										<td align="center" rowspan="2">
											<span class="Dz">2</span>
										</td>
										<td class="Izq" rowspan="2">
											<span class="Dz">BBVA</span>
										</td>
										<td class="Izq">
											<span class="Dz">Dice</span>
										</td>
										<td class="Der">
											<span class="Dz">37</span>
										</td>
										<td class="Der">
											<span class="Dz">71,637.54</span>
										</td>
										<td align="center" rowspan="2">
											<span class="Dz">10/05/2024 02:34 PM</span>
										</td>
									</tr>
									<tr class="Def">
										<td class="Izq">
											<span class="Dz">Debe decir</span>
										</td>
										<td class="Der">
											<span class="Dz">0</span>
										</td>
										<td class="Der">
											<span class="Dz">0.00</span>
										</td>
									</tr>
								
								
							</tbody>
						</table>
					</td>
				</tr>
			
		</tbody>

		<tfoot>
			<tr>
				<td colspan="10" class="Izq">
					<span class="Dz">Nota: Para mostrar las rectificaciones RIC y RAC (si los hubiese), haga clic en la imagen</span> <img src="images/arrow-down.png" alt="Mostrar Rectificaciones" title="Mostrar Rectificaciones" style="vertical-align: middle">
				</td>
			</tr>
		</tfoot>
	</table>



Donde al dar click en <div class="arrow" title="Mostrar Rectificaciones"></div> aca si se desglosa (no cambia de URL ni refreshea). Lo mismo con este otro <div class="arrow" title="Mostrar Rectificaciones"></div> (en html son lo mismo creo) pero la cosa que hay dos de ellos en diferentes filas.

Entonces el flujo deberia ser, si no encuentra esta segunda tabla, deberá tomar captura solo despues de dar click en "Otros repotes" en el menú. En caso si exista esta tabla adicional deberá entrar, desglosar las dos opciones que figuran y tomar la captura de toda la pantalla del navegador o incluso mejor solo tomar captura a esta parte

<div id="Contenido"> que tiene todo el contenido de las tablas. Se podrá hacer esto? 

