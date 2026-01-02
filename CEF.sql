Cuando me carga la pagina despues del login quisiera que de click a este boton

<tr>
              <td class="Izq" valign="top">
                <a onclick="f_hub('/criesgos/criesgos/criesgos.jsp', '/criesgos/login', '00002', '_self', 'S'); return false;" href="#" class="descripcion">Aquí Ud. podrá consultar la situación de deuda de los clientes del sistema financiero, así mismo Ud. podra ver el histórico de dicha información.</a>
              </td>
            </tr>





Eso me llevara a otra pagina donde el tendrá un formulario dentro hay tipo un listbox donde quisiwera que seleccionara el DNI

<select name="as_tipo_doc" id="as_tipo_doc" style="width: 200px;">
                  <option value="*">Tipo de Documento</option>
                  <option value="11" selected="selected">1. LE/DNI</option>
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
                  <option value="1C">C. Documento expedido por la CEPR del Ministerio de Relaciones Exteriores
                    que acredita que la solicitud de refugiado se encuentra en trámite</option>
                </select>



Luego que seleccionara

Que escriba el siguiente DNI 78801600 acá


<input type="text" size="15" maxlength="16" class="input-upper" onchange="ValInputValid(this)" onkeypress="return OnKeyPressValidInput(event)" name="as_doc_iden" value="Número de Doc." onclick="this.select(); this.focus();">



Luego de ello que le de click aquí

<input type="submit" name="Submit" value="Consultar" class="boton" id="btnConsultar">
