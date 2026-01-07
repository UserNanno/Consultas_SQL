Mira, de la primera pagina que se carga al buscar el DNI en RBM (la pagina de consumo) quiero extraer el valor del campo de segmento, es decir deberia traerme el valor ENALTA y pegarlo en la celda C11 de la hoja llamada "Inicio". Lo podemos encontrar en el siguiente HTML de la pagina
<!-- /.panel-heading -->
            <div class="panel-body">
                    <div class="row">
                        <div class="col-md-12">
                            <div class="form-group col-md-12" style="display: flex; flex-direction:row;">
                                <div class="editor-label col-md-3">
                                    Tipo de documento
                                </div>
                                <div class="editor-field col-md-3">
                                    D.N.I.
                                </div>
                                <div class="editor-label col-md-3">
                                    Cliente
                                </div>
                                <div class="editor-field col-md-3">
                                    MARGALL GRANDEZ MIGUEL JEAN FRANCO
                                </div>
                            </div>
                            <!-- /.col-md-12 -->
                            <div class="form-group col-md-12" style="display: flex; flex-direction:row;">
                                <div class="editor-label col-md-3">
                                    Número de documento
                                </div>
                                <div class="editor-field col-md-3">
                                    73600493
                                </div>
                                <div class="editor-label col-md-3">
                                    Segmento
                                </div>
                                <div class="editor-field col-md-3">
                                    ENALTA
                                </div>



Asimismo, quiero traer otro valor que es del campo Segmento de Riesgo que se encuentra acá:

                                                <td style="text-align:center;color:#1F497D;">
                                                    A
                                                    <input id="SegmentoRiesgo" name="SegmentoRiesgo" type="hidden" value="A">
                                                </td>

deberá traer el valor "A" y pegarlo en la celda C12 de la hoja "Inicio"

Asimismo para otro campo que es 

                                                        <li class="list-group-item d-flex justify-content-between align-items-center">
                                                            <div class="d-flex align-items-center" style="cursor: pointer;" data-toggle="tooltip" data-placement="top" title="" data-original-title="Estado actual del empleo del cliente.">
                                                                Situación Laboral
                                                            </div>

                                                            <span class="badge badge-primary badge-pill" style="cursor: pointer;" data-toggle="tooltip" data-placement="top" title="" data-original-title="Labora en una empresa, sin abono de haberes en el BCP.">
                                                                No PdH – Dependiente
                                                            </span>
                                                        </li>

En caso dijera "PDH" en el valor del span, entonces deberá colocarse "Si" en la celda, en cualquier otro caso, colocar "No" en la celda C13 de la hoja "Inicio"


Asimismo, para otro campo deberás buscar en:

                                                        <li class="list-group-item d-flex justify-content-between align-items-center" style="background-color: #DCE6F1;">
                                                            Score RCC
                                                            <span class="badge badge-primary badge-pill">
                                                                289
                                                            </span>
                                                        </li>

Donde deberás colocar el valor del span en la celda C15 de la hoja "Inicio"
