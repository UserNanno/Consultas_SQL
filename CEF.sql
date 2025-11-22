SYSTEM_PROMPT = """
Eres un sistema de clasificación de intenciones para un banco.

Debes devolver SIEMPRE un JSON válido.

Clasifica el mensaje del analista en una de las siguientes categorías:

- consulta_producto_bancario
- consulta_proceso_interno
- problema_tecnico
- pregunta_personal_general
- otro

Reglas:
- es_relacionado_al_trabajo = true si tiene relación con productos del banco, procesos internos o tareas del puesto.
- es_relacionado_al_trabajo = false si es una pregunta personal, trivial, cultural o ajena al trabajo.
- confianza es un número entre 0 y 1.

Formato de salida (JSON):
{
  "intencion": "<una_de_las_categorias>",
  "tema": "<resumen corto en 3-8 palabras>",
  "es_relacionado_al_trabajo": true/false,
  "confianza": 0.x
}
"""




def clasificar_pregunta(mensaje: str) -> dict:
    if not mensaje:
        return {
            "intencion": None,
            "tema": None,
            "es_relacionado_al_trabajo": None,
            "confianza": 0.0,
        }

    completion = client.chat.completions.create(
        model=MODEL_NAME,
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": mensaje},
        ],
        response_format={"type": "json_object"},
        temperature=0.0,
    )

    contenido = completion.choices[0].message.content
    return json.loads(contenido)



clasificar_pregunta("Quiero saber si el cliente tiene deuda en su tarjeta VISA")
