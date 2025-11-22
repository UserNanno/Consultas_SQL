MODEL_NAME = "databricks-llama-4-maverick"

SYSTEM_PROMPT = """
Eres un sistema de clasificación de intenciones para un banco.

Tu única tarea es devolver UN OBJETO JSON VÁLIDO.

Muy importante:
- Devuelve ÚNICAMENTE el JSON.
- NO incluyas explicaciones.
- NO uses markdown.
- NO uses ``` ni etiquetas como ```json.
- No escribas texto antes ni después del JSON.

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

Formato de salida (JSON), recuerda: SOLO esto, sin nada más:

{
  "intencion": "<una_de_las_categorias>",
  "tema": "<resumen corto en 3-8 palabras>",
  "es_relacionado_al_trabajo": true/false,
  "confianza": 0.x
}
"""





import json

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
        temperature=0.0,
        max_tokens=500,
    )

    contenido = completion.choices[0].message.content.strip()

    # --- LIMPIEZA AUTOMÁTICA DE FORMATO ---

    # 1) eliminar bloques ``` si vienen
    if "```" in contenido:
        contenido = contenido.replace("```json", "")
        contenido = contenido.replace("```", "")
        contenido = contenido.strip()

    # 2) quedarnos solo con lo que está entre { y }
    if "{" in contenido and "}" in contenido:
        start = contenido.find("{")
        end = contenido.rfind("}") + 1
        contenido = contenido[start:end].strip()

    # 3) intentar parsear
    try:
        data = json.loads(contenido)
    except json.JSONDecodeError:
        return {
            "intencion": None,
            "tema": contenido[:200],  # muestra lo que devolvió, para debugging
            "es_relacionado_al_trabajo": None,
            "confianza": 0.0,
        }

    return {
        "intencion": data.get("intencion"),
        "tema": data.get("tema"),
        "es_relacionado_al_trabajo": data.get("es_relacionado_al_trabajo"),
        "confianza": float(data.get("confianza", 0.0)),
    }




clasificar_pregunta("Quiero saber si el cliente tiene deuda en su tarjeta VISA")
