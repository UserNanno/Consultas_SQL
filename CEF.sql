from openai import OpenAI
import os

# Token del propio notebook (no necesitas crear PAT)
DATABRICKS_TOKEN = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()

client = OpenAI(
    api_key=DATABRICKS_TOKEN,
    base_url="https://adb-6238163592670798.18.azuredatabricks.net/serving-endpoints"
)



MODEL_NAME = "databricks-llama-4-maverick"

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

    contenido = completion.choices[0].message.content

    try:
        return json.loads(contenido)
    except json.JSONDecodeError:
        # Si devuelve texto no parseable, devolvemos algo seguro
        return {
            "intencion": None,
            "tema": contenido[:200],  # log de lo que devolvió
            "es_relacionado_al_trabajo": None,
            "confianza": 0.0,
        }



clasificar_pregunta("Quiero saber si el cliente tiene deuda en su tarjeta VISA")
