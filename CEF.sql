%pip install --upgrade openai databricks-sdk[openai]
dbutils.library.restartPython()


from openai import OpenAI
import os

# Base URL para foundation models de tu workspace
os.environ["OPENAI_BASE_URL"] = (
    "https://" + spark.conf.get("spark.databricks.workspaceUrl") + "/serving-endpoints/"
)

# Token del propio notebook (PAT interno)
os.environ["OPENAI_API_KEY"] = (
    dbutils.notebook.entry_point.getDbutils()
    .notebook()
    .getContext()
    .apiToken()
    .getOrElse(None)
)

client = OpenAI()




SYSTEM_PROMPT = """
Eres un sistema de clasificación de intenciones para un banco.

Dado un mensaje escrito por un analista a GenIA, debes clasificarlo en base a estas categorías:

- consulta_producto_bancario
- consulta_proceso_interno
- problema_tecnico
- pregunta_personal_general
- otro

Reglas:
- es_relacionado_al_trabajo = true si el mensaje tiene relación con el banco, sus productos, procesos internos o el trabajo del analista.
- es_relacionado_al_trabajo = false si es una pregunta personal, de cultura general, chistes, temas random, etc.
- confianza es un número entre 0 y 1 (0 = nada seguro, 1 = totalmente seguro).

Devuelve SIEMPRE un JSON con este formato EXACTO:

{
  "intencion": "<una_de_las_categorias>",
  "tema": "<resumen corto en 3-8 palabras>",
  "es_relacionado_al_trabajo": true/false,
  "confianza": 0.x
}
"""

MODEL_NAME = "databricks-meta-llama-3-3-70b-instruct"  # o el modelo foundation que tengas habilitado







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
        response_format={"type": "json_object"},  # fuerza JSON bien formado
        temperature=0.0,  # más determinista
    )

    contenido = completion.choices[0].message.content
    try:
        data = json.loads(contenido)
    except json.JSONDecodeError:
        # fallback en caso de que devuelva algo raro
        data = {
            "intencion": None,
            "tema": None,
            "es_relacionado_al_trabajo": None,
            "confianza": 0.0,
        }

    # asegurar campos
    return {
        "intencion": data.get("intencion"),
        "tema": data.get("tema"),
        "es_relacionado_al_trabajo": data.get("es_relacionado_al_trabajo"),
        "confianza": float(data.get("confianza", 0.0)),
    }





clasificar_pregunta("Quiero saber si el cliente tiene deuda en su tarjeta VISA")
