NotFoundError: Error code: 404 - {'error_code': 'ENDPOINT_NOT_FOUND', 'message': 'The given endpoint does not exist, please retry after checking the specified model and version deployment exists.'}
---------------------------------------------------------------------------
NotFoundError                             Traceback (most recent call last)
File <command-7024870534985306>, line 1
----> 1 clasificar_pregunta("Quiero saber si el cliente tiene deuda en su tarjeta VISA")

File <command-7024870534985305>, line 10, in clasificar_pregunta(mensaje)
      2 if not mensaje:
      3     return {
      4         "intencion": None,
      5         "tema": None,
      6         "es_relacionado_al_trabajo": None,
      7         "confianza": 0.0,
      8     }
---> 10 completion = client.chat.completions.create(
     11     model=MODEL_NAME,
     12     messages=[
     13         {"role": "system", "content": SYSTEM_PROMPT},
     14         {"role": "user", "content": mensaje},
     15     ],
     16     response_format={"type": "json_object"},
     17     temperature=0.0,
