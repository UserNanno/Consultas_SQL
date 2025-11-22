BadRequestError: Error code: 400 - {'error_code': 'BAD_REQUEST', 'message': 'Bad request: "messages" must contain the word "json" in some form, to use "response_format" of type "json_object".\n'}
---------------------------------------------------------------------------
BadRequestError                           Traceback (most recent call last)
File <command-7024870534985305>, line 1
----> 1 clasificar_pregunta("Quiero saber si el cliente tiene deuda en su tarjeta VISA")

File <command-7024870534985304>, line 12, in clasificar_pregunta(mensaje)
      4 if not mensaje:
      5     return {
      6         "intencion": None,
      7         "tema": None,
      8         "es_relacionado_al_trabajo": None,
      9         "confianza": 0.0,
     10     }
---> 12 completion = client.chat.completions.create(
     13     model=MODEL_NAME,
     14     messages=[
     15         {"role": "system", "content": SYSTEM_PROMPT},
     16         {"role": "user", "content": mensaje},
     17     ],
     18     # Esto le dice al endpoint que nos devuelva JSON bien formado
     19     response_format={"type": "json_object"},
     20     temperature=0.0,
     21     max_tokens=500,  # opcional
     22 )
     24 contenido = completion.choices[0].message.content
     25 return json.loads(contenido)

File /local_disk0/.ephemeral_nfs/envs/pythonEnv-495a8d2f-6eaa-451e-8475-331fe99d1f07/lib/python3.10/site-packages/openai/_utils/_utils.py:286, in required_args.<locals>.inner.<locals>.wrapper(*args, **kwargs)
    284             msg = f"Missing required argument: {quote(missing[0])}"
    285     raise TypeError(msg)
--> 286 return func(*args, **kwargs)

File /local_disk0/.ephemeral_nfs/envs/pythonEnv-495a8d2f-6eaa-451e-8475-331fe99d1f07/lib/python3.10/site-packages/openai/resources/chat/completions/completions.py:1189, in Completions.create(self, messages, model, audio, frequency_penalty, function_call, functions, logit_bias, logprobs, max_completion_tokens, max_tokens, metadata, modalities, n, parallel_tool_calls, prediction, presence_penalty, prompt_cache_key, prompt_cache_retention, reasoning_effort, response_format, safety_identifier, seed, service_tier, stop, store, stream, stream_options, temperature, tool_choice, tools, top_logprobs, top_p, user, verbosity, web_search_options, extra_headers, extra_query, extra_body, timeout)
   1142 @required_args(["messages", "model"], ["messages", "model", "stream"])
   1143 def create(
   1144     self,
   (...)
   1186     timeout: float | httpx.Timeout | None | NotGiven = not_given,
   1187 ) -> ChatCompletion | Stream[ChatCompletionChunk]:
   1188     validate_response_format(response_format)
-> 1189     return self._post(
   1190         "/chat/completions",
   1191         body=maybe_transform(
   1192             {
   1193                 "messages": messages,
   1194                 "model": model,
   1195                 "audio": audio,
   1196                 "frequency_penalty": frequency_penalty,
   1197                 "function_call": function_call,
   1198                 "functions": functions,
   1199                 "logit_bias": logit_bias,
   1200                 "logprobs": logprobs,
   1201                 "max_completion_tokens": max_completion_tokens,
   1202                 "max_tokens": max_tokens,
   1203                 "metadata": metadata,
   1204                 "modalities": modalities,
   1205                 "n": n,
   1206                 "parallel_tool_calls": parallel_tool_calls,
   1207                 "prediction": prediction,
   1208                 "presence_penalty": presence_penalty,
   1209                 "prompt_cache_key": prompt_cache_key,
   1210                 "prompt_cache_retention": prompt_cache_retention,
   1211                 "reasoning_effort": reasoning_effort,
   1212                 "response_format": response_format,
   1213                 "safety_identifier": safety_identifier,
   1214                 "seed": seed,
   1215                 "service_tier": service_tier,
   1216                 "stop": stop,
   1217                 "store": store,
   1218                 "stream": stream,
   1219                 "stream_options": stream_options,
   1220                 "temperature": temperature,
   1221                 "tool_choice": tool_choice,
   1222                 "tools": tools,
   1223                 "top_logprobs": top_logprobs,
   1224                 "top_p": top_p,
   1225                 "user": user,
   1226                 "verbosity": verbosity,
   1227                 "web_search_options": web_search_options,
   1228             },
   1229             completion_create_params.CompletionCreateParamsStreaming
   1230             if stream
   1231             else completion_create_params.CompletionCreateParamsNonStreaming,
   1232         ),
   1233         options=make_request_options(
   1234             extra_headers=extra_headers, extra_query=extra_query, extra_body=extra_body, timeout=timeout
   1235         ),
   1236         cast_to=ChatCompletion,
   1237         stream=stream or False,
   1238         stream_cls=Stream[ChatCompletionChunk],
   1239     )

File /local_disk0/.ephemeral_nfs/envs/pythonEnv-495a8d2f-6eaa-451e-8475-331fe99d1f07/lib/python3.10/site-packages/openai/_base_client.py:1259, in SyncAPIClient.post(self, path, cast_to, body, options, files, stream, stream_cls)
   1245 def post(
   1246     self,
   1247     path: str,
   (...)
   1254     stream_cls: type[_StreamT] | None = None,
   1255 ) -> ResponseT | _StreamT:
   1256     opts = FinalRequestOptions.construct(
   1257         method="post", url=path, json_data=body, files=to_httpx_files(files), **options
   1258     )
-> 1259     return cast(ResponseT, self.request(cast_to, opts, stream=stream, stream_cls=stream_cls))

File /local_disk0/.ephemeral_nfs/envs/pythonEnv-495a8d2f-6eaa-451e-8475-331fe99d1f07/lib/python3.10/site-packages/openai/_base_client.py:1047, in SyncAPIClient.request(self, cast_to, options, stream, stream_cls)
   1044             err.response.read()
   1046         log.debug("Re-raising status error")
-> 1047         raise self._make_status_error_from_response(err.response) from None
   1049     break
   1051 assert response is not None, "could not resolve response (should never happen)"

BadRequestError: Error code: 400 - {'error_code': 'BAD_REQUEST', 'message': 'Bad request: "messages" must contain the word "json" in some form, to use "response_format" of type "json_object".\n'}
