
(venv) D:\Datos de Usuarios\T72496\Desktop\MODELOS_RPTs\WebAutomatic>py main.py
----
ruc_consultado: 10788016005
error: Message:
Stacktrace:
Symbols not available. Dumping unresolved backtrace:
        0x7ff679d488e5
        0x7ff679d48940
        0x7ff679b2165d
        0x7ff679b79a33
        0x7ff679b79d3c
        0x7ff679bcdf67
        0x7ff679bcac97
        0x7ff679b6ac29
        0x7ff679b6ba93
        0x7ff67a060640
        0x7ff67a05af80
        0x7ff67a0796e6
        0x7ff679d65de4
        0x7ff679d6ed8c
        0x7ff679d52004
        0x7ff679d521b5
        0x7ff679d37ee2
        0x7ffeea26259d
        0x7ffeeae2af78


(venv) D:\Datos de Usuarios\T72496\Desktop\MODELOS_RPTs\WebAutomatic>


if __name__ == "__main__":
    rucs = ["10788016005"]
    resultados = consultar_muchos(rucs, headless=True) # changes visibility

    for r in resultados:
        print("----")
        for k, v in r.items():
            print(f"{k}: {v}")
