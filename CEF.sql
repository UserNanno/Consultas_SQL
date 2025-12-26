(venv) D:\Datos de Usuarios\T72496\Desktop\MODELOS_RPTs\WebAutomatic>py edge.py
Traceback (most recent call last):
  File "D:\Datos de Usuarios\T72496\Desktop\MODELOS_RPTs\WebAutomatic\edge.py", line 185, in <module>
    main()
    ~~~~^^
  File "D:\Datos de Usuarios\T72496\Desktop\MODELOS_RPTs\WebAutomatic\edge.py", line 135, in main
    driver = webdriver.Edge(options=opts)
  File "D:\Datos de Usuarios\T72496\Desktop\MODELOS_RPTs\venv\Lib\site-packages\selenium\webdriver\edge\webdriver.py", line 48, in __init__
    super().__init__(
    ~~~~~~~~~~~~~~~~^
        browser_name=DesiredCapabilities.EDGE["browserName"],
        ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    ...<3 lines>...
        keep_alive=keep_alive,
        ^^^^^^^^^^^^^^^^^^^^^^
    )
    ^
  File "D:\Datos de Usuarios\T72496\Desktop\MODELOS_RPTs\venv\Lib\site-packages\selenium\webdriver\chromium\webdriver.py", line 67, in __init__
    super().__init__(command_executor=executor, options=options)
    ~~~~~~~~~~~~~~~~^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "D:\Datos de Usuarios\T72496\Desktop\MODELOS_RPTs\venv\Lib\site-packages\selenium\webdriver\remote\webdriver.py", line 257, in __init__
    self.start_session(capabilities)
    ~~~~~~~~~~~~~~~~~~^^^^^^^^^^^^^^
  File "D:\Datos de Usuarios\T72496\Desktop\MODELOS_RPTs\venv\Lib\site-packages\selenium\webdriver\remote\webdriver.py", line 352, in start_session
    response = self.execute(Command.NEW_SESSION, caps)["value"]
               ~~~~~~~~~~~~^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "D:\Datos de Usuarios\T72496\Desktop\MODELOS_RPTs\venv\Lib\site-packages\selenium\webdriver\remote\webdriver.py", line 432, in execute
    self.error_handler.check_response(response)
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~^^^^^^^^^^
  File "D:\Datos de Usuarios\T72496\Desktop\MODELOS_RPTs\venv\Lib\site-packages\selenium\webdriver\remote\errorhandler.py", line 232, in check_response
    raise exception_class(message, screen, stacktrace)
selenium.common.exceptions.SessionNotCreatedException: Message: session not created: cannot connect to microsoft edge at 127.0.0.1:9222
from chrome not reachable; For documentation on this error, please visit: https://www.selenium.dev/documentation/webdriver/troubleshooting/errors#sessionnotcreatedexception
Stacktrace:
Symbols not available. Dumping unresolved backtrace:
        0x7ff6b6a587d5
        0x7ff6b69c8e44
        0x7ff6b67dc746
        0x7ff6b67d24e0
        0x7ff6b681591c
        0x7ff6b680c480
        0x7ff6b684cfbb
        0x7ff6b681982a
        0x7ff6b6818b33
        0x7ff6b6819653
        0x7ff6b69022e4
        0x7ff6b691109c
        0x7ff6b690ac7f
        0x7ff6b6ae9b37
        0x7ff6b69d46a6
        0x7ff6b69ceab4
        0x7ff6b69cebf9
        0x7ff6b69c2cbd
        0x7ffd919a259d
        0x7ffd93d4af78


(venv) D:\Datos de Usuarios\T72496\Desktop\MODELOS_RPTs\WebAutomatic>













