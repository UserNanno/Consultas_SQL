Traceback (most recent call last):
  File "main.py", line 124, in <module>
  File "utils\decorators.py", line 9, in wrapper
  File "main.py", line 46, in main
  File "infrastructure\selenium_driver.py", line 15, in create
  File "selenium\webdriver\remote\webdriver.py", line 914, in set_window_size
    self.set_window_rect(width=int(width), height=int(height))
  File "selenium\webdriver\remote\webdriver.py", line 986, in set_window_rect
    return self.execute(Command.SET_WINDOW_RECT, {"x": x, "y": y, "width": width, "height": height})["value"]
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "selenium\webdriver\remote\webdriver.py", line 432, in execute
    self.error_handler.check_response(response)
  File "selenium\webdriver\remote\errorhandler.py", line 232, in check_response
    raise exception_class(message, screen, stacktrace)
selenium.common.exceptions.WebDriverException: Message: unknown error: failed to change window state to 'normal', current state is 'maximized'
  (Session info: MicrosoftEdge=143.0.3650.96)
Stacktrace:
Symbols not available. Dumping unresolved backtrace:
	0x7ff70fe687d5
	0x7ff70fdd8e44
	0x7ff7101c31f2
	0x7ff70fbd3dfb
	0x7ff70fbd1f9e
	0x7ff70fbd3799
	0x7ff70fc806c8
	0x7ff70fc523ea
	0x7ff70fc2a2e5
	0x7ff70fc6c8de
	0x7ff70fc2982a
	0x7ff70fc28b33
	0x7ff70fc29653
	0x7ff70fd122e4
	0x7ff70fd2109c
	0x7ff70fd1ac7f
	0x7ff70fef9b37
	0x7ff70fde46a6
	0x7ff70fddeab4
	0x7ff70fddebf9
	0x7ff70fdd2cbd
	0x7ff964d8259d
	0x7ff96652af78
