import logging
import traceback
from functools import wraps

def log_exceptions(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        try:
            return fn(*args, **kwargs)
        except Exception:
            logging.error("EXCEPCION:\n%s", traceback.format_exc())
            raise
    return wrapper
