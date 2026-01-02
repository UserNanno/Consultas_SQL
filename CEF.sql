    finally:
        # Cierra el webdriver (tabs controladas)
        try:
            driver.quit()
        except Exception:
            pass

        # Cierra Edge (el proceso que abriste con debugging)
        try:
            launcher.close()
        except Exception:
            pass








import socket
import time
import subprocess
import os
from pathlib import Path

from config.settings import EDGE_EXE, DEBUG_PORT


class EdgeDebugLauncher:
    def __init__(self):
        self.process = None

    def _wait_port(self, host, port, timeout=15):
        start = time.time()
        while time.time() - start < timeout:
            try:
                with socket.create_connection((host, port), timeout=1):
                    return True
            except OSError:
                time.sleep(0.2)
        return False

    def ensure_running(self):
        profile_dir = Path(os.environ["LOCALAPPDATA"]) / "PrismaProject" / "edge_profile"
        profile_dir.mkdir(parents=True, exist_ok=True)

        # Lanza Edge (guardamos el process para poder cerrarlo al final)
        self.process = subprocess.Popen([
            EDGE_EXE,
            f"--remote-debugging-port={DEBUG_PORT}",
            f"--user-data-dir={profile_dir}",
            "--start-maximized",
            "--new-window",
        ], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

        if not self._wait_port("127.0.0.1", DEBUG_PORT, timeout=20):
            raise RuntimeError("Edge no abrió el puerto de debugging")

    def close(self):
        """
        Cierra el Edge que abrió este launcher.
        Importante: esto mata el proceso y sus hijos (/T).
        """
        if self.process is None:
            return

        try:
            subprocess.run(
                ["taskkill", "/PID", str(self.process.pid), "/T", "/F"],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                check=False
            )
        finally:
            self.process = None
