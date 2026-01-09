import socket
import time
import subprocess
import os
from pathlib import Path
from datetime import datetime

from config.settings import EDGE_EXE, DEBUG_PORT


class EdgeDebugLauncher:
    def __init__(self):
        self.process = None
        self.profile_dir = None  # <- guardamos el perfil único por corrida

    def _wait_port(self, host, port, timeout=15):
        start = time.time()
        while time.time() - start < timeout:
            try:
                with socket.create_connection((host, port), timeout=1):
                    return True
            except OSError:
                time.sleep(0.2)
        return False

    def _is_port_open(self, host, port) -> bool:
        try:
            with socket.create_connection((host, port), timeout=0.5):
                return True
        except OSError:
            return False

    def ensure_running(self):
        """
        Lanza Edge con remote debugging SI el puerto no está ocupado.
        Usa user-data-dir único por ejecución para poder matar SOLO ese Edge al final.
        """
        # Si ya está abierto el puerto, no relanzamos (evita duplicados).
        # OJO: si el puerto quedó abierto de una corrida previa "colgada", preferimos fallar
        # para no adjuntarnos a un Edge viejo.
        if self._is_port_open("127.0.0.1", DEBUG_PORT):
            raise RuntimeError(
                f"El puerto {DEBUG_PORT} ya está en uso. "
                "Cierra instancias previas de Edge debug o reinicia el proceso."
            )

        base = Path(os.environ["LOCALAPPDATA"]) / "PrismaProject" / "edge_profile"
        base.mkdir(parents=True, exist_ok=True)

        # ✅ perfil ÚNICO por corrida
        stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.profile_dir = base / f"run_{stamp}"
        self.profile_dir.mkdir(parents=True, exist_ok=True)

        args = [
            EDGE_EXE,
            f"--remote-debugging-port={DEBUG_PORT}",
            f"--user-data-dir={str(self.profile_dir)}",
            "--no-first-run",
            "--no-default-browser-check",
            "--new-window",
            "about:blank",
        ]

        # Lanza Edge (guardamos el process)
        self.process = subprocess.Popen(
            args,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )

        if not self._wait_port("127.0.0.1", DEBUG_PORT, timeout=20):
            raise RuntimeError("Edge no abrió el puerto de debugging")

    def close(self):
        """
        Cierra SOLO el Edge debug de esta corrida.
        Estrategia robusta:
        - matar msedge.exe cuyo CommandLine contenga nuestro user-data-dir y el puerto.
        - luego, como fallback, intentar taskkill al PID si existe.
        """
        # 1) kill por command line (más confiable)
        if self.profile_dir:
            prof = str(self.profile_dir).replace("\\", "\\\\")  # para PowerShell string
            ps = f"""
            $ErrorActionPreference = 'SilentlyContinue';
            $port = '{DEBUG_PORT}';
            $prof = '{prof}';
            $procs = Get-CimInstance Win32_Process -Filter "Name='msedge.exe'";
            foreach ($p in $procs) {{
                $cmd = ($p.CommandLine | Out-String);
                if ($cmd -and ($cmd -like "*--remote-debugging-port=$port*") -and ($cmd -like "*--user-data-dir=$prof*")) {{
                    Stop-Process -Id $p.ProcessId -Force;
                }}
            }}
            """

            try:
                subprocess.run(
                    ["powershell", "-NoProfile", "-ExecutionPolicy", "Bypass", "-Command", ps],
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                    check=False,
                )
            except Exception:
                pass

        # 2) fallback: taskkill por PID (si quedó)
        if self.process is not None:
            try:
                subprocess.run(
                    ["taskkill", "/PID", str(self.process.pid), "/T", "/F"],
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                    check=False,
                )
            except Exception:
                pass

        self.process = None
        self.profile_dir = None
