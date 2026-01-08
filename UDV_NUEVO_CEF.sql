import tkinter as tk
from tkinter import ttk, messagebox

from controllers.consulta_controller import ConsultaController
from ui.widgets.document_form import DocumentForm
from ui.widgets.log_panel import LogPanel
from ui.widgets.status_bar import StatusBar
from ui.sbs_credentials_window import SbsCredentialsWindow
from ui.matanalista_window import MatanalistaWindow  # ✅ NUEVO


class MainWindow(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("Prisma Selenium - Consulta")
        self.geometry("580x390")
        self.resizable(False, False)
        self._build()
        self.controller = ConsultaController(
            on_log=self._ui_log,
            on_status=self._ui_status,
            on_busy=self._ui_busy,
            on_success=self._ui_success,
            on_error=self._ui_error,
        )

    def _build(self):
        root = ttk.Frame(self, padding=12)
        root.pack(fill="both", expand=True)

        self.form = DocumentForm(root)
        self.form.pack(fill="x")

        actions = ttk.Frame(root)
        actions.pack(fill="x", pady=(10, 0))

        self.btn_run = ttk.Button(actions, text="Ejecutar", command=self._on_run)
        self.btn_run.pack(side="left")

        self.btn_sbs = ttk.Button(actions, text="Credenciales SBS", command=self._on_sbs_credentials)
        self.btn_sbs.pack(side="left", padx=(8, 0))

        # ✅ NUEVO BOTÓN MATANALISTA
        self.btn_mat = ttk.Button(actions, text="MATANALISTA", command=self._on_matanalista)
        self.btn_mat.pack(side="left", padx=(8, 0))

        self.status = StatusBar(actions)
        self.status.pack(side="left", padx=(12, 0))

        self.log_panel = LogPanel(root)
        self.log_panel.pack(fill="both", expand=True, pady=(10, 0))

    def _on_sbs_credentials(self):
        SbsCredentialsWindow(self)

    def _on_matanalista(self):
        MatanalistaWindow(self)

    def _on_run(self):
        req = self.form.get_request()
        self.controller.run(req)

    # --- callbacks thread-safe ---
    def _ui_log(self, msg: str):
        self.after(0, lambda: self.log_panel.append(msg))

    def _ui_status(self, text: str):
        self.after(0, lambda: self.status.set(text))

    def _ui_busy(self, busy: bool):
        def _apply():
            self.btn_run.configure(state="disabled" if busy else "normal")
            self.btn_sbs.configure(state="disabled" if busy else "normal")
            self.btn_mat.configure(state="disabled" if busy else "normal")
            self.form.set_busy(busy)
        self.after(0, _apply)

    def _ui_success(self, res):
        def _apply():
            self.status.set("finalizado")
            self.log_panel.append(f"OK: {res.out_xlsm}")
            self.log_panel.append(f"Evidencias: {res.results_dir}")
        self.after(0, _apply)

    def _ui_error(self, e: Exception):
        def _apply():
            self.status.set("error")
            self.log_panel.append(f"ERROR: {repr(e)}")
            messagebox.showerror("Error", str(e))
        self.after(0, _apply)
