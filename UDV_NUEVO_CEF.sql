import tkinter as tk
from tkinter import ttk, messagebox

from config.settings import USUARIO, CLAVE
from config.credentials_store import load_sbs_credentials, save_sbs_credentials


class SbsCredentialsWindow(tk.Toplevel):
    def __init__(self, master=None):
        super().__init__(master)
        self.title("Credenciales SBS")
        self.resizable(False, False)
        self.transient(master)   # ventana encima del parent
        self.grab_set()          # modal (opcional, recomendado)

        user, pwd = load_sbs_credentials(USUARIO, CLAVE)

        root = ttk.Frame(self, padding=12)
        root.pack(fill="both", expand=True)

        ttk.Label(root, text="SBS", font=("Segoe UI", 11, "bold")).grid(
            row=0, column=0, columnspan=2, sticky="w", pady=(0, 10)
        )

        ttk.Label(root, text="Usuario").grid(row=1, column=0, sticky="w")
        self.var_user = tk.StringVar(value=user)
        self.ent_user = ttk.Entry(root, textvariable=self.var_user, width=28, state="disabled")
        self.ent_user.grid(row=2, column=0, sticky="w", padx=(0, 10))

        ttk.Label(root, text="Contraseña").grid(row=1, column=1, sticky="w")
        self.var_pwd = tk.StringVar(value=pwd)
        self.ent_pwd = ttk.Entry(root, textvariable=self.var_pwd, width=28, show="*", state="disabled")
        self.ent_pwd.grid(row=2, column=1, sticky="w")

        actions = ttk.Frame(root)
        actions.grid(row=3, column=0, columnspan=2, sticky="e", pady=(12, 0))

        self.btn_edit = ttk.Button(actions, text="Editar", command=self._toggle_edit)
        self.btn_edit.pack(side="left")

        self.btn_save = ttk.Button(actions, text="Guardar", state="disabled", command=self._save)
        self.btn_save.pack(side="left", padx=(8, 0))

        self._editing = False

    def _toggle_edit(self):
        self._editing = not self._editing
        state = "normal" if self._editing else "disabled"
        self.ent_user.config(state=state)
        self.ent_pwd.config(state=state)
        self.btn_save.config(state=("normal" if self._editing else "disabled"))
        self.btn_edit.config(text=("Cancelar" if self._editing else "Editar"))

        if self._editing:
            self.ent_user.focus_set()

    def _save(self):
        user = (self.var_user.get() or "").strip()
        pwd = (self.var_pwd.get() or "").strip()

        if not user or not pwd:
            messagebox.showwarning("Validación", "Usuario y contraseña no pueden estar vacíos.")
            return

        save_sbs_credentials(user, pwd)
        messagebox.showinfo("OK", "Credenciales SBS guardadas.")
        self._toggle_edit()
        self.destroy()

























import tkinter as tk
from tkinter import ttk, messagebox

from controllers.consulta_controller import ConsultaController
from ui.widgets.document_form import DocumentForm
from ui.widgets.log_panel import LogPanel
from ui.widgets.status_bar import StatusBar
from ui.sbs_credentials_window import SbsCredentialsWindow  # <-- NUEVO


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

        # ✅ NUEVO BOTÓN
        self.btn_sbs = ttk.Button(actions, text="Credenciales SBS", command=self._on_sbs_credentials)
        self.btn_sbs.pack(side="left", padx=(8, 0))

        self.status = StatusBar(actions)
        self.status.pack(side="left", padx=(12, 0))

        self.log_panel = LogPanel(root)
        self.log_panel.pack(fill="both", expand=True, pady=(10, 0))

    def _on_sbs_credentials(self):
        # abre la ventana modal
        SbsCredentialsWindow(self)

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
            self.btn_sbs.configure(state="disabled" if busy else "normal")  # <-- opcional, evita editar en ejecución
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
