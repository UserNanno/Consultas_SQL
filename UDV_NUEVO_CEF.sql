import tkinter as tk
from tkinter import ttk, messagebox

from config.credentials_store import load_sbs_credentials, save_sbs_credentials


class SbsCredentialsWindow(tk.Toplevel):
    def __init__(self, master=None):
        super().__init__(master)
        self.title("Credenciales SBS")
        self.resizable(False, False)
        self.transient(master)   # ventana encima del parent
        self.grab_set()          # modal

        user, pwd = load_sbs_credentials("", "")

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
        self.destroy()
