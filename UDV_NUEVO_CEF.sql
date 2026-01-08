sbs_credentials_window.py

import tkinter as tk
from tkinter import messagebox
from config.credentials_store import load_sbs_credentials, save_sbs_credentials
from config.settings import USUARIO, CLAVE

class SbsCredentialsWindow(tk.Toplevel):
    def __init__(self, master=None):
        super().__init__(master)
        self.title("Credenciales SBS")
        self.resizable(False, False)

        # Cargar actuales (guardadas o defaults)
        user, pwd = load_sbs_credentials(USUARIO, CLAVE)

        # --- UI ---
        frm = tk.Frame(self, padx=12, pady=12)
        frm.pack(fill="both", expand=True)

        tk.Label(frm, text="SBS", font=("Segoe UI", 11, "bold")).grid(row=0, column=0, columnspan=2, sticky="w", pady=(0, 8))

        tk.Label(frm, text="Usuario").grid(row=1, column=0, sticky="w")
        self.var_user = tk.StringVar(value=user)
        self.ent_user = tk.Entry(frm, textvariable=self.var_user, width=28, state="disabled")
        self.ent_user.grid(row=2, column=0, sticky="w", padx=(0, 10))

        tk.Label(frm, text="Contraseña").grid(row=1, column=1, sticky="w")
        self.var_pwd = tk.StringVar(value=pwd)
        self.ent_pwd = tk.Entry(frm, textvariable=self.var_pwd, width=28, show="*", state="disabled")
        self.ent_pwd.grid(row=2, column=1, sticky="w")

        self.btn_edit = tk.Button(frm, text="Editar", width=10, command=self._toggle_edit)
        self.btn_edit.grid(row=3, column=0, sticky="w", pady=(10, 0))

        self.btn_save = tk.Button(frm, text="Guardar", width=10, state="disabled", command=self._save)
        self.btn_save.grid(row=3, column=1, sticky="e", pady=(10, 0))

        self._editing = False

    def _toggle_edit(self):
        self._editing = not self._editing
        state = "normal" if self._editing else "disabled"
        self.ent_user.config(state=state)
        self.ent_pwd.config(state=state)
        self.btn_save.config(state=("normal" if self._editing else "disabled"))
        self.btn_edit.config(text=("Cancelar" if self._editing else "Editar"))

    def _save(self):
        user = self.var_user.get().strip()
        pwd = self.var_pwd.get().strip()

        if not user or not pwd:
            messagebox.showwarning("Validación", "Usuario y contraseña no pueden estar vacíos.")
            return

        save_sbs_credentials(user, pwd)
        messagebox.showinfo("OK", "Credenciales SBS guardadas.")
        self._toggle_edit()
