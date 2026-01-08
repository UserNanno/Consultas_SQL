import tkinter as tk
from tkinter import ttk, messagebox

from config.analyst_store import load_matanalista, save_matanalista


class MatanalistaWindow(tk.Toplevel):
    def __init__(self, master):
        super().__init__(master)
        self.title("Matrícula Analista")
        self.resizable(False, False)

        self.var_mat = tk.StringVar(value=load_matanalista(""))

        frm = ttk.Frame(self, padding=12)
        frm.pack(fill="both", expand=True)

        ttk.Label(frm, text="MATANALISTA (obligatorio):").grid(row=0, column=0, sticky="w")
        ent = ttk.Entry(frm, textvariable=self.var_mat, width=26)
        ent.grid(row=1, column=0, sticky="w", pady=(6, 0))
        ent.focus_set()

        btns = ttk.Frame(frm)
        btns.grid(row=2, column=0, sticky="w", pady=(12, 0))

        ttk.Button(btns, text="Guardar", command=self._save).pack(side="left")
        ttk.Button(btns, text="Cerrar", command=self.destroy).pack(side="left", padx=(8, 0))

        self.bind("<Return>", lambda e: self._save())

        # modal
        self.transient(master)
        self.grab_set()

    def _save(self):
        mat = (self.var_mat.get() or "").strip()
        if not mat:
            messagebox.showerror("Validación", "MATANALISTA es obligatorio.")
            return

        # Normalizamos (opcional): uppercase sin espacios
        mat = mat.upper().replace(" ", "")
        save_matanalista(mat)
        messagebox.showinfo("OK", "MATANALISTA guardado.")
        self.destroy()
