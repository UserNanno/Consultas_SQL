
from __future__ import annotations
import tkinter as tk
from tkinter import ttk

from domain.models import ConsultaRequest, PersonDocument, DocumentType
from config.product_catalog import list_productos, list_desproductos


class DocumentForm(ttk.Frame):
    def __init__(self, master):
        super().__init__(master)

        self.var_titular = tk.StringVar()
        self.var_use_spouse = tk.BooleanVar(value=False)
        self.var_spouse = tk.StringVar()

        self.var_numoportunidad = tk.StringVar()
        self.var_producto = tk.StringVar()
        self.var_desproducto = tk.StringVar()

        self._build()

    def _build(self):
        # ------------------------
        # Documentos
        # ------------------------
        ttk.Label(self, text="Documento Titular (DNI - 8 dígitos):").grid(row=0, column=0, sticky="w")
        self.ent_tit = ttk.Entry(self, textvariable=self.var_titular, width=24)
        self.ent_tit.grid(row=0, column=1, sticky="w", padx=(8, 0))

        self.chk = ttk.Checkbutton(
            self,
            text="Incluir cónyuge",
            variable=self.var_use_spouse,
            command=self._toggle_spouse
        )
        self.chk.grid(row=1, column=0, sticky="w", columnspan=2, pady=(6, 0))

        ttk.Label(self, text="Documento Cónyuge (DNI - 8 dígitos):").grid(row=2, column=0, sticky="w", pady=(6, 0))
        self.ent_sp = ttk.Entry(self, textvariable=self.var_spouse, width=24, state="disabled")
        self.ent_sp.grid(row=2, column=1, sticky="w", padx=(8, 0), pady=(6, 0))

        # ------------------------
        # Datos de oportunidad (obligatorio)
        # ------------------------
        ttk.Separator(self, orient="horizontal").grid(row=3, column=0, columnspan=2, sticky="ew", pady=(10, 10))

        ttk.Label(self, text="NUMOPORTUNIDAD (obligatorio):").grid(row=4, column=0, sticky="w")
        self.ent_numop = ttk.Entry(self, textvariable=self.var_numoportunidad, width=24)
        self.ent_numop.grid(row=4, column=1, sticky="w", padx=(8, 0))

        ttk.Label(self, text="PRODUCTO (obligatorio):").grid(row=5, column=0, sticky="w", pady=(6, 0))
        self.cmb_producto = ttk.Combobox(
            self,
            textvariable=self.var_producto,
            values=list_productos(),
            state="readonly",
            width=22
        )
        self.cmb_producto.grid(row=5, column=1, sticky="w", padx=(8, 0), pady=(6, 0))
        self.cmb_producto.bind("<<ComboboxSelected>>", self._on_producto_changed)

        ttk.Label(self, text="DESPRODUCTO (obligatorio):").grid(row=6, column=0, sticky="w", pady=(6, 0))
        self.cmb_desproducto = ttk.Combobox(
            self,
            textvariable=self.var_desproducto,
            values=[],
            state="readonly",
            width=22
        )
        self.cmb_desproducto.grid(row=6, column=1, sticky="w", padx=(8, 0), pady=(6, 0))

        # defaults (para que combobox tenga selección inicial)
        productos = list_productos()
        if productos:
            self.var_producto.set(productos[0])
            self._refresh_desproductos()

        self.columnconfigure(1, weight=1)

    def _toggle_spouse(self):
        if self.var_use_spouse.get():
            self.ent_sp.configure(state="normal")
        else:
            self.ent_sp.configure(state="disabled")
            self.var_spouse.set("")

    def _on_producto_changed(self, event=None):
        self._refresh_desproductos()

    def _refresh_desproductos(self):
        producto = (self.var_producto.get() or "").strip()
        options = list_desproductos(producto)
        self.cmb_desproducto["values"] = options
        self.var_desproducto.set(options[0] if options else "")

    def get_request(self) -> ConsultaRequest:
        # -------- documentos --------
        titular_doc = PersonDocument(DocumentType.DNI, (self.var_titular.get() or "").strip())
        incluir = bool(self.var_use_spouse.get())

        cony = None
        if incluir:
            cony = PersonDocument(DocumentType.DNI, (self.var_spouse.get() or "").strip())

        # -------- obligatorios --------
        numop = (self.var_numoportunidad.get() or "").strip()
        producto = (self.var_producto.get() or "").strip()
        desproducto = (self.var_desproducto.get() or "").strip()

        # Validación rápida (UI-level) para cortar antes
        if not numop:
            raise ValueError("NUMOPORTUNIDAD es obligatorio.")
        if not producto:
            raise ValueError("PRODUCTO es obligatorio.")
        if not desproducto:
            raise ValueError("DESPRODUCTO es obligatorio.")

        return ConsultaRequest(
            titular=titular_doc,
            incluir_conyuge=incluir,
            conyuge=cony,
            numoportunidad=numop,
            producto=producto,
            desproducto=desproducto,
        )

    def set_busy(self, busy: bool):
        state = "disabled" if busy else "normal"

        self.ent_tit.configure(state=state)
        self.chk.configure(state=state)

        if self.var_use_spouse.get():
            self.ent_sp.configure(state=state)
        else:
            self.ent_sp.configure(state="disabled")

        # ✅ nuevos campos
        self.ent_numop.configure(state=state)
        self.cmb_producto.configure(state="disabled" if busy else "readonly")
        self.cmb_desproducto.configure(state="disabled" if busy else "readonly")
