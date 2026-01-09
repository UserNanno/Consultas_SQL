ui/main_window.py
import tkinter as tk
from tkinter import ttk, messagebox

from controllers.consulta_controller import ConsultaController
from ui.widgets.document_form import DocumentForm
from ui.widgets.log_panel import LogPanel
from ui.widgets.status_bar import StatusBar
from ui.sbs_credentials_window import SbsCredentialsWindow
from ui.matanalista_window import MatanalistaWindow

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

        self.btn_mat = ttk.Button(actions, text="Matricula", command=self._on_matanalista)
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




controllers/consultacontroller.py
from __future__ import annotations
import threading
from typing import Callable, Optional

from domain.models import ConsultaRequest, ConsultaResult, PersonDocument, DocumentType
from config.product_catalog import PRODUCT_CATALOG, list_desproductos
from main import run_app


def _validate_document(doc: PersonDocument) -> Optional[str]:
    n = (doc.doc_number or "").strip()
    if doc.doc_type == DocumentType.DNI:
        if not n.isdigit() or len(n) != 8:
            return "DNI inválido (debe ser numérico de 8 dígitos)."
        return None
    if doc.doc_type == DocumentType.RUC:
        if not n.isdigit() or len(n) != 11:
            return "RUC inválido (debe ser numérico de 11 dígitos)."
        return None
    if doc.doc_type == DocumentType.CE:
        if len(n) < 6:
            return "CE inválido."
        return None
    if doc.doc_type == DocumentType.PASSPORT:
        if len(n) < 6:
            return "Pasaporte inválido."
        return None
    return "Tipo de documento no soportado."


class ConsultaController:
    def __init__(
        self,
        on_log: Callable[[str], None],
        on_status: Callable[[str], None],
        on_busy: Callable[[bool], None],
        on_success: Callable[[ConsultaResult], None],
        on_error: Callable[[Exception], None],
    ):
        self.on_log = on_log
        self.on_status = on_status
        self.on_busy = on_busy
        self.on_success = on_success
        self.on_error = on_error

    def validate(self, req: ConsultaRequest) -> Optional[str]:
        # ---- documentos ----
        err = _validate_document(req.titular)
        if err:
            return err

        if req.incluir_conyuge:
            if req.conyuge is None:
                return "Marcaste 'Incluir cónyuge' pero no enviaste documento del cónyuge."
            err2 = _validate_document(req.conyuge)
            if err2:
                return f"Cónyuge: {err2}"

        # ---- obligatorios de oportunidad ----
        if not (req.numoportunidad or "").strip():
            return "NUMOPORTUNIDAD es obligatorio."
        if not (req.producto or "").strip():
            return "PRODUCTO es obligatorio."
        if not (req.desproducto or "").strip():
            return "DESPRODUCTO es obligatorio."

        # ---- coherencia con catálogo ----
        producto = req.producto.strip()
        desproducto = req.desproducto.strip()

        if producto not in PRODUCT_CATALOG:
            return f"PRODUCTO inválido: '{producto}'."
        if desproducto not in list_desproductos(producto):
            return f"DESPRODUCTO inválido: '{desproducto}' para PRODUCTO '{producto}'."

        return None

    def run(self, req: ConsultaRequest):
        err = self.validate(req)
        if err:
            self.on_error(ValueError(err))
            return

        self.on_busy(True)
        self.on_status("ejecutando...")
        self.on_log(
            f"Iniciando: Titular={req.titular.doc_type.value} {req.titular.doc_number} | "
            f"Conyuge={'SI' if req.incluir_conyuge else 'NO'} | "
            f"NUMOPORTUNIDAD={req.numoportunidad} | PRODUCTO={req.producto} | DESPRODUCTO={req.desproducto}"
        )

        t = threading.Thread(target=self._worker, args=(req,), daemon=True)
        t.start()

    def _worker(self, req: ConsultaRequest):
        try:
            if req.titular.doc_type != DocumentType.DNI:
                raise ValueError("Por ahora solo está implementado DNI en el motor.")

            dni_titular = req.titular.doc_number.strip()

            dni_conyuge = None
            if req.incluir_conyuge:
                if req.conyuge is None:
                    raise ValueError("No llegó documento del cónyuge.")
                if req.conyuge.doc_type != DocumentType.DNI:
                    raise ValueError("Por ahora el cónyuge solo está implementado para DNI.")
                dni_conyuge = req.conyuge.doc_number.strip()

            out_xlsm, results_dir = run_app(
                dni_titular=dni_titular,
                dni_conyuge=dni_conyuge,
                numoportunidad=req.numoportunidad,
                producto=req.producto,
                desproducto=req.desproducto,
            )

            self.on_success(ConsultaResult(out_xlsm=out_xlsm, results_dir=results_dir))

        except Exception as e:
            self.on_error(e)
        finally:
            self.on_busy(False)
