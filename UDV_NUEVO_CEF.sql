Este es mi ui/widgets/document_form.py
from __future__ import annotations
import tkinter as tk
from tkinter import ttk
from domain.models import ConsultaRequest, PersonDocument, DocumentType

class DocumentForm(ttk.Frame):
   def __init__(self, master):
       super().__init__(master)
       self.var_titular = tk.StringVar()
       self.var_use_spouse = tk.BooleanVar(value=False)
       self.var_spouse = tk.StringVar()
       self._build()
   def _build(self):
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
       self.columnconfigure(1, weight=1)
   def _toggle_spouse(self):
       if self.var_use_spouse.get():
           self.ent_sp.configure(state="normal")
       else:
           self.ent_sp.configure(state="disabled")
           self.var_spouse.set("")
   def get_request(self) -> ConsultaRequest:
       titular_doc = PersonDocument(DocumentType.DNI, (self.var_titular.get() or "").strip())
       incluir = bool(self.var_use_spouse.get())
       cony = None
       if incluir:
           cony = PersonDocument(DocumentType.DNI, (self.var_spouse.get() or "").strip())
       return ConsultaRequest(
           titular=titular_doc,
           incluir_conyuge=incluir,
           conyuge=cony
       )
   def set_busy(self, busy: bool):
       state = "disabled" if busy else "normal"
       self.ent_tit.configure(state=state)
       self.chk.configure(state=state)
       if self.var_use_spouse.get():
           self.ent_sp.configure(state=state)
       else:
           self.ent_sp.configure(state="disabled")







Y este es mi controllers/consulta_controller.py
from __future__ import annotations
import threading
from typing import Callable, Optional
from domain.models import ConsultaRequest, ConsultaResult, PersonDocument, DocumentType
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
       # regla mínima por ahora (ajustable)
       if len(n) < 6:
           return "CE inválido."
       return None
   if doc.doc_type == DocumentType.PASSPORT:
       # regla mínima por ahora (ajustable)
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
       err = _validate_document(req.titular)
       if err:
           return err
       if req.incluir_conyuge:
           if req.conyuge is None:
               return "Marcaste 'Incluir cónyuge' pero no enviaste documento del cónyuge."
           err2 = _validate_document(req.conyuge)
           if err2:
               return f"Cónyuge: {err2}"
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
           f"Conyuge={'SI' if req.incluir_conyuge else 'NO'}"
       )
       t = threading.Thread(target=self._worker, args=(req,), daemon=True)
       t.start()
   def _worker(self, req: ConsultaRequest):
       try:
           # Si mañana soportas más tipos, aquí haces el switch y llamas a la función correcta.
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
           out_xlsm, results_dir = run_app(dni_titular, dni_conyuge)
           self.on_success(ConsultaResult(out_xlsm=out_xlsm, results_dir=results_dir))
       except Exception as e:
           self.on_error(e)
       finally:
           self.on_busy(False)



services/rbm_flow.py
from pathlib import Path
import logging

from pages.rbm.rbm_page import RbmPage
from selenium.common.exceptions import TimeoutException, WebDriverException


class RbmFlow:
    def __init__(self, driver):
        self.page = RbmPage(driver)
        self.driver = driver  # para screenshot en error

    def run(self, dni: str, consumos_img_path: Path, cem_img_path: Path):
        try:
            self.page.open()

            self.page.consultar_dni(dni)
            inicio_fields = self.page.extract_inicio_fields()
            self.page.screenshot_panel_body_cdp(consumos_img_path)

            self.page.go_cem_tab()
            cem_3cols = self.page.extract_cem_3cols()
            self.page.screenshot_panel_body_cdp(cem_img_path)

            out = {"ok": True, "inicio": inicio_fields, "cem": cem_3cols}
            logging.info("RBM flow return: %s", out)
            return out

        except (TimeoutException, WebDriverException) as e:
            logging.warning("[RBM] No disponible (soft-fail). dni=%s | err=%s", dni, repr(e))
            logging.warning("[RBM] url=%s | title=%s", self.driver.current_url, self.driver.title)

            # Evidencia (siempre útil)
            try:
                err_path = Path(consumos_img_path).with_name("rbm_error.png")
                self.driver.save_screenshot(str(err_path))
                logging.info("[RBM] Screenshot error guardado: %s", err_path)
            except Exception:
                pass

            return {
                "ok": False,
                "error": "RBM_NO_DISPONIBLE",
                "error_detail": repr(e),
                "inicio": {},
                "cem": {},
            }
