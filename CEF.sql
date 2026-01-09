    def _ui_success(self, res):
        def _apply():
            self.status.set("finalizado")
            self.log_panel.append(f"OK: {res.out_xlsm}")
            self.log_panel.append(f"Evidencias: {res.results_dir}")

            messagebox.showinfo(
                "Proceso finalizado",
                "✅ El proceso terminó correctamente.\n\n"
                f"Archivo XLSM:\n{res.out_xlsm}\n\n"
                f"Evidencias:\n{res.results_dir}"
            )
        self.after(0, _apply)
