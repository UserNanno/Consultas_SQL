    def extract_scores_por_producto(self, producto: str, desproducto: str | None = None) -> dict:
        """
        Reglas explícitas por catálogo:

        CREDITO EFECTIVO:
            - LIBRE DISPONIBILIDAD                  -> Venta CEF LD/RE
            - COMPRA DE DEUDA                      -> Venta CEF CdD
            - LD + CONSOLIDACION                   -> Venta CEF LD/RE
            - LD + COMPRA DE DUDA Y/O CONSOLIDACION -> Venta CEF CdD
            - C83 siempre: Portafolio CEF (Score BHV)

        TARJETA DE CREDITO:
            - TARJETA NUEVA -> Venta TC Nueva
            - C83 = None
        """

        self.wait_not_loading(timeout=40)

        p = (producto or "").strip().upper()
        d = " ".join((desproducto or "").strip().upper().split())  # normaliza espacios

        out = {"inicio_c14": None, "inicio_c83": None}

        if p == "CREDITO EFECTIVO":

            if d == "LIBRE DISPONIBILIDAD":
                out["inicio_c14"] = self.extract_badge_by_label_contains("Venta CEF LD/RE")

            elif d == "COMPRA DE DEUDA":
                out["inicio_c14"] = self.extract_badge_by_label_contains("Venta CEF CdD")

            elif d == "LD + CONSOLIDACION":
                out["inicio_c14"] = self.extract_badge_by_label_contains("Venta CEF LD/RE")

            elif d == "LD + COMPRA DE DUDA Y/O CONSOLIDACION":
                out["inicio_c14"] = self.extract_badge_by_label_contains("Venta CEF CdD")

            else:
                logging.warning("RBM DESPRODUCTO no reconocido para CREDITO EFECTIVO: %s", d)
                out["inicio_c14"] = None

            # Siempre para Crédito Efectivo
            out["inicio_c83"] = self.extract_badge_by_label_contains("Portafolio CEF (Score BHV)")

        elif p == "TARJETA DE CREDITO":

            if d == "TARJETA NUEVA":
                out["inicio_c14"] = self.extract_badge_by_label_contains("Venta TC Nueva")
            else:
                logging.warning("RBM DESPRODUCTO no reconocido para TARJETA DE CREDITO: %s", d)
                out["inicio_c14"] = None

            out["inicio_c83"] = None

        else:
            logging.warning("RBM PRODUCTO no reconocido: %s", p)

        logging.info("RBM extract_scores_por_producto: %s", out)
        return out
