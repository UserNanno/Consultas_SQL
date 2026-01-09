    def extract_scores_por_producto(self, producto: str, desproducto: str | None = None) -> dict:
        """
        Reglas:
        - Si producto == 'CREDITO EFECTIVO':
            - C14 depende de DESPRODUCTO:
                * Si contiene 'COMPRA DE DEUDA' (en cualquier variante) -> 'Venta CEF CdD'
                * Caso contrario -> 'Venta CEF LD/RE'
            - C83 siempre: 'Portafolio CEF (Score BHV)'
        - Si producto == 'TARJETA DE CREDITO':
            - C14: 'Venta TC Nueva'
            - C83: None
        """
        self.wait_not_loading(timeout=40)

        p = (producto or "").strip().upper()
        d = " ".join((desproducto or "").strip().upper().split())  # normaliza espacios

        out = {"inicio_c14": None, "inicio_c83": None}

        if p == "CREDITO EFECTIVO":
            if "COMPRA DE DEUDA" in d:
                out["inicio_c14"] = self.extract_badge_by_label_contains("Venta CEF CdD")
            else:
                out["inicio_c14"] = self.extract_badge_by_label_contains("Venta CEF LD/RE")

            out["inicio_c83"] = self.extract_badge_by_label_contains("Portafolio CEF (Score BHV)")

        elif p == "TARJETA DE CREDITO":
            out["inicio_c14"] = self.extract_badge_by_label_contains("Venta TC Nueva")
            out["inicio_c83"] = None

        logging.info("RBM extract_scores_por_producto: %s", out)
        return out



aca quiero que no se acon IN, qu sea un IF por cada elemento del catlaogo
PRODUCT_CATALOG = {
    "CREDITO EFECTIVO": [
        "LIBRE DISPONIBILIDAD",
        "COMPRA DE DEUDA",
        "LD + CONSOLIDACION",
        "LD + COMPRA DE DUDA Y/O CONSOLIDACION",
    ],
    "TARJETA DE CREDITO": [
        "TARJETA NUEVA",

    ],
}


