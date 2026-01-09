def extract_badge_by_label_contains(self, label_text: str) -> str:
    """
    Busca un list-group-item que contenga label_text y devuelve el número del badge-pill asociado.
    Si el badge dice 'SIN SCORE', devuelve '0'.
    """
    self.wait_not_loading(timeout=40)
    label_text = self._text_norm(label_text)

    xpath = (
        "//li[contains(@class,'list-group-item')]"
        f"[.//div[contains(normalize-space(.), {self._xpath_literal(label_text)})]]"
        "//span[contains(@class,'badge') and contains(@class,'badge-pill')]"
    )
    el = self.wait.until(EC.presence_of_element_located((By.XPATH, xpath)))

    raw = self._text_norm(el.text).upper()

    # ✅ regla: "SIN SCORE" => 0
    if raw == "SIN SCORE":
        return "0"

    return self._text_norm(el.text)
