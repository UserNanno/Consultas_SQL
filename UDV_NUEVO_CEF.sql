    def expand_all_rectificaciones(self, expected: int = 2):
        """
        Hace click en las flechas 'arrow' dentro de #expand.
        Por defecto intentamos expandir 2 (como mencionaste).
        """
        # Esperar que existan flechas (si no hay, simplemente no hace nada)
        try:
            self.wait.until(lambda d: len(d.find_elements(*self.ARROWS)) > 0)
        except TimeoutException:
            return
        arrows = self.driver.find_elements(*self.ARROWS)
        # click a las primeras `expected` flechas
        for i, arrow in enumerate(arrows[:expected]):
            try:
                self.driver.execute_script("arguments[0].scrollIntoView({block:'center'});", arrow)
                try:
                    arrow.click()
                except Exception:
                    self.driver.execute_script("arguments[0].click();", arrow)
                # Esperar que se muestre la fila detalle siguiente (tr.Vde) asociada:
                # la estructura suele ser: tr.master + tr.Vde (display:none -> display:table-row)
                def _expanded(d):
                    try:
                        master_tr = arrow.find_element(By.XPATH, "./ancestor::tr[contains(@class,'master')]")
                        detail_tr = master_tr.find_element(By.XPATH, "following-sibling::tr[contains(@class,'Vde')][1]")
                        style = (detail_tr.get_attribute("style") or "").lower()
                        # cuando se despliega, suele quitar display:none o poner display:table-row
                        return "display: none" not in style
                    except Exception:
                        return True  # si no podemos verificar, no bloqueamos el flujo
                self.wait.until(_expanded)
            except Exception:
                # no bloqueamos por un arrow que falle
                continue
