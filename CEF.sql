def logout_modulo(self):
        """
        1) Click en 'Salir' del módulo /criesgos/logout...
        2) Espera a que cargue la pantalla siguiente
        """
        link = self.wait.until(EC.element_to_be_clickable(self.LINK_SALIR_MODULO))
        link.click()

        # tras salir del módulo, debería aparecer el botón de logout del portal
        self.wait.until(EC.presence_of_element_located(self.BTN_SALIR_PORTAL))

    def logout_portal(self):
        """
        Click en el botón final del portal: goTo('logout')
        """
        btn = self.wait.until(EC.element_to_be_clickable(self.BTN_SALIR_PORTAL))
        btn.click()
