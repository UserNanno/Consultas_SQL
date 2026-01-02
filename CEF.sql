    finally:
        # Cierra el webdriver (tabs controladas)
        try:
            driver.quit()
        except Exception:
            pass

        # Cierra Edge (el proceso que abriste con debugging)
        try:
            launcher.close()
        except Exception:
            pass
