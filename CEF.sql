def build_driver(
    headless: bool = False,
    user_data_dir: Optional[str] = None,
    profile_dir: Optional[str] = None,
) -> webdriver.Chrome:

    opts = Options()

    # ⚡ Optimización
    opts.add_argument("--disable-gpu")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-extensions")
    opts.add_argument("--disable-notifications")
    opts.add_argument("--disable-popup-blocking")
    opts.add_argument("--blink-settings=imagesEnabled=false")
    opts.add_argument("--window-size=1200,800")

    # Reusar perfil (opcional)
    if user_data_dir:
        opts.add_argument(f"--user-data-dir={user_data_dir}")
    if profile_dir:
        opts.add_argument(f"--profile-directory={profile_dir}")

    if headless:
        opts.add_argument("--headless=new")

    # ✅ Selenium 4: capability aquí
    opts.set_capability("pageLoadStrategy", "eager")

    # Servicio EXPLÍCITO (NO Selenium Manager)
    service = Service(executable_path=CHROMEDRIVER_PATH)

    driver = webdriver.Chrome(
        service=service,
        options=opts,
    )

    driver.set_page_load_timeout(20)
    driver.set_script_timeout(20)

    return driver
