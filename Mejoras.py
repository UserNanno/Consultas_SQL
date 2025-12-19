for c in df_sf_estados_raw.columns:
    if "estado" in c.lower() and "paso" in c.lower():
        print(repr(c))
