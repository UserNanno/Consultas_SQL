# config/product_catalog.py

# PRODUCTO -> lista de DESPRODUCTO
PRODUCT_CATALOG = {
    "HIPOTECARIO": [
        "HIPOTECARIO TRADICIONAL",
        "MI VIVIENDA",
        "REPROGRAMADO",
    ],
    "VEHICULAR": [
        "VEHICULAR NUEVO",
        "VEHICULAR USADO",
    ],
    "TARJETA": [
        "TARJETA CLASICA",
        "TARJETA ORO",
        "TARJETA PLATINUM",
    ],
    "PYME": [
        "CAPITAL DE TRABAJO",
        "INVERSION",
    ],
}

def list_productos() -> list[str]:
    return sorted(PRODUCT_CATALOG.keys())

def list_desproductos(producto: str) -> list[str]:
    return PRODUCT_CATALOG.get(producto, [])










