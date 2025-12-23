Cuando se ejecutó esto
# 1) STAGING
df_org = load_organico(spark, BASE_DIR_ORGANICO)
df_org_tokens = build_org_tokens(df_org)
df_estados = load_sf_estados(spark, PATH_SF_ESTADOS)
df_productos = load_sf_productos_validos(spark, PATH_SF_PRODUCTOS)
df_apps = load_powerapps(spark, PATH_PA_SOLICITUDES)
# 2) ENRIQUECIMIENTO CON ORGANICO
df_estados_enriq = enrich_estados_con_organico(df_estados, df_org_tokens)
df_productos_enriq = enrich_productos_con_organico(df_productos, df_org_tokens)

Hice lo siguiente

display(df_estados_enriq[df_estados_enriq["CODSOLICITUD"]== "O0018169329"])

aca me sale en el primer registro:

NBRULTACTOR -> LESLY
NBRULTACTORPASO -> LESLY
MATORGANICO -> S18795 (MATRICULA DE SUPERVISORA DE LESLY)
MATSUPERIOR -> U17293 (MATRICULA DEL GERENTE)
MATORGANICOPASO -> S18795
MATSUPERIORPASO -> U17293

en el segundo registro me sale

NBRULTACTOR -> EVELYN (sUPERVISOR)
NBRULTACTORPASO -> LESLY
MATORGANICO -> S18795 (MATRICULA DE SUPERVISORA DE LESLY)
MATSUPERIOR -> U17293 (MATRICULA DEL GERENTE)
MATORGANICOPASO -> S18795
MATSUPERIORPASO -> U17293


EL PROBLEMA ES ACÁ QUE NO PEGA BIEN SU MATRÍCULA DEL ANALISTA. POR QUÉ PASA ESO? 
