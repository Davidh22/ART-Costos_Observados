# Codigo: Merge subprogramas clasificados con base SECOP + ruralidad
# Proyecto: ART Costeo Unitario
# Descripcion: Une la base de subprogramas (con clasificacion UNSPSC) con el
#              parquet de contratos SECOP enriquecido con datos de ruralidad
#              (DIVIPOLA). El resultado es el insumo principal del pipeline.

rm(list = ls())

library(readxl)
library(arrow)
library(dplyr)
library(tidyr)

# =============================================================================
# CONFIGURACION - ajusta estas rutas
# =============================================================================

RUTA_SUBPROGRAMAS <- "subprogramas_clasificados.xlsx"
RUTA_PARQUET      <- "SECOP_Divipola_Ruralidad.parquet"
RUTA_SALIDA       <- "Base_Final_Suprogramas_Ruralidad.parquet"


# =============================================================================
# CARGA DE DATOS
# =============================================================================

subprogramas <- read_excel(RUTA_SUBPROGRAMAS)
data         <- read_parquet(RUTA_PARQUET)

cat("Subprogramas cargados:", nrow(subprogramas), "filas\n")
cat("Base SECOP cargada   :", nrow(data), "filas\n")


# =============================================================================
# PROCESAMIENTO
# =============================================================================

# Renombrar columnas de clasificacion para el join
colnames(subprogramas)[c(2, 3)] <- c("segmento_sub", "familia_sub")

# Paso 1: Colapsar subprogramas por segmento + familia
# (un contrato puede pertenecer a multiples subprogramas)
subprogramas_unico <- subprogramas %>%
  group_by(segmento_sub, familia_sub) %>%
  summarise(
    subprograma = paste(subprograma, collapse = "} {"),
    .groups = "drop"
  ) %>%
  mutate(subprograma = paste0("{", subprograma, "}"))

# Paso 2: Merge conservando TODAS las filas de la base SECOP
resultado <- data %>%
  left_join(
    subprogramas_unico,
    by = c("nombre_segmento" = "segmento_sub",
           "nombre_familia"  = "familia_sub")
  )

# Verificar que no se perdieron filas
stopifnot(nrow(resultado) == nrow(data))
cat("Join exitoso - filas conservadas:", nrow(resultado), "\n")

# Paso 3: Reemplazar NA por 'Sin clasificar'
resultado <- resultado %>%
  mutate(subprograma = ifelse(is.na(subprograma), "Sin clasificar", subprograma))

# Paso 4: Verificar distribucion
cat("\nDistribucion subprograma Sin clasificar:\n")
print(table(resultado$subprograma == "Sin clasificar"))

# --- Filtros de ejemplo (comentados) ---
# resultado %>% filter(grepl("Acceso a alimentos", subprograma))
# resultado %>% filter(subprograma != "Sin clasificar")


# =============================================================================
# EXPORTACION
# =============================================================================

write_parquet(resultado, RUTA_SALIDA)
cat("\nGuardado:", RUTA_SALIDA, "\n")
cat("Total filas:", nrow(resultado), "\n")
