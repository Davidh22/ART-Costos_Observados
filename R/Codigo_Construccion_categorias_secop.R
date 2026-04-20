# Codigo: Construccion de categorias SECOP (Segmento, Familia, Clase, Producto)
# Proyecto: ART Costeo Unitario
# Descripcion: Lee el parquet de SECOP II y construye las tablas de jerarquia
#              UNSPSC exportadas como Excel.

library(arrow)
library(dplyr)
library(openxlsx)

# --- Cargar datos ---
# Ajusta la ruta al parquet de SECOP en tu equipo
data <- read_parquet("SECOP_contratos_cruce_clasificador_productos.parquet")

# --- Conteos de referencia ---
segmentos <- as.data.frame(table(data$nombre_segmento))
familias  <- as.data.frame(table(data$nombre_familia))
clases    <- as.data.frame(table(data$nombre_clase))
productos <- as.data.frame(table(data$nombre_producto))

# --- BASE 1: Segmento + Familia ---
seg_fam <- data %>%
  select(nombre_segmento, nombre_familia) %>%
  distinct() %>%
  arrange(nombre_segmento, nombre_familia)

# --- BASE 2: Segmento + Familia + Clase ---
seg_fam_cla <- data %>%
  select(nombre_segmento, nombre_familia, nombre_clase) %>%
  distinct() %>%
  arrange(nombre_segmento, nombre_familia, nombre_clase)

# --- BASE 3: Jerarquia completa ---
jerarquia_completa <- data %>%
  select(nombre_segmento, nombre_familia, nombre_clase, nombre_producto) %>%
  distinct() %>%
  arrange(nombre_segmento, nombre_familia, nombre_clase, nombre_producto)

# --- Exportar ---
write.xlsx(seg_fam,            file = "seg_fam.xlsx")
write.xlsx(seg_fam_cla,        file = "seg_fam_cla.xlsx")
write.xlsx(jerarquia_completa, file = "jerarquia_completa.xlsx")

cat("Exportados:\n")
cat("  seg_fam.xlsx           :", nrow(seg_fam), "filas\n")
cat("  seg_fam_cla.xlsx       :", nrow(seg_fam_cla), "filas\n")
cat("  jerarquia_completa.xlsx:", nrow(jerarquia_completa), "filas\n")
