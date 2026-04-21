# ART — Costos Observados de Subprogramas PDET

> **Pipeline automatizado para el costeo unitario indicativo de los Planes de Acción para la Transformación Regional (PATR)**, desarrollado en el marco de la consultoría 2026 para la **Agencia de Renovación del Territorio (ART)** de Colombia.

---

## 1. Contexto del Proyecto

Los **Programas de Desarrollo con Enfoque Territorial (PDET)** son el instrumento de planificación rural para transformar los **170 municipios** más afectados por el conflicto armado en Colombia, organizados en **16 subregiones** (Alto Patía y Norte del Cauca, Catatumbo, Sur de Bolívar, Macarena–Guaviare, Montes de María, etc.). Los PATR —uno por cada subregión— contienen **subprogramas e indicadores** de política pública que materializan los compromisos del Acuerdo Final de Paz.

La **ART**, como entidad rectora del PDET, requiere estimar **costos indicativos** para los capítulos de programas y proyectos de los PATR que están en proceso de revisión y actualización, en concordancia con la línea "Información y Prospectiva" del PPO. El ejercicio debe partir de **costos observados reales** de contratos ejecutados en los territorios PDET.

### El problema central

SECOP II (el repositorio nacional de contratación pública colombiana, con ~5 millones de registros) **no publica cantidades ni precios unitarios** — solo el valor total de cada contrato. Además, no existe un campo directo "PDET = Sí/No": la variable `espostconflicto` cubre apenas el 0.7% de los contratos y deja por fuera la mayoría de la inversión PDET. Por lo tanto, el costeo unitario **no puede hacerse desde la API de SECOP** y obliga a procesar miles de documentos contractuales (Estudios Previos, Anexos Técnicos, Pliegos) que sí contienen las cantidades y especificaciones técnicas.

### La solución

Este repositorio implementa un **pipeline end-to-end** que:

1. Acota el universo de contratos al ámbito PDET mediante filtros geográficos (170 municipios PDET) y semánticos (keywords por indicador).
2. Descarga masivamente los documentos contractuales desde SECOP II.
3. Usa **Claude (Anthropic)** para extraer del texto/imagen de cada PDF los datos de costeo: cantidad, unidad física, precio total y costo unitario calculado.
4. Valida semánticamente que cada contrato sea efectivamente relevante para el indicador PDET correspondiente.
5. Reconstruye la trazabilidad completa: PDF → contrato SECOP → indicador PDET → subprograma → subregión.

---

## 2. Arquitectura del Pipeline

```
  ┌──────────────────────────────────────────────────────────────────┐
  │   Base_Indicadores_Final (Excel) — indicadores priorizados       │
  │   por subregión × subprograma (PATR)                             │
  └─────────────────────────┬────────────────────────────────────────┘
                            │
                            ▼
  ┌──────────────────────────────────────────────────────────────────┐
  │  [1] secop_fase1_v4_iterativo_5.py                               │
  │                                                                  │
  │  • Genera keywords con Claude a partir del nombre del indicador  │
  │  • Consulta la API de SECOP II con 4 capas geográficas:          │
  │        Capa 1: municipios PDET de la subregión                   │
  │        Capa 2: departamento de la subregión                      │
  │        Capa 3: otras subregiones PDET (factor de ajuste)         │
  │        Capa 4: nacional (referente de último recurso)            │
  │  • Persiste todo en SQLite + exporta Excel de 8 hojas            │
  └─────────────────────────┬────────────────────────────────────────┘
                            │
                            ▼
  ┌──────────────────────────────────────────────────────────────────┐
  │  [2] validar_contratos_llm.py                                    │
  │                                                                  │
  │  • Toma cada contrato encontrado y pregunta a Claude:            │
  │    "¿el objeto de este contrato es realmente relevante para      │
  │     el indicador al que fue asociado?"                           │
  │  • Marca validado_llm = 1 (relevante) / 0 (no relevante)         │
  │  • Lotes de 12 contratos para minimizar costo de API             │
  └─────────────────────────┬────────────────────────────────────────┘
                            │
                            ▼
  ┌──────────────────────────────────────────────────────────────────┐
  │  [3] secop_descargador_indicadores_final.py                      │
  │                                                                  │
  │  • Lee los 4 Excel de indicadores (GA1..GA4)                     │
  │  • Usa Selenium + Chrome para navegar cada URL SECOP II          │
  │  • Clasifica documentos por tipo (Estudio Previo / Anexo Técnico │
  │    / Secundario) y descarga los prioritarios                     │
  │  • Organiza en carpetas: descargas_indicadores/GAx/CO1.NTC.XXXX/ │
  │  • Genera ZIP final + informe Excel                              │
  └─────────────────────────┬────────────────────────────────────────┘
                            │
                            ▼
  ┌──────────────────────────────────────────────────────────────────┐
  │  [4] extraer_costeo_pdfs_2.py                                    │
  │                                                                  │
  │  • Envía cada PDF a Claude Sonnet (base64, API Messages)         │
  │  • Extrae 13 campos: código de contrato, descripción, municipio, │
  │    departamento, subregión, año, precio_cop, moneda, cantidad,   │
  │    unidad_cantidad, costo_unitario_cop, nivel de confianza,      │
  │    observaciones                                                 │
  │  • Reanudación automática (checkpoint cada 5 PDFs)               │
  └─────────────────────────┬────────────────────────────────────────┘
                            │
                            ▼
  ┌──────────────────────────────────────────────────────────────────┐
  │  [5] recuperar_codigo_indicador.py                               │
  │                                                                  │
  │  • Cruza el Excel de costeo con el log de descargas y los        │
  │    Excel de indicadores (GA1..GA4)                               │
  │  • Añade: codigo_indicador, nombre_indicador, notice_uid, ga     │
  │  • Resultado = trazabilidad completa fila por fila               │
  └─────────────────────────┬────────────────────────────────────────┘
                            │
                            ▼
  ┌──────────────────────────────────────────────────────────────────┐
  │  [6] clasificar_indicadores_2fases.py                            │
  │                                                                  │
  │  • Clasifica cada indicador PDET en la jerarquía UNSPSC de       │
  │    SECOP II (Segmento → Familia → Clase → Producto)              │
  │  • 2 fases para minimizar tokens:                                │
  │       Fase 1: Indicador → Segmento (catálogo de 57 segmentos)    │
  │       Fase 2: Indicador → Producto dentro del segmento           │
  │                (catálogo 28× más pequeño que el total)           │
  └──────────────────────────────────────────────────────────────────┘
                            │
                            ▼
         Excel final con costos unitarios observados
         por indicador × subregión × subprograma
```

Los scripts de R (`R/Codigo_Construccion_categorias_secop.R` y `R/Codigo_Final_Merge_Subprogramas.R`) son insumos previos: construyen la tabla de jerarquía UNSPSC desde el parquet oficial de SECOP y hacen el merge entre subprogramas clasificados y contratos con datos de ruralidad (DIVIPOLA).

---

## 3. Descripción detallada de los scripts

### Scripts de Python

| Script | Líneas | Rol en el pipeline |
|---|---:|---|
| **`secop_fase1_v4_iterativo_5.py`** | 1367 | Inventario de contratos SECOP por subregión × subprograma con 4 capas de búsqueda geográfica. Genera keywords con Claude, consulta la API de Socrata (datos.gov.co), persiste en SQLite y exporta Excel de 8 hojas. Es el **corazón del pipeline**. |
| **`validar_contratos_llm.py`** | 534 | Validación semántica fase 1.5. Para cada contrato asociado a un indicador por keywords, pregunta a Claude si es realmente relevante. Filtra falsos positivos antes de la descarga masiva de PDFs. |
| **`secop_descargador_indicadores_final.py`** | 647 | Descarga masiva de Estudios Previos y Anexos Técnicos desde SECOP II usando Selenium + Chrome. Clasifica documentos por tipo y organiza en carpetas por GA y notice UID. |
| **`extraer_costeo_pdfs_2.py`** | 566 | Extracción de datos de costeo unitario de cada PDF usando Claude Sonnet (API base64 con document input). Extrae 13 campos por contrato con nivel de confianza (alta/media/baja). |
| **`recuperar_codigo_indicador.py`** | 222 | Post-procesamiento que añade trazabilidad: cruza el Excel de costeo con el log de descargas y los Excel de indicadores para vincular cada PDF con su código de indicador PDET, notice UID y grupo de análisis. |
| **`clasificar_indicadores_2fases.py`** | 370 | Clasificación UNSPSC en 2 fases (Segmento → Producto) usando Claude con estrategia de mínimo consumo de tokens (catálogo 28× más pequeño que enviar los 12,732 productos). |

### Scripts de R (en `/R`)

| Script | Rol |
|---|---|
| **`Codigo_Construccion_categorias_secop.R`** | Lee el parquet oficial de SECOP II y construye las tablas jerárquicas UNSPSC (segmento, familia, clase, producto) exportadas como Excel. Alimenta el clasificador de indicadores. |
| **`Codigo_Final_Merge_Subprogramas.R`** | Hace el merge entre los subprogramas clasificados (UNSPSC) y el parquet de contratos SECOP enriquecido con ruralidad DIVIPOLA. Produce la base final que alimenta la Fase 1. |

---

## 4. Instalación

### Dependencias Python

```bash
pip install anthropic pandas openpyxl requests selenium webdriver-manager tqdm
```

### Dependencias R

```r
install.packages(c("arrow", "dplyr", "openxlsx", "readxl", "tidyr"))
```

### Variables de entorno (API keys)

Todos los scripts leen las API keys desde variables de entorno — **nunca deben estar hardcoded en el código**.

**En Linux / macOS:**
```bash
export ANTHROPIC_API_KEY="sk-ant-..."
export SOCRATA_TOKEN="tu_token_de_socrata"
```

**En Windows PowerShell:**
```powershell
$env:ANTHROPIC_API_KEY = "sk-ant-..."
$env:SOCRATA_TOKEN = "tu_token_de_socrata"
```

- **Anthropic API key**: regístrate en https://console.anthropic.com/
- **Socrata App Token** (para la API de datos.gov.co): gratuito en https://data.socrata.com/signup

---

## 5. Archivos de entrada esperados

El pipeline necesita los siguientes Excel de entrada (no se incluyen en el repo por ser datos de trabajo):

| Archivo | Descripción |
|---|---|
| `Cruce_Base_v4_v3.xlsx` (hoja `Base_Indicadores_Final`) | Base priorizada de indicadores con columnas: `Subregion`, `CodSubprograma`, `Cod_indicador`, `nombreindicador`, `prioridadsubprogramasguillermo`, `subprograma` |
| `Indicadores_Final_GA1.xlsx` … `GA4.xlsx` | Indicadores por Grupo de Análisis (GA), con URLs de SECOP II en columna `URL SECOP II` (GA1, GA2) o embebidas en `Nota de Costeo` (GA3, GA4) |
| `data_priorizados_Indicadores.xlsx` (hoja `Indicador_asociado`) | Indicadores con su texto completo para clasificar en UNSPSC |
| `jerarquia_completa.xlsx` | Jerarquía UNSPSC: Segmento → Familia → Clase → Producto (generada por el script de R) |
| `SECOP_contratos_cruce_clasificador_productos.parquet` | Base SECOP original (insumo de los scripts R) |
| `SECOP_Divipola_Ruralidad.parquet` | Base SECOP enriquecida con DIVIPOLA y ruralidad |

---

## 6. Orden de ejecución recomendado

```bash
# --- Preparación (una sola vez) ---
Rscript R/Codigo_Construccion_categorias_secop.R
Rscript R/Codigo_Final_Merge_Subprogramas.R

# --- Pipeline principal ---
# 1. Inventario SECOP (puede reanudarse automáticamente)
python secop_fase1_v4_iterativo_5.py --modo batch

# 2. Validación semántica con Claude
python validar_contratos_llm.py

# 3. Descarga masiva de PDFs (interactivo — requiere resolver CAPTCHA)
python secop_descargador_indicadores_final.py

# 4. Extracción de costeo unitario desde los PDFs
python extraer_costeo_pdfs_2.py

# 5. Añadir trazabilidad PDF → indicador PDET
python recuperar_codigo_indicador.py

# 6. Clasificación UNSPSC de indicadores (puede correr en paralelo)
python clasificar_indicadores_2fases.py
```

Los scripts 1, 2 y 4 tienen **reanudación automática**: si se interrumpen, al volver a ejecutarse retoman donde quedaron.

---

## 7. Notas metodológicas

- **Universo de contratos**: el pipeline no usa la variable `espostconflicto` como filtro único porque cubre solo el 0.7% de los contratos SECOP. En su lugar combina filtros municipales (170 municipios PDET) con filtros semánticos por keywords generadas con Claude a partir del nombre del indicador.
- **4 capas geográficas**: permiten construir un nivel de confianza en el costeo: **Alto** (contratos en la subregión exacta), **Medio** (en el departamento), **Bajo** (solo departamento con pocos casos), **Solo referencia** (otras subregiones PDET o nacional), **Sin datos**.
- **Validación semántica (Fase 1.5)**: las keywords producen muchos falsos positivos (un contrato puede mencionar "vía" sin ser una vía terciaria rural). La validación con Claude en batches de 12 reduce el ruido antes del paso costoso de descarga.
- **Tokens / costos de API**: el clasificador UNSPSC en 2 fases usa ~117,000 tokens totales — aproximadamente 28× menos que enviar los 12,732 productos completos en cada llamada.
- **Estados de contrato**: se incluyen `Cerrado`, `terminado`, `En ejecucion` (sin tilde — la API rechaza caracteres especiales en queries SoQL), `Modificado`.

---

## 8. Estructura del repositorio

```
ART-Costos_Observados/
├── README.md                                 ← este archivo
├── .gitignore                                ← excluye Excel, logs, PDFs
├── secop_fase1_v4_iterativo_5.py             ← pipeline principal (SECOP + SQLite)
├── validar_contratos_llm.py                  ← validación semántica
├── secop_descargador_indicadores_final.py    ← descarga masiva Selenium
├── extraer_costeo_pdfs_2.py                  ← extracción Claude Vision (base64)
├── recuperar_codigo_indicador.py             ← trazabilidad
├── clasificar_indicadores_2fases.py          ← clasificación UNSPSC
└── R/
    ├── Codigo_Construccion_categorias_secop.R
    └── Codigo_Final_Merge_Subprogramas.R
```

---

## 9. Créditos

Consultoría 2026 — **Agencia de Renovación del Territorio (ART)** de Colombia.
Equipo técnico: análisis económico y construcción de la metodología de costeo.
Automatización del pipeline: desarrollo de los scripts aquí publicados.

## 10. Licencia

MIT — libre uso con atribución.
