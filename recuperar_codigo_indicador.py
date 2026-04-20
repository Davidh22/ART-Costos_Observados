# -*- coding: utf-8 -*-
"""
RECUPERADOR DE CÓDIGO DE INDICADOR
====================================
Cruza el Excel de costeo unitario ya generado (costeo_unitario_contratos.xlsx)
con el historial de descargas (secop_indicadores.log) y los archivos de
indicadores (Indicadores_Final_GAX.xlsx) para añadir las columnas:
  - codigo_indicador  (ej: P1.33., P4.28., P2.26.)
  - nombre_indicador  (nombre completo del indicador PDET)
  - notice_uid        (CO1.NTC.XXXXXXX del contrato)
  - ga                (GA1, GA2, GA3 o GA4)

USO:
    python recuperar_codigo_indicador.py

SALIDA:
    costeo_unitario_CON_INDICADOR.xlsx  — Excel final con trazabilidad completa
"""

import re
import pandas as pd
from pathlib import Path

# =============================================================================
# CONFIGURACIÓN — ajusta estas rutas si es necesario
# =============================================================================

# Excel generado por el extractor de costeo
EXCEL_COSTEO = r"C:\Users\velez\Documents\Consultorias_2026\ART_Costeo\Datos\Nueva_Estrategia\Fase_II\Pdf_mcp_final\costeo_unitario_contratos.xlsx"

# Log de descargas generado por secop_descargador_indicadores_final.py
LOG_DESCARGAS = r"C:\Users\velez\Documents\Consultorias_2026\ART_Costeo\Datos\Nueva_Estrategia\Fase_II\secop_indicadores.log"

# Carpeta donde están los 4 Excel de indicadores
CARPETA_INDICADORES = r"C:\Users\velez\Documents\Consultorias_2026\ART_Costeo\Datos\Nueva_Estrategia\Fase_II"

EXCEL_SALIDA = r"C:\Users\velez\Documents\Consultorias_2026\ART_Costeo\Datos\Nueva_Estrategia\Fase_II\Pdf_mcp_final\costeo_unitario_CON_INDICADOR.xlsx"


# =============================================================================
# PASO 1: Parsear el log → mapeo nombre_pdf → notice_uid → GA
# =============================================================================

def parsear_log(ruta_log: str) -> pd.DataFrame:
    with open(ruta_log, 'r', encoding='utf-8', errors='replace') as f:
        lines = f.readlines()

    mapeo = []
    ga_actual = notice_actual = None

    for line in lines:
        # Línea de contrato: [N/90] GA1 — CO1.NTC.XXXXXXX
        m = re.search(r'\[(\d+)/\d+\]\s+(GA\d+)\s+.+?(CO1\.NTC\.\d+)', line)
        if m:
            ga_actual = m.group(2)
            notice_actual = m.group(3)
            continue

        # Línea de PDF (Timeout esperando o Descargado)
        m2 = re.search(r'(?:Timeout esperando|Descargado):\s*(.+\.pdf)', line, re.IGNORECASE)
        if m2 and notice_actual:
            nombre = m2.group(1).strip()
            mapeo.append({
                'ga':         ga_actual,
                'notice_uid': notice_actual,
                'nombre_pdf': nombre,
            })

    df = pd.DataFrame(mapeo)
    print(f"  Log parseado: {len(df)} relaciones nombre_pdf → notice_uid")
    return df


# =============================================================================
# PASO 2: Cargar Excel de indicadores → mapeo notice_uid → código + nombre
# =============================================================================

def extraer_notice_uid(texto) -> str | None:
    if not isinstance(texto, str):
        return None
    m = re.search(r'noticeUID=([A-Z0-9\.]+)', texto)
    return m.group(1) if m else None


def cargar_indicadores(carpeta: str) -> pd.DataFrame:
    archivos = {
        'GA1': 'Indicadores_Final_GA1.xlsx',
        'GA2': 'Indicadores_Final_GA2.xlsx',
        'GA3': 'Indicadores_Final_GA3.xlsx',
        'GA4': 'Indicadores_Final_GA4.xlsx',
    }

    dfs = []
    for ga, nombre_archivo in archivos.items():
        ruta = Path(carpeta) / nombre_archivo
        if not ruta.exists():
            print(f"  ADVERTENCIA: No se encontró {ruta}")
            continue

        df = pd.read_excel(ruta)
        df.columns = [str(c).replace('\n', ' ').strip() for c in df.columns]
        df['ga'] = ga

        # Extraer notice_uid de la URL SECOP
        if 'URL SECOP II' in df.columns:
            df['notice_uid'] = df['URL SECOP II'].apply(extraer_notice_uid)
        elif 'Nota de Costeo' in df.columns:
            df['notice_uid'] = df['Nota de Costeo'].astype(str).apply(extraer_notice_uid)
        else:
            df['notice_uid'] = None

        # Identificar columnas de código e indicador
        cod_col = next((c for c in df.columns if 'digo' in c or c == 'Código'), df.columns[1])
        ind_col = next((c for c in df.columns if 'ndicador' in c), df.columns[2])

        df = df[['ga', 'notice_uid', cod_col, ind_col]].rename(columns={
            cod_col: 'codigo_indicador',
            ind_col: 'nombre_indicador',
        })
        dfs.append(df)

    df_ind = pd.concat(dfs, ignore_index=True).dropna(subset=['notice_uid'])
    # Si un mismo notice_uid aparece varias veces (varios indicadores), consolidar
    df_ind = df_ind.drop_duplicates(subset=['notice_uid'])
    print(f"  Indicadores cargados: {len(df_ind)} con notice_uid")
    return df_ind


# =============================================================================
# PASO 3: Cruzar todo y añadir columnas al Excel de costeo
# =============================================================================

def main():
    print("=" * 60)
    print("RECUPERADOR DE CÓDIGO DE INDICADOR")
    print("=" * 60)

    # Cargar Excel de costeo
    print(f"\nCargando Excel de costeo...")
    df_costeo = pd.read_excel(EXCEL_COSTEO, sheet_name='Costeo_Unitario')
    print(f"  Filas en el Excel de costeo: {len(df_costeo)}")

    # Parsear log
    print(f"\nParsando log de descargas...")
    df_log = parsear_log(LOG_DESCARGAS)

    # Cargar indicadores
    print(f"\nCargando Excel de indicadores...")
    df_ind = cargar_indicadores(CARPETA_INDICADORES)

    # Cruzar log con indicadores: nombre_pdf → notice_uid → codigo_indicador
    print(f"\nCruzando datos...")
    # df_log ya tiene columna 'ga'; df_ind también — renombrar la de df_ind para evitar conflicto
    df_ref = df_log.merge(
        df_ind[['notice_uid', 'ga', 'codigo_indicador', 'nombre_indicador']].rename(
            columns={'ga': 'ga_ind'}
        ),
        on='notice_uid',
        how='left'
    )
    # Usar ga del log (más confiable); si falta, usar la del Excel de indicadores
    df_ref['ga'] = df_ref['ga'].fillna(df_ref['ga_ind'])
    df_ref = df_ref.drop(columns=['ga_ind'])

    # Normalizar nombre_pdf para el cruce con el Excel de costeo
    df_ref['nombre_pdf_norm'] = df_ref['nombre_pdf'].str.strip().str.lower()
    df_costeo['archivo_pdf_norm'] = df_costeo['archivo_pdf'].astype(str).str.strip().str.lower()

    # Cruce principal: por nombre de archivo (normalizado).
    # Las columnas se llaman distinto en cada DataFrame, así que uso left_on/right_on.
    df_ref_dedup = df_ref.drop_duplicates(subset=['nombre_pdf_norm'])
    df_final = df_costeo.merge(
        df_ref_dedup[['nombre_pdf_norm', 'notice_uid', 'ga', 'codigo_indicador', 'nombre_indicador']],
        left_on='archivo_pdf_norm',
        right_on='nombre_pdf_norm',
        how='left'
    )

    # Limpiar columnas auxiliares
    df_final = df_final.drop(columns=['archivo_pdf_norm', 'nombre_pdf_norm'])

    # Reordenar columnas: poner codigo_indicador y notice_uid al frente
    cols_frente = ['archivo_pdf', 'ga', 'notice_uid', 'codigo_indicador', 'nombre_indicador']
    cols_resto  = [c for c in df_final.columns if c not in cols_frente]
    df_final    = df_final[cols_frente + cols_resto]

    # Estadísticas del cruce
    con_codigo  = df_final['codigo_indicador'].notna().sum()
    sin_codigo  = df_final['codigo_indicador'].isna().sum()
    print(f"  Con codigo_indicador recuperado: {con_codigo}")
    print(f"  Sin codigo_indicador (no estaban en el log): {sin_codigo}")

    if sin_codigo > 0:
        print(f"\n  Archivos SIN código (revisar manualmente):")
        for nombre in df_final[df_final['codigo_indicador'].isna()]['archivo_pdf'].tolist():
            print(f"    - {nombre}")

    # Guardar Excel final
    print(f"\nGuardando Excel final...")
    df_final.to_excel(EXCEL_SALIDA, index=False, sheet_name='Costeo_Unitario')
    print(f"  Guardado en: {EXCEL_SALIDA}")

    # Resumen por código de indicador
    print(f"\nResumen por código de indicador:")
    resumen = (
        df_final.groupby('codigo_indicador', dropna=False)
        .agg(
            archivos=('archivo_pdf', 'count'),
            con_costo_unitario=('costo_unitario_cop', lambda x: x.notna().sum()),
        )
        .reset_index()
        .sort_values('codigo_indicador')
    )
    print(resumen.to_string(index=False))

    print("\n" + "=" * 60)
    print("LISTO. Abre costeo_unitario_CON_INDICADOR.xlsx")
    print("=" * 60)


if __name__ == '__main__':
    main()
