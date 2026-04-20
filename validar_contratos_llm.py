# -*- coding: utf-8 -*-
"""
VALIDADOR SEMÁNTICO DE CONTRATOS — Fase 1.5
=============================================
Lee la base de datos SQLite generada por secop_fase1_v3.py y valida
semánticamente cada contrato usando Claude (Anthropic API).

Para cada contrato se responde Sí/No: ¿el objeto del contrato es relevante
para el indicador al que fue asociado por keywords?

Los contratos NO relevantes se conservan en la BD con validado_llm = 0.
Los contratos relevantes quedan con validado_llm = 1.
Los contratos aún no procesados quedan con validado_llm = NULL.

PRERREQUISITOS:
    pip install anthropic pandas openpyxl

USO:
    python validar_contratos_llm.py
    python validar_contratos_llm.py --db mi_bd.db --excel resultado_v2.xlsx
    python validar_contratos_llm.py --solo-exportar   (regenera Excel sin llamar API)

SALIDAS:
    - BD SQLite actualizada (columna validado_llm en tabla contratos)
    - Excel con hoja adicional "09_Contratos_Validados_LLM"
"""

import os
import anthropic
import sqlite3
import pandas as pd
import json
import time
import logging
import sys
import argparse
from datetime import datetime
from pathlib import Path


# =============================================================================
# CONFIGURACIÓN — edita estas variables
# =============================================================================

ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")  # set in env: export ANTHROPIC_API_KEY="sk-ant-..."

DB_PATH      = "fase1_inventario_v3.db"      # BD generada por secop_fase1_v3.py
EXCEL_SALIDA = "fase1_resultado_v3_validado.xlsx"
LOG_FILE     = "validacion_llm.log"

BATCH_SIZE        = 12    # contratos por llamada al API (12 es equilibrio costo/velocidad)
PAUSA_ENTRE_LOTES = 0.5   # segundos entre lotes
MAX_REINTENTOS    = 3     # reintentos ante error de API

# Descripción extendida por indicador para darle contexto al LLM.
# Si un indicador no está aquí se usa el nombre de la tabla inventario.
DESCRIPCION_INDICADOR = {
    "P1.41.": (
        "Formalización y titulación de predios rurales de pequeña y mediana propiedad. "
        "Incluye catastro multipropósito, escrituración, trámites ANT, registro de predios."
    ),
    "P6.76.": (
        "Créditos agropecuarios otorgados por FINAGRO o Banco Agrario a productores rurales. "
        "Incluye líneas especiales de crédito, subsidios, garantías agropecuarias."
    ),
    "P6.64.": (
        "Participación de mujeres rurales en operaciones de crédito agropecuario. "
        "Incluye programas de inclusión financiera con enfoque de género para campesinas y productoras rurales."
    ),
    "P6.75.": (
        "Empresas con Registro Nacional de Turismo activo en zonas rurales. "
        "Incluye fomento al turismo comunitario, ecoturismo, agroturismo y turismo rural."
    ),
    "P2.26.": (
        "Intervención de vías terciarias, caminos rurales y caminos ancestrales. "
        "Incluye construcción, mejoramiento y mantenimiento de vías rurales, placa huella, afirmado, pontones."
    ),
    "P8.61.": (
        "Mujeres operadoras de conciliación en derecho y equidad. "
        "Incluye casas de justicia, centros de conciliación, métodos alternativos de resolución de conflictos, "
        "acceso a justicia en zonas rurales."
    ),
}

# =============================================================================


def init_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.FileHandler(LOG_FILE, encoding="utf-8"),
            logging.StreamHandler(sys.stdout),
        ],
    )


def agregar_columna_si_no_existe(conn: sqlite3.Connection):
    """Agrega la columna validado_llm a contratos si no existe todavía."""
    cols = [r[1] for r in conn.execute("PRAGMA table_info(contratos)").fetchall()]
    if "validado_llm" not in cols:
        conn.execute("ALTER TABLE contratos ADD COLUMN validado_llm INTEGER DEFAULT NULL")
        conn.commit()
        logging.info("Columna 'validado_llm' agregada a la tabla contratos.")
    else:
        logging.info("Columna 'validado_llm' ya existe.")


def cargar_pendientes(conn: sqlite3.Connection) -> pd.DataFrame:
    """
    Devuelve los contratos de capas 1 y 2 que aún no han sido validados
    (validado_llm IS NULL). Incluye el nombre del indicador para el prompt.
    """
    query = """
        SELECT
            c.rowid          AS rowid,
            c.id_contrato,
            c.cod_indicador,
            c.subregion,
            c.capa,
            c.objeto_contrato,
            i.nombre_indicador
        FROM contratos c
        LEFT JOIN inventario i
            ON c.cod_indicador = i.cod_indicador
           AND c.subregion     = i.subregion
        WHERE c.capa IN (1, 2)
          AND c.validado_llm IS NULL
          AND c.objeto_contrato IS NOT NULL
          AND trim(c.objeto_contrato) != ''
        ORDER BY c.cod_indicador, c.subregion
    """
    df = pd.read_sql_query(query, conn)
    logging.info(f"Contratos pendientes de validar: {len(df)}")
    return df


def construir_prompt(batch_rows: list[dict], desc_indicador: str) -> str:
    """
    Construye el prompt para un batch de contratos.
    batch_rows: lista de dicts con claves id, objeto_contrato.
    """
    contratos_txt = "\n".join([
        f"{r['idx']}. {r['objeto_contrato'][:300]}"
        for r in batch_rows
    ])

    return f"""Eres un experto en contratación pública colombiana y en los Programas de Desarrollo con Enfoque Territorial (PDET).

INDICADOR A EVALUAR:
{desc_indicador}

Tu tarea es determinar si el objeto de cada contrato tiene relación DIRECTA con el indicador. \
Un contrato es RELEVANTE si financia, ejecuta, apoya operativamente o mide actividades \
vinculadas al indicador. No es relevante si solo menciona palabras similares pero tiene otro propósito.

Responde ÚNICAMENTE con un JSON array válido, sin texto adicional ni backticks:
[{{"id": <número>, "relevante": true}}, {{"id": <número>, "relevante": false}}]

CONTRATOS A EVALUAR:
{contratos_txt}"""


def validar_batch(
    client: anthropic.Anthropic,
    batch_rows: list[dict],
    desc_indicador: str,
) -> dict[int, bool]:
    """
    Llama al API y devuelve un dict {idx -> bool relevante}.
    Reintenta hasta MAX_REINTENTOS veces ante errores.
    """
    prompt = construir_prompt(batch_rows, desc_indicador)
    intentos = 0

    while intentos < MAX_REINTENTOS:
        try:
            msg = client.messages.create(
                model="claude-sonnet-4-20250514",
                max_tokens=800,
                messages=[{"role": "user", "content": prompt}],
            )
            raw = msg.content[0].text.strip()
            raw = raw.replace("```json", "").replace("```", "").strip()
            resultados = json.loads(raw)
            return {int(r["id"]): bool(r["relevante"]) for r in resultados}

        except json.JSONDecodeError as e:
            intentos += 1
            logging.warning(f"JSON inválido (intento {intentos}): {e}")
            time.sleep(2 * intentos)

        except anthropic.RateLimitError:
            intentos += 1
            logging.warning(f"Rate limit (intento {intentos}), esperando 30s...")
            time.sleep(30)

        except Exception as e:
            intentos += 1
            logging.warning(f"Error inesperado (intento {intentos}): {e}")
            time.sleep(3 * intentos)

    logging.error(f"Batch fallido tras {MAX_REINTENTOS} intentos. Se omite.")
    return {}


def guardar_validaciones(conn: sqlite3.Connection, validaciones: dict[int, bool]):
    """Actualiza la columna validado_llm por rowid."""
    cur = conn.cursor()
    for rowid, relevante in validaciones.items():
        cur.execute(
            "UPDATE contratos SET validado_llm = ? WHERE rowid = ?",
            (1 if relevante else 0, rowid),
        )
    conn.commit()


def run_validacion(db_path: str):
    """Proceso principal de validación semántica."""
    if not Path(db_path).exists():
        print(f"\nERROR: No se encontró la BD: {db_path}")
        sys.exit(1)

    conn = sqlite3.connect(db_path)
    agregar_columna_si_no_existe(conn)

    df = cargar_pendientes(conn)
    if df.empty:
        logging.info("No hay contratos pendientes. La BD ya está completamente validada.")
        conn.close()
        return

    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)

    # Agrupar por indicador para dar contexto consistente al LLM
    grupos = df.groupby("cod_indicador")
    total_lotes = 0
    total_relevantes = 0
    total_no_relevantes = 0

    for cod_ind, grupo in grupos:
        nombre_ind = grupo["nombre_indicador"].iloc[0] or cod_ind
        desc = DESCRIPCION_INDICADOR.get(cod_ind, nombre_ind)

        filas = grupo.reset_index(drop=True)
        batches = [
            filas.iloc[i:i + BATCH_SIZE]
            for i in range(0, len(filas), BATCH_SIZE)
        ]

        logging.info(
            f"\n{'─'*55}\n"
            f"  Indicador : {cod_ind} — {nombre_ind[:55]}\n"
            f"  Contratos : {len(filas)} en {len(batches)} lotes\n"
            f"{'─'*55}"
        )

        for b_idx, batch_df in enumerate(batches):
            batch_rows = [
                {
                    "idx": i + 1,
                    "objeto_contrato": row["objeto_contrato"],
                    "rowid": row["rowid"],
                }
                for i, (_, row) in enumerate(batch_df.iterrows())
            ]

            print(
                f"  Lote {b_idx+1}/{len(batches)} "
                f"({len(batch_rows)} contratos)...",
                end=" ", flush=True,
            )

            resultados_idx = validar_batch(client, batch_rows, desc)

            # Mapear índice local → rowid real
            validaciones_rowid = {}
            for local_idx, relevante in resultados_idx.items():
                if 1 <= local_idx <= len(batch_rows):
                    rowid = batch_rows[local_idx - 1]["rowid"]
                    validaciones_rowid[rowid] = relevante

            guardar_validaciones(conn, validaciones_rowid)

            n_rel = sum(1 for v in validaciones_rowid.values() if v)
            n_no  = sum(1 for v in validaciones_rowid.values() if not v)
            total_relevantes    += n_rel
            total_no_relevantes += n_no
            total_lotes         += 1

            print(f"✅ {n_rel} relevantes  ❌ {n_no} no relevantes")
            time.sleep(PAUSA_ENTRE_LOTES)

    conn.close()

    print(f"\n{'='*55}")
    print(f"  VALIDACIÓN COMPLETADA")
    print(f"  Lotes procesados  : {total_lotes}")
    print(f"  Relevantes        : {total_relevantes}")
    print(f"  No relevantes     : {total_no_relevantes}")
    tasa = (
        round(total_relevantes / (total_relevantes + total_no_relevantes) * 100, 1)
        if (total_relevantes + total_no_relevantes) > 0 else 0
    )
    print(f"  Tasa de precisión : {tasa}%")
    print(f"{'='*55}")


# =============================================================================
# EXPORTACIÓN EXCEL
# =============================================================================

def exportar_excel_validado(db_path: str, excel_out: str):
    """
    Genera el Excel de resultados incluyendo la hoja 09 con contratos validados.
    Preserva las 8 hojas originales y agrega la nueva.
    """
    if not Path(db_path).exists():
        print(f"ERROR: BD no encontrada: {db_path}")
        return

    conn = sqlite3.connect(db_path)
    print(f"\nExportando a: {excel_out}")

    with pd.ExcelWriter(excel_out, engine="openpyxl") as writer:

        # ── Hojas originales (igual que en secop_fase1_v3.py) ──────────────

        pd.read_sql_query("""
            SELECT subregion, subprograma, cod_indicador, nombre_indicador,
                   departamento_sr, nivel_confianza, hay_datos_propios,
                   requiere_ajuste, total_contratos,
                   total_c1 AS contratos_subregion_exacta,
                   total_c2 AS contratos_departamento,
                   total_c3_otras_sr AS contratos_otras_subregiones,
                   anno_min, anno_max, annos_lista,
                   ROUND(valor_total,0) AS valor_total_cop,
                   ROUND(valor_promedio,0) AS valor_promedio_cop,
                   ROUND(valor_min,0) AS valor_min_cop,
                   ROUND(valor_max,0) AS valor_max_cop,
                   fuente_ajuste, nota_ajuste, entidades_lista, keywords_usadas
            FROM inventario
            ORDER BY subregion, subprograma, cod_indicador
        """, conn).to_excel(writer, sheet_name="01_Inventario_Completo", index=False)

        df_inv = pd.read_sql_query(
            "SELECT * FROM inventario", conn
        )
        df_inv[df_inv["hay_datos_propios"] == 1].to_excel(
            writer, sheet_name="02_Con_Datos_Propios", index=False
        )
        df_inv[df_inv["requiere_ajuste"] == 1].to_excel(
            writer, sheet_name="03_Requieren_Ajuste", index=False
        )

        pd.read_sql_query("""
            SELECT c.subregion, c.cod_indicador, i.nombre_indicador, i.subprograma,
                   c.capa,
                   CASE c.capa WHEN 1 THEN 'Subregion exacta'
                               WHEN 2 THEN 'Departamento' END AS nivel_geografico,
                   c.validado_llm,
                   c.id_contrato, c.referencia, c.proceso_compra,
                   c.nombre_entidad, c.departamento, c.ciudad,
                   c.tipo_contrato, c.objeto_contrato,
                   ROUND(c.valor_contrato,0) AS valor_contrato_cop,
                   c.anno_firma, c.estado_contrato, c.url_proceso
            FROM contratos c
            LEFT JOIN inventario i ON c.cod_indicador=i.cod_indicador
                                  AND c.subregion=i.subregion
            WHERE c.capa IN (1,2)
            ORDER BY c.subregion, c.cod_indicador, c.capa, c.valor_contrato DESC
        """, conn).to_excel(writer, sheet_name="04_Contratos_IDs_URLs", index=False)

        pd.read_sql_query("""
            SELECT c.subregion AS subregion_sin_datos, c.cod_indicador,
                   i.nombre_indicador, i.subprograma,
                   c.subregion_origen AS subregion_referencia,
                   c.id_contrato, c.referencia, c.nombre_entidad,
                   c.departamento, c.ciudad, c.tipo_contrato, c.objeto_contrato,
                   ROUND(c.valor_contrato,0) AS valor_contrato_cop,
                   c.anno_firma, c.estado_contrato, c.url_proceso
            FROM contratos c
            LEFT JOIN inventario i ON c.cod_indicador=i.cod_indicador
                                  AND c.subregion=i.subregion
            WHERE c.capa=3
            ORDER BY c.subregion, c.cod_indicador, c.valor_contrato DESC
        """, conn).to_excel(writer, sheet_name="05_Contratos_Otras_SR_Ajuste", index=False)

        pd.read_sql_query("""
            SELECT subregion,
                   COUNT(*) AS total_indicadores,
                   SUM(CASE WHEN nivel_confianza='Alto'  THEN 1 ELSE 0 END) AS alta,
                   SUM(CASE WHEN nivel_confianza='Medio' THEN 1 ELSE 0 END) AS media,
                   SUM(CASE WHEN nivel_confianza='Bajo'  THEN 1 ELSE 0 END) AS baja,
                   SUM(CASE WHEN nivel_confianza='Solo otras subregiones'
                            THEN 1 ELSE 0 END) AS solo_referencia,
                   SUM(CASE WHEN nivel_confianza='Sin datos'
                            THEN 1 ELSE 0 END) AS sin_datos,
                   SUM(total_contratos) AS total_contratos_locales,
                   ROUND(SUM(valor_total),0) AS inversion_total_cop,
                   SUM(requiere_ajuste) AS indicadores_con_ajuste
            FROM inventario
            GROUP BY subregion ORDER BY total_contratos_locales DESC
        """, conn).to_excel(writer, sheet_name="06_Resumen_Subregiones", index=False)

        pd.read_sql_query("""
            SELECT cod_indicador, nombre_indicador, subprograma,
                   COUNT(DISTINCT subregion) AS subregiones_evaluadas,
                   SUM(CASE WHEN nivel_confianza IN ('Alto','Medio')
                            THEN 1 ELSE 0 END) AS con_datos_buenos,
                   SUM(requiere_ajuste) AS requieren_ajuste,
                   SUM(total_contratos) AS total_contratos_todos,
                   ROUND(AVG(valor_promedio),0) AS valor_promedio_general
            FROM inventario
            GROUP BY cod_indicador, nombre_indicador, subprograma
            ORDER BY cod_indicador
        """, conn).to_excel(writer, sheet_name="07_Resumen_Indicadores", index=False)

        pd.read_sql_query(
            "SELECT * FROM log_queries ORDER BY timestamp", conn
        ).to_excel(writer, sheet_name="08_Log", index=False)

        # ── Hoja 09: Contratos validados por LLM ───────────────────────────
        df_validados = pd.read_sql_query("""
            SELECT
                c.subregion,
                c.cod_indicador,
                i.nombre_indicador,
                i.subprograma,
                c.capa,
                CASE c.capa
                    WHEN 1 THEN 'Subregion exacta'
                    WHEN 2 THEN 'Departamento'
                END AS nivel_geografico,
                CASE c.validado_llm
                    WHEN 1 THEN 'Relevante'
                    WHEN 0 THEN 'No relevante'
                    ELSE   'Pendiente'
                END AS validacion_llm,
                c.id_contrato,
                c.nombre_entidad,
                c.departamento,
                c.ciudad,
                c.tipo_contrato,
                c.objeto_contrato,
                ROUND(c.valor_contrato, 0) AS valor_contrato_cop,
                c.anno_firma,
                c.estado_contrato,
                c.url_proceso
            FROM contratos c
            LEFT JOIN inventario i
                ON c.cod_indicador = i.cod_indicador
               AND c.subregion     = i.subregion
            WHERE c.capa IN (1, 2)
            ORDER BY
                c.subregion,
                c.cod_indicador,
                c.validado_llm DESC,   -- relevantes primero
                c.valor_contrato DESC
        """, conn)

        df_validados.to_excel(writer, sheet_name="09_Contratos_Validados_LLM", index=False)

        # ── Hoja 10: Resumen de validación por indicador ───────────────────
        df_resumen_val = pd.read_sql_query("""
            SELECT
                c.cod_indicador,
                i.nombre_indicador,
                COUNT(*)                                         AS total_evaluados,
                SUM(CASE WHEN c.validado_llm = 1 THEN 1 ELSE 0 END) AS relevantes,
                SUM(CASE WHEN c.validado_llm = 0 THEN 1 ELSE 0 END) AS no_relevantes,
                SUM(CASE WHEN c.validado_llm IS NULL THEN 1 ELSE 0 END) AS pendientes,
                ROUND(
                    100.0 * SUM(CASE WHEN c.validado_llm=1 THEN 1 ELSE 0 END)
                    / NULLIF(SUM(CASE WHEN c.validado_llm IS NOT NULL THEN 1 ELSE 0 END), 0),
                    1
                ) AS pct_precision
            FROM contratos c
            LEFT JOIN inventario i
                ON c.cod_indicador = i.cod_indicador
               AND c.subregion     = i.subregion
            WHERE c.capa IN (1, 2)
            GROUP BY c.cod_indicador, i.nombre_indicador
            ORDER BY pct_precision ASC
        """, conn)

        df_resumen_val.to_excel(
            writer, sheet_name="10_Resumen_Validacion_LLM", index=False
        )

    conn.close()
    print(f"  Guardado: {excel_out}")
    print(f"  Hojas exportadas: 01 a 10 (incluyendo validación LLM)")


# =============================================================================
# PUNTO DE ENTRADA
# =============================================================================

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Validador semántico LLM para contratos SECOP — Fase 1.5"
    )
    parser.add_argument("--db",
        default=DB_PATH,
        help="Ruta a la BD SQLite de fase 1")
    parser.add_argument("--excel",
        default=EXCEL_SALIDA,
        help="Ruta del Excel de salida")
    parser.add_argument("--solo-exportar",
        action="store_true",
        help="Regenera el Excel sin llamar al API (usa validaciones ya guardadas)")
    parser.add_argument("--api-key",
        default=None,
        help="API key de Anthropic (alternativa a editar el script)")

    args = parser.parse_args()

    init_logging()

    api_key = args.api_key or ANTHROPIC_API_KEY
    if not args.solo_exportar and "TU_API_KEY" in api_key:
        print("\nERROR: Configura ANTHROPIC_API_KEY en el script o usa --api-key.")
        sys.exit(1)

    if not args.solo_exportar:
        # Inyectar api_key si viene por argumento
        if args.api_key:
            import anthropic as _ant
            _original = _ant.Anthropic
        run_validacion(args.db)

    exportar_excel_validado(args.db, args.excel)
