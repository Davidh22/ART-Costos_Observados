# -*- coding: utf-8 -*-
"""
FASE 1 - INVENTARIO DE CONTRATOS POR SUBREGION x SUBPROGRAMA (con seleccion de indicador)
Sistema de Costeo Unitario de Subprogramas PDET
Version: 4.4 - Sin upper() en queries para evitar timeouts en SECOP

LOGICA DE ITERACION:
  1. Lee la hoja 'Base_Indicadores_Final' del Excel de entrada
  2. Para cada Subregion (una a la vez, en orden):
       a. Filtra todos los registros de esa subregion
       b. Para cada CodSubprograma dentro de la subregion:
            - Si hay varios indicadores => selecciona el de mayor 'prioridadsubprogramasguillermo'
            - Si hay empate => usa el primero segun orden del Excel
            - Si la subregion no tiene datos de contratos => se registra explicitamente
       c. Ejecuta las 3 capas de busqueda SECOP para el indicador seleccionado
  3. Guarda resultados en SQLite y exporta Excel de 8 hojas

EXCEL DE ENTRADA (hoja: Base_Indicadores_Final):
  Subregion                      -> nombre de la subregion PDET
  CodSubprograma                 -> codigo del subprograma
  Cod_indicador                  -> codigo del indicador
  nombreindicador                -> descripcion del indicador
  prioridadsubprogramasguillermo -> prioridad (mayor = mas relevante)
  subprograma                    -> descripcion del subprograma

ESTADOS DE CONTRATO INCLUIDOS:
  Cerrado, terminado, En ejecucion, Modificado
  NOTA: sin tilde en 'ejecucion' - la API SECOP rechaza caracteres especiales

LOGICA DE BUSQUEDA - 4 CAPAS:
  Capa 1: keywords AND municipios de la subregion propia
  Capa 2: keywords AND departamento de la subregion
  Capa 3: keywords en otras subregiones PDET (para factor de ajuste territorial)
  Capa 4: keywords en todo el pais (solo si capas 1-3 retornan 0 contratos)

COMANDOS:
  pip install requests pandas openpyxl tqdm anthropic
  python secop_fase1_v4_iterativo.py --modo test --subregion "Alto Patia y Norte del Cauca" --token "TU_TOKEN"
  python secop_fase1_v4_iterativo.py --modo batch --token "TU_TOKEN"
  python secop_fase1_v4_iterativo.py --modo exportar
"""

import os
import requests
import pandas as pd
import sqlite3
import time
import logging
import json
import sys
from datetime import datetime
from typing import List, Dict, Optional, Tuple

# =============================================================================
# CONFIGURACION - EDITA ESTAS VARIABLES
# =============================================================================

EXCEL_ENTRADA  = r"Cruce_Base_v4_v3.xlsx"
HOJA_ENTRADA   = "Base_Indicadores_Final"

DB_PATH        = "fase1_inventario_v4.db"
EXCEL_SALIDA   = "fase1_resultado_v4.xlsx"
LOG_FILE       = "fase1_log_v4.log"

SOCRATA_TOKEN  = "TU_APP_TOKEN_AQUI"
SECOP_URL      = "https://www.datos.gov.co/resource/jbjy-vk9h.json"
ANTHROPIC_KEY = os.environ.get("ANTHROPIC_API_KEY", "")  # set in env: export ANTHROPIC_API_KEY="sk-ant-..."

ANNO_MINIMO    = 2018
MAX_C1_C2      = 100      # Reducido para evitar timeouts por queries largas
MAX_C3         = 50       # Reducido para evitar timeouts por queries largas
MAX_C4         = 50       # Maximo contratos capa 4 (busqueda nacional)
PAUSA          = 2.0      # Pausa entre llamadas
TIMEOUT        = 120      # Segundos de espera por llamada a SECOP
MAX_REINTENTOS = 3        # Reintentos automaticos ante timeout

# =============================================================================
# ESTADOS DE CONTRATO VALIDOS (sin tildes)
# =============================================================================
ESTADOS_VALIDOS = ["Cerrado", "terminado", "En ejecucion", "Modificado"]

# =============================================================================
# CAMPOS A RECUPERAR DE SECOP
# =============================================================================
CAMPOS_SELECT = (
    "id_contrato,proceso_de_compra,referencia_del_contrato,"
    "nombre_entidad,nit_entidad,departamento,ciudad,sector,"
    "tipo_de_contrato,modalidad_de_contratacion,objeto_del_contrato,"
    "valor_del_contrato,fecha_de_firma,fecha_de_fin_del_contrato,"
    "estado_contrato,proveedor_adjudicado,es_pyme,espostconflicto,"
    "urlproceso,presupuesto_general_de_la_nacion_pgn,"
    "sistema_general_de_regal_as,sistema_general_de_participaciones,"
    "duraci_n_del_contrato"
)

# =============================================================================
# SUBREGIONES PDET - municipios y departamentos (sin tildes en nombres)
# =============================================================================
SUBREGIONES = {
    "Alto Patia y Norte del Cauca": {
        "departamento_principal": "Cauca",
        "departamentos": ["Cauca"],
        "municipios": [
            "Patia", "El Bordo", "Mercaderes", "Bolivar", "La Sierra", "La Vega",
            "Florencia", "Almaguer", "San Sebastian", "Santa Rosa", "Rosas",
            "Corinto", "El Tambo", "Miranda", "Padilla", "Puerto Tejada",
            "Santander de Quilichao", "Suarez", "Toribio", "Villa Rica",
        ],
        "alias_excel": ["Alto Patía y Norte del Cauca", "Alto Patia y Norte del Cauca"],
    },
    "Arauca": {
        "departamento_principal": "Arauca",
        "departamentos": ["Arauca"],
        "municipios": ["Arauca", "Arauquita", "Fortul", "Puerto Rondon", "Saravena", "Tame"],
        "alias_excel": ["Arauca"],
    },
    "Bajo Cauca": {
        "departamento_principal": "Antioquia",
        "departamentos": ["Antioquia"],
        "municipios": [
            "Caceres", "Caucasia", "El Bagre", "Nechi", "Taraza", "Zaragoza",
            "Anori", "Briceno", "Ituango", "Valdivia", "Yarumal",
        ],
        "alias_excel": ["Bajo Cauca"],
    },
    "Catatumbo": {
        "departamento_principal": "Norte de Santander",
        "departamentos": ["Norte de Santander"],
        "municipios": [
            "Convencion", "El Carmen", "El Tarra", "Hacari", "La Playa",
            "San Calixto", "Sardinata", "Teorama", "Tibu",
        ],
        "alias_excel": ["Catatumbo"],
    },
    "Choco": {
        "departamento_principal": "Choco",
        "departamentos": ["Choco"],
        "municipios": [
            "Alto Baudo", "Bahia Solano", "Bajo Baudo", "Bojaya", "Carmen del Darien",
            "Condoto", "El Canton del San Pablo", "El Litoral del San Juan", "Istmina",
            "Jurado", "Lloro", "Medio Atrato", "Medio Baudo", "Medio San Juan",
            "Novita", "Nuqui", "Quibdo", "Rio Iro", "Rio Quito", "Riosucio",
            "San Jose del Palmar", "Sipi", "Tado", "Union Panamericana",
        ],
        "alias_excel": ["Chocó", "Choco"],
    },
    "Cuenca del Caguan": {
        "departamento_principal": "Caqueta",
        "departamentos": ["Caqueta"],
        "municipios": [
            "Albania", "Cartagena del Chaira", "Curillo", "El Doncello", "El Paujil",
            "La Montanita", "Milan", "Morelia", "Puerto Rico", "San Jose del Fragua",
            "San Vicente del Caguan", "Solano", "Solita", "Valparaiso",
        ],
        "alias_excel": ["Cuenca del Caguán", "Cuenca del Caguan", "Cuenca Caguan"],
    },
    "Macarena Guaviare": {
        "departamento_principal": "Meta",
        "departamentos": ["Meta", "Guaviare"],
        "municipios": [
            "La Macarena", "La Uribe", "Maripiran", "Mesetas", "Puerto Concordia",
            "Puerto Gaitan", "Puerto Lleras", "Puerto Rico", "San Juan de Arama",
            "Vista Hermosa", "San Jose del Guaviare", "El Retorno", "Calamar",
        ],
        "alias_excel": ["Macarena Guaviare"],
    },
    "Montes de Maria": {
        "departamento_principal": "Bolivar",
        "departamentos": ["Bolivar", "Sucre"],
        "municipios": [
            "Carmen de Bolivar", "Cordoba", "El Guamo", "Marialabaja",
            "San Jacinto", "San Juan Nepomuceno", "Zambrano", "Chalan",
            "Coloso", "Los Palmitos", "Morroa", "Ovejas", "Palmito",
            "San Antonio de Palmito", "San Onofre", "Since", "Toluviejo",
        ],
        "alias_excel": ["Montes de María", "Montes de Maria"],
    },
    "Pacifico Medio": {
        "departamento_principal": "Choco",
        "departamentos": ["Choco"],
        "municipios": [
            "Bahia Solano", "El Litoral del San Juan", "Jurado", "Nuqui",
            "Bajo Baudo", "Sipi",
        ],
        "alias_excel": ["Pacífico Medio", "Pacifico Medio"],
    },
    "Pacifico y Frontera Narinense": {
        "departamento_principal": "Narino",
        "departamentos": ["Narino"],
        "municipios": [
            "Barbacoas", "El Charco", "Francisco Pizarro", "La Tola", "Magui",
            "Mosquera", "Olaya Herrera", "Roberto Payan", "Santa Barbara", "Tumaco",
        ],
        "alias_excel": [
            "Pacífico y frontera Nariñense",
            "Pacifico y frontera Narinense",
            "Pacifico y Frontera Narinense",
        ],
    },
    "Putumayo": {
        "departamento_principal": "Putumayo",
        "departamentos": ["Putumayo"],
        "municipios": [
            "Colon", "Leguizamo", "Mocoa", "Orito", "Puerto Asis",
            "Puerto Caicedo", "Puerto Guzman", "Puerto Leguizamo", "San Francisco",
            "San Miguel", "Santiago", "Sibundoy", "Valle del Guamuez", "Villagarzon",
        ],
        "alias_excel": ["Putumayo"],
    },
    "Sierra Nevada": {
        "departamento_principal": "Magdalena",
        "departamentos": ["Magdalena", "Cesar", "La Guajira"],
        "municipios": [
            "Aracataca", "Cienaga", "El Reten", "Fundacion", "Puebloviejo",
            "Santa Marta", "Zona Bananera", "Becerril", "La Jagua de Ibirico",
            "Manaure", "San Diego", "Valledupar", "Dibulla",
        ],
        "alias_excel": ["Sierra Nevada"],
    },
    "Sur de Bolivar": {
        "departamento_principal": "Bolivar",
        "departamentos": ["Bolivar"],
        "municipios": [
            "Cantagallo", "Morales", "San Pablo", "Santa Rosa del Sur",
            "Simiti", "Tiquisio", "Montecristo", "Norosi", "Regidor", "Rio Viejo",
        ],
        "alias_excel": ["Sur de Bolívar", "Sur de Bolivar"],
    },
    "Sur de Cordoba": {
        "departamento_principal": "Cordoba",
        "departamentos": ["Cordoba"],
        "municipios": [
            "Montelibano", "Puerto Libertador", "San Jose de Ure",
            "Tierralta", "Valencia",
        ],
        "alias_excel": ["Sur de Córdoba", "Sur de Cordoba"],
    },
    "Sur de Tolima": {
        "departamento_principal": "Tolima",
        "departamentos": ["Tolima"],
        "municipios": ["Ataco", "Chaparral", "Planadas", "Rioblanco", "San Antonio"],
        "alias_excel": ["Sur de Tolima"],
    },
    "Uraba Antioqueno": {
        "departamento_principal": "Antioquia",
        "departamentos": ["Antioquia"],
        "municipios": [
            "Apartado", "Arboletes", "Carepa", "Chigorodo", "Murindo", "Mutata",
            "Necocli", "San Juan de Uraba", "San Pedro de Uraba", "Turbo",
            "Vigia del Fuerte",
        ],
        "alias_excel": ["Urabá Antioqueño", "Uraba Antioqueno"],
    },
}

# Mapa alias -> nombre interno
ALIAS_MAP = {
    alias: nombre
    for nombre, datos in SUBREGIONES.items()
    for alias in datos["alias_excel"]
}


# =============================================================================
# GENERACION DE KEYWORDS VIA CLAUDE API
# =============================================================================

def generar_keywords_claude(nombre_indicador: str, cod_indicador: str,
                             anthropic_key: str) -> Dict:
    """Llama a Claude para generar keywords de busqueda para un indicador."""
    import anthropic

    client = anthropic.Anthropic(api_key=anthropic_key)
    prompt = f"""Eres un experto en contratacion publica colombiana y politica social PDET.
Para el indicador: "{nombre_indicador}" (codigo: {cod_indicador})

Genera keywords de busqueda para encontrar contratos relacionados en SECOP II.
Responde SOLO con JSON, sin texto adicional, sin bloques de codigo:
{{
  "kw_primarias": ["keyword1", "keyword2", "keyword3", "keyword4", "keyword5"],
  "kw_secundarias": ["kw6", "kw7", "kw8", "kw9", "kw10"]
}}

Reglas:
- Sin tildes ni caracteres especiales (la API SECOP las rechaza)
- Palabras clave tecnicas del sector publico colombiano
- Primarias: las mas especificas y discriminantes
- Secundarias: terminos relacionados mas amplios
"""

    try:
        msg = client.messages.create(
            model="claude-opus-4-5",
            max_tokens=400,
            messages=[{"role": "user", "content": prompt}]
        )
        raw = msg.content[0].text.strip()
        raw = raw.replace("```json", "").replace("```", "").strip()
        return json.loads(raw)
    except Exception as e:
        logging.warning(f"Error generando keywords para {cod_indicador}: {e}")
        nombre_limpio = nombre_indicador[:50].replace("á","a").replace("é","e") \
                        .replace("í","i").replace("ó","o").replace("ú","u") \
                        .replace("ñ","n").replace("Á","A").replace("É","E") \
                        .replace("Í","I").replace("Ó","O").replace("Ú","U")
        palabras = [p for p in nombre_limpio.split() if len(p) > 4][:5]
        return {
            "kw_primarias": palabras if palabras else ["contrato", "servicio"],
            "kw_secundarias": ["municipio", "rural", "PDET", "comunidad"]
        }


def cargar_o_generar_keywords(df: pd.DataFrame, anthropic_key: str,
                               json_path: str = "keywords_v4.json") -> Dict:
    """Carga keywords del JSON cache o las genera con Claude para cada indicador unico."""
    import os

    # Indicadores unicos en el dataframe
    indicadores_unicos = (
        df[["Cod_indicador", "nombreindicador"]]
        .drop_duplicates(subset=["Cod_indicador"])
        .set_index("Cod_indicador")["nombreindicador"]
        .to_dict()
    )

    # Cargar cache existente
    cache = {}
    if os.path.exists(json_path):
        try:
            with open(json_path, "r", encoding="utf-8") as f:
                cache = json.load(f)
            print(f"  Keywords cargadas del cache: {len(cache)} indicadores")
        except Exception:
            pass

    # Generar solo los que faltan
    faltantes = [cod for cod in indicadores_unicos if cod not in cache]
    if faltantes and anthropic_key and anthropic_key != "TU_ANTHROPIC_KEY_AQUI":
        print(f"  Generando keywords para {len(faltantes)} indicadores nuevos...")
        for i, cod in enumerate(faltantes, 1):
            nombre = indicadores_unicos[cod]
            print(f"    [{i}/{len(faltantes)}] {cod}: {nombre[:60]}...")
            kws = generar_keywords_claude(nombre, cod, anthropic_key)
            cache[cod] = {"nombre": nombre, **kws}
            time.sleep(0.3)

        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(cache, f, ensure_ascii=False, indent=2)
        print(f"  Keywords guardadas en: {json_path}")
    elif faltantes:
        print(f"  AVISO: {len(faltantes)} indicadores sin keywords y sin API key de Anthropic.")
        print(f"         Se usaran keywords genericas basadas en el nombre del indicador.")
        for cod in faltantes:
            nombre = indicadores_unicos[cod]
            nombre_limpio = nombre.replace("á","a").replace("é","e").replace("í","i") \
                           .replace("ó","o").replace("ú","u").replace("ñ","n")
            palabras = [p for p in nombre_limpio.split() if len(p) > 4][:5]
            cache[cod] = {
                "nombre": nombre,
                "kw_primarias": palabras if palabras else ["contrato", "servicio"],
                "kw_secundarias": ["municipio", "rural", "PDET", "comunidad"]
            }

    return cache


# =============================================================================
# CONSTRUCCION DE QUERIES SOQL
# =============================================================================

def clause_estado() -> str:
    partes = " OR ".join([f"estado_contrato='{e}'" for e in ESTADOS_VALIDOS])
    return f"({partes})"


def clause_anno() -> str:
    return f"fecha_de_firma >= '{ANNO_MINIMO}-01-01T00:00:00.000'"


def clause_keywords(kw_lista: List[str]) -> str:
    partes = [
        f"objeto_del_contrato LIKE '%{kw.lower()}%'"
        for kw in kw_lista[:4]
    ]
    return "(" + " OR ".join(partes) + ")"


def clause_municipios(municipios: List[str]) -> str:
    munis_lower = [f"'{m.lower()}'" for m in municipios[:8]]
    en_ciudad = f"lower(ciudad) IN ({', '.join(munis_lower)})"
    en_texto = " OR ".join([
        f"objeto_del_contrato LIKE '%{m.lower()}%'"
        for m in municipios[:3]
    ])
    return f"({en_ciudad} OR {en_texto})"


def clause_departamentos(deptos: List[str]) -> str:
    deptos_lower = [f"'{d.lower()}'" for d in deptos]
    return f"lower(departamento) IN ({', '.join(deptos_lower)})"


def build_query_c1(kw_prim: List[str], municipios: List[str]) -> str:
    return (
        f"{clause_keywords(kw_prim)} AND "
        f"{clause_municipios(municipios)} AND "
        f"{clause_estado()} AND {clause_anno()}"
    )


def build_query_c2(kw_prim: List[str], kw_sec: List[str],
                   deptos: List[str]) -> str:
    kws_all = list(dict.fromkeys(kw_prim + kw_sec))
    return (
        f"{clause_keywords(kws_all)} AND "
        f"{clause_departamentos(deptos)} AND "
        f"{clause_estado()} AND {clause_anno()}"
    )


def build_query_c3(kw_prim: List[str], municipios: List[str],
                   deptos: List[str]) -> str:
    munis_lower  = [f"'{m.lower()}'" for m in municipios[:15]]
    deptos_lower = [f"'{d.lower()}'" for d in deptos]
    excl_mun  = f"NOT lower(ciudad) IN ({', '.join(munis_lower)})"
    excl_dep  = f"NOT lower(departamento) IN ({', '.join(deptos_lower)})"
    return (
        f"{clause_keywords(kw_prim)} AND "
        f"{excl_mun} AND {excl_dep} AND "
        f"{clause_estado()} AND {clause_anno()}"
    )


def build_query_c4(kw_prim: List[str]) -> str:
    """Capa 4: busqueda nacional sin filtro geografico. Solo keywords primarias."""
    return (
        f"{clause_keywords(kw_prim)} AND "
        f"{clause_estado()} AND {clause_anno()}"
    )


# =============================================================================
# CLIENTE SECOP II
# =============================================================================

class SECOPClient:
    def __init__(self, token: str = None):
        headers = {"Accept": "application/json"}
        if token and token != "TU_APP_TOKEN_AQUI":
            headers["X-App-Token"] = token
            print("  Token Socrata configurado.")
        else:
            print("  AVISO: Sin token Socrata. Limite ~1000 consultas/hora.")
        self.session = requests.Session()
        self.session.headers.update(headers)

    def query(self, where: str, limit: int = MAX_C1_C2) -> List[Dict]:
        params = {
            "$select": CAMPOS_SELECT,
            "$where": where,
            "$limit": limit,
            "$order": "fecha_de_firma DESC",
        }
        for intento in range(1, MAX_REINTENTOS + 1):
            try:
                r = self.session.get(SECOP_URL, params=params, timeout=TIMEOUT)
                if r.status_code == 400:
                    logging.error(f"Error 400 Bad Request. Query: {where[:300]}")
                    return []
                r.raise_for_status()
                return r.json()
            except requests.exceptions.ReadTimeout:
                espera = 10 * intento
                logging.warning(
                    f"Timeout intento {intento}/{MAX_REINTENTOS} — "
                    f"reintentando en {espera}s..."
                )
                print(f"      ⏱ Timeout intento {intento}/{MAX_REINTENTOS} — esperando {espera}s")
                time.sleep(espera)
            except requests.exceptions.ConnectionError as e:
                espera = 15 * intento
                logging.warning(f"Error de conexion intento {intento}/{MAX_REINTENTOS}: {e}")
                print(f"      ⚠ Error de conexion intento {intento}/{MAX_REINTENTOS} — esperando {espera}s")
                time.sleep(espera)
            except requests.exceptions.HTTPError as e:
                logging.warning(f"HTTP Error: {e}")
                return []
            except Exception as e:
                logging.warning(f"Error inesperado: {e}")
                time.sleep(5)
        logging.error(f"Fallaron {MAX_REINTENTOS} intentos — retornando lista vacia")
        print(f"      ✗ {MAX_REINTENTOS} intentos fallidos — se omite esta consulta")
        return []


# =============================================================================
# BASE DE DATOS SQLITE
# =============================================================================

def init_db(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS inventario (
            id                INTEGER PRIMARY KEY AUTOINCREMENT,
            subregion         TEXT NOT NULL,
            cod_subprograma   TEXT NOT NULL,
            subprograma       TEXT,
            cod_indicador     TEXT NOT NULL,
            nombre_indicador  TEXT,
            prioridad_ind     INTEGER DEFAULT 0,
            n_indicadores_sr  INTEGER DEFAULT 0,
            departamento_sr   TEXT,
            total_c1          INTEGER DEFAULT 0,
            total_c2          INTEGER DEFAULT 0,
            total_c3_otras_sr INTEGER DEFAULT 0,
            total_c4_nacional  INTEGER DEFAULT 0,
            total_contratos   INTEGER DEFAULT 0,
            annos_lista       TEXT,
            anno_min          INTEGER,
            anno_max          INTEGER,
            valor_total       REAL DEFAULT 0,
            valor_promedio    REAL DEFAULT 0,
            valor_min         REAL DEFAULT 0,
            valor_max         REAL DEFAULT 0,
            entidades_lista   TEXT,
            nivel_confianza   TEXT,
            hay_datos_propios INTEGER DEFAULT 0,
            requiere_ajuste   INTEGER DEFAULT 0,
            fuente_ajuste     TEXT,
            nota_ajuste       TEXT,
            sin_datos_contratos INTEGER DEFAULT 0,
            keywords_usadas   TEXT,
            timestamp         TEXT,
            UNIQUE(subregion, cod_subprograma)
        );

        CREATE TABLE IF NOT EXISTS contratos (
            id_contrato       TEXT NOT NULL,
            cod_indicador     TEXT NOT NULL,
            cod_subprograma   TEXT NOT NULL,
            subregion         TEXT NOT NULL,
            capa              INTEGER NOT NULL,
            referencia        TEXT,
            proceso_compra    TEXT,
            nombre_entidad    TEXT,
            nit_entidad       TEXT,
            departamento      TEXT,
            ciudad            TEXT,
            sector            TEXT,
            tipo_contrato     TEXT,
            modalidad         TEXT,
            objeto_contrato   TEXT,
            valor_contrato    REAL DEFAULT 0,
            anno_firma        INTEGER,
            fecha_firma       TEXT,
            fecha_fin         TEXT,
            estado_contrato   TEXT,
            proveedor         TEXT,
            es_pyme           TEXT,
            espostconflicto   TEXT,
            url_proceso       TEXT,
            fuente_pgn        REAL DEFAULT 0,
            fuente_sgr        REAL DEFAULT 0,
            fuente_sgp        REAL DEFAULT 0,
            duracion          TEXT,
            subregion_origen  TEXT,
            fecha_consulta    TEXT,
            PRIMARY KEY(id_contrato, cod_indicador, cod_subprograma, subregion, capa)
        );

        CREATE TABLE IF NOT EXISTS log_queries (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            subregion     TEXT,
            cod_subprograma TEXT,
            cod_indicador TEXT,
            capa          INTEGER,
            n_resultados  INTEGER,
            where_clause  TEXT,
            timestamp     TEXT
        );

        CREATE TABLE IF NOT EXISTS subregiones_sin_datos (
            subregion     TEXT PRIMARY KEY,
            motivo        TEXT,
            timestamp     TEXT
        );

        CREATE INDEX IF NOT EXISTS idx_inv_sr    ON inventario(subregion);
        CREATE INDEX IF NOT EXISTS idx_inv_sub   ON inventario(cod_subprograma);
        CREATE INDEX IF NOT EXISTS idx_cont_sr   ON contratos(subregion);
        CREATE INDEX IF NOT EXISTS idx_cont_ind  ON contratos(cod_indicador);
    """)
    conn.commit()
    return conn


def guardar_contratos_db(conn, contratos: List[Dict], cod_ind: str,
                         cod_subprog: str, subregion: str, capa: int,
                         subregion_origen: str = "propia"):
    ts = datetime.now().isoformat()
    cur = conn.cursor()
    for c in contratos:
        idc = c.get("id_contrato", "")
        if not idc:
            continue

        url = ""
        urlproceso = c.get("urlproceso")
        if isinstance(urlproceso, dict):
            url = urlproceso.get("url", "")

        anno = None
        fecha = c.get("fecha_de_firma", "")
        if fecha:
            try:
                anno = int(str(fecha)[:4])
            except Exception:
                pass

        cur.execute("""
            INSERT OR REPLACE INTO contratos (
                id_contrato, cod_indicador, cod_subprograma, subregion, capa,
                referencia, proceso_compra, nombre_entidad, nit_entidad,
                departamento, ciudad, sector, tipo_contrato, modalidad,
                objeto_contrato, valor_contrato,
                anno_firma, fecha_firma, fecha_fin, estado_contrato,
                proveedor, es_pyme, espostconflicto, url_proceso,
                fuente_pgn, fuente_sgr, fuente_sgp, duracion,
                subregion_origen, fecha_consulta
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            idc, cod_ind, cod_subprog, subregion, capa,
            c.get("referencia_del_contrato", ""),
            c.get("proceso_de_compra", ""),
            c.get("nombre_entidad", ""),
            c.get("nit_entidad", ""),
            c.get("departamento", ""),
            c.get("ciudad", ""),
            c.get("sector", ""),
            c.get("tipo_de_contrato", ""),
            c.get("modalidad_de_contratacion", ""),
            c.get("objeto_del_contrato", ""),
            float(c.get("valor_del_contrato", 0) or 0),
            anno, fecha,
            c.get("fecha_de_fin_del_contrato", ""),
            c.get("estado_contrato", ""),
            c.get("proveedor_adjudicado", ""),
            c.get("es_pyme", ""),
            c.get("espostconflicto", ""),
            url,
            float(c.get("presupuesto_general_de_la_nacion_pgn", 0) or 0),
            float(c.get("sistema_general_de_regal_as", 0) or 0),
            float(c.get("sistema_general_de_participaciones", 0) or 0),
            c.get("duraci_n_del_contrato", ""),
            subregion_origen, ts,
        ))
    conn.commit()


def calcular_inventario(conn, subregion: str, cod_subprog: str, subprograma: str,
                        cod_ind: str, nombre_ind: str, prioridad: int,
                        n_indicadores_sr: int, departamento: str,
                        c1: List, c2: List, c3: List, c4: List, kws_str: str):
    locales = c1 + c2

    valores = [
        float(c.get("valor_del_contrato", 0) or 0)
        for c in locales
        if float(c.get("valor_del_contrato", 0) or 0) > 0
    ]

    annos = []
    for c in locales:
        f = c.get("fecha_de_firma", "")
        if f:
            try:
                annos.append(int(str(f)[:4]))
            except Exception:
                pass

    entidades = list(dict.fromkeys([
        c.get("nombre_entidad", "")
        for c in locales
        if c.get("nombre_entidad", "")
    ]))[:10]

    if len(c1) >= 3:
        confianza = "Alto"
    elif len(c1) >= 1 or len(c2) >= 3:
        confianza = "Medio"
    elif len(c2) >= 1:
        confianza = "Bajo"
    elif len(c3) >= 1:
        confianza = "Solo otras subregiones"
    elif len(c4) >= 1:
        confianza = "Solo referencia nacional"
    else:
        confianza = "Sin datos"

    hay_datos  = 1 if confianza in ("Alto", "Medio", "Bajo") else 0
    req_ajuste = 0 if confianza in ("Alto", "Medio") else 1
    sin_datos  = 1 if confianza == "Sin datos" else 0

    if confianza in ("Alto", "Medio"):
        fuente = "Datos propios subregion"
        nota   = "Usar contratos capas 1 y 2 directamente para el costeo"
    elif confianza == "Bajo":
        fuente = "Departamento (ajuste menor)"
        nota   = "Aplicar factor de ajuste por escasez de contratos municipales"
    elif confianza == "Solo otras subregiones":
        fuente = "Otras subregiones PDET"
        nota   = "Usar contratos otras subregiones + factor de ajuste territorial"
    elif confianza == "Solo referencia nacional":
        fuente = "Referencia nacional"
        nota   = "Sin contratos en zonas PDET — usar contratos nacionales como referente de costeo"
    else:
        fuente = "Sin referencia contractual"
        nota   = "Requiere estimacion por metodos alternativos"

    conn.execute("""
        INSERT OR REPLACE INTO inventario (
            subregion, cod_subprograma, subprograma,
            cod_indicador, nombre_indicador, prioridad_ind, n_indicadores_sr,
            departamento_sr,
            total_c1, total_c2, total_c3_otras_sr, total_c4_nacional,
            total_contratos, annos_lista, anno_min, anno_max,
            valor_total, valor_promedio, valor_min, valor_max,
            entidades_lista, nivel_confianza, hay_datos_propios,
            requiere_ajuste, fuente_ajuste, nota_ajuste,
            sin_datos_contratos, keywords_usadas, timestamp
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, (
        subregion, cod_subprog, subprograma,
        cod_ind, nombre_ind, prioridad, n_indicadores_sr,
        departamento,
        len(c1), len(c2), len(c3), len(c4),
        len(locales),
        json.dumps(sorted(set(annos))),
        min(annos) if annos else None,
        max(annos) if annos else None,
        sum(valores) if valores else 0,
        round(sum(valores) / len(valores), 0) if valores else 0,
        min(valores) if valores else 0,
        max(valores) if valores else 0,
        json.dumps(entidades),
        confianza, hay_datos, req_ajuste,
        fuente, nota,
        sin_datos,
        kws_str, datetime.now().isoformat(),
    ))
    conn.commit()


def log_query(conn, sr: str, cod_subprog: str, cod: str,
              capa: int, n: int, where: str):
    conn.execute(
        "INSERT INTO log_queries "
        "(subregion, cod_subprograma, cod_indicador, capa, "
        " n_resultados, where_clause, timestamp) "
        "VALUES (?,?,?,?,?,?,?)",
        (sr, cod_subprog, cod, capa, n, (where or "")[:600],
         datetime.now().isoformat()),
    )
    conn.commit()


# =============================================================================
# CARGA Y SELECCION DE INDICADOR POR PRIORIDAD
# =============================================================================

def cargar_excel(excel_path: str, hoja: str = HOJA_ENTRADA) -> pd.DataFrame:
    print(f"\nCargando: {excel_path} | Hoja: {hoja}")
    try:
        df = pd.read_excel(excel_path, sheet_name=hoja)
    except FileNotFoundError:
        print(f"\nERROR: No se encontro el archivo: {excel_path}")
        sys.exit(1)
    except Exception as e:
        print(f"\nERROR al leer el Excel: {e}")
        sys.exit(1)

    # Normalizar nombre de subregion
    col_sr = None
    for posible in ["Subregión", "Subregion", df.columns[0]]:
        if posible in df.columns:
            col_sr = posible
            break

    df["Subregion_raw"] = df[col_sr].astype(str).str.strip()
    df["Subregion_norm"] = df["Subregion_raw"].apply(
        lambda x: ALIAS_MAP.get(x, x)
    )

    # Asegurar tipo numerico en prioridad
    df["prioridadsubprogramasguillermo"] = pd.to_numeric(
        df["prioridadsubprogramasguillermo"], errors="coerce"
    ).fillna(0)

    print(f"  OK: {len(df)} filas | {df['Subregion_norm'].nunique()} subregiones")
    for sr in sorted(df["Subregion_norm"].unique()):
        n  = len(df[df["Subregion_norm"] == sr])
        ok = "OK" if sr in SUBREGIONES else "NO RECONOCIDA"
        print(f"    [{n:3d} filas] {sr} [{ok}]")

    return df


def seleccionar_indicador_por_prioridad(
    df_subprog: pd.DataFrame
) -> Tuple[str, str, int, int]:
    """
    Dado un grupo (subregion + subprograma) con posiblemente varios indicadores,
    devuelve el indicador con mayor prioridadsubprogramasguillermo.
    En caso de empate, usa el primero segun orden del DataFrame.

    Retorna: (cod_indicador, nombre_indicador, prioridad, n_indicadores_total)
    """
    n_total = len(df_subprog)

    if n_total == 0:
        return "", "", 0, 0

    # Seleccionar el de mayor prioridad (idxmax respeta el primer empate)
    idx_max = df_subprog["prioridadsubprogramasguillermo"].idxmax()
    row     = df_subprog.loc[idx_max]

    return (
        str(row["Cod_indicador"]).strip(),
        str(row["nombreindicador"]).strip(),
        int(row["prioridadsubprogramasguillermo"]),
        n_total
    )


# =============================================================================
# EXPORTACION A EXCEL
# =============================================================================

def exportar_excel(conn, excel_out: str):
    print(f"\nExportando a: {excel_out}")

    with pd.ExcelWriter(excel_out, engine="openpyxl") as writer:

        # Hoja 1 - Inventario completo
        df_inv = pd.read_sql_query("""
            SELECT
                subregion, cod_subprograma, subprograma,
                cod_indicador, nombre_indicador,
                prioridad_ind AS prioridad_indicador_seleccionado,
                n_indicadores_sr AS n_indicadores_disponibles_subprograma,
                departamento_sr, nivel_confianza,
                hay_datos_propios, requiere_ajuste, sin_datos_contratos,
                total_contratos,
                total_c1  AS contratos_subregion_exacta,
                total_c2  AS contratos_departamento,
                total_c3_otras_sr AS contratos_otras_subregiones,
                total_c4_nacional AS contratos_referencia_nacional,
                anno_min, anno_max, annos_lista,
                ROUND(valor_total, 0)    AS valor_total_cop,
                ROUND(valor_promedio, 0) AS valor_promedio_cop,
                ROUND(valor_min, 0)      AS valor_min_cop,
                ROUND(valor_max, 0)      AS valor_max_cop,
                fuente_ajuste, nota_ajuste,
                entidades_lista, keywords_usadas
            FROM inventario
            ORDER BY subregion, cod_subprograma
        """, conn)
        df_inv.to_excel(writer, sheet_name="01_Inventario_Completo", index=False)

        # Hoja 2 - Con datos propios
        df_con = df_inv[df_inv["hay_datos_propios"] == 1].copy()
        df_con.to_excel(writer, sheet_name="02_Con_Datos_Propios", index=False)

        # Hoja 3 - Requieren ajuste
        df_aj = df_inv[df_inv["requiere_ajuste"] == 1].copy()
        df_aj.to_excel(writer, sheet_name="03_Requieren_Ajuste", index=False)

        # Hoja 4 - Subregiones/subprogramas sin datos de contratos
        df_sd = df_inv[df_inv["sin_datos_contratos"] == 1].copy()
        df_sd.to_excel(writer, sheet_name="04_Sin_Datos_Contratos", index=False)

        # Hoja 4b - Contratos referencia nacional (capa 4)
        df_c4 = pd.read_sql_query("""
            SELECT
                c.subregion, c.cod_subprograma,
                i.subprograma, c.cod_indicador, i.nombre_indicador,
                c.id_contrato, c.referencia,
                c.nombre_entidad, c.departamento, c.ciudad,
                c.tipo_contrato, c.objeto_contrato,
                ROUND(c.valor_contrato, 0) AS valor_contrato_cop,
                c.anno_firma, c.estado_contrato, c.url_proceso
            FROM contratos c
            LEFT JOIN inventario i
                ON c.cod_subprograma = i.cod_subprograma
               AND c.subregion       = i.subregion
            WHERE c.capa = 4
            ORDER BY c.subregion, c.cod_subprograma, c.valor_contrato DESC
        """, conn)
        df_c4.to_excel(writer, sheet_name="04b_Contratos_Ref_Nacional", index=False)

        # Hoja 5 - Contratos IDs y URLs (capas 1 y 2)
        df_urls = pd.read_sql_query("""
            SELECT
                c.subregion, c.cod_subprograma,
                i.subprograma, c.cod_indicador, i.nombre_indicador,
                c.capa,
                CASE c.capa
                    WHEN 1 THEN 'Subregion exacta'
                    WHEN 2 THEN 'Departamento'
                    WHEN 3 THEN 'Otra subregion PDET'
                END AS nivel_geografico,
                c.id_contrato, c.referencia, c.proceso_compra,
                c.nombre_entidad, c.departamento, c.ciudad,
                c.tipo_contrato, c.objeto_contrato,
                ROUND(c.valor_contrato, 0) AS valor_contrato_cop,
                c.anno_firma, c.estado_contrato,
                c.es_pyme, c.espostconflicto, c.duracion,
                c.url_proceso,
                ROUND(c.fuente_pgn, 0) AS fuente_pgn,
                ROUND(c.fuente_sgr, 0) AS fuente_sgr,
                ROUND(c.fuente_sgp, 0) AS fuente_sgp
            FROM contratos c
            LEFT JOIN inventario i
                ON c.cod_subprograma = i.cod_subprograma
               AND c.subregion       = i.subregion
            WHERE c.capa IN (1, 2)
            ORDER BY c.subregion, c.cod_subprograma, c.capa, c.valor_contrato DESC
        """, conn)
        df_urls.to_excel(writer, sheet_name="05_Contratos_IDs_URLs", index=False)

        # Hoja 6 - Contratos otras subregiones (capa 3, factor de ajuste)
        df_c3 = pd.read_sql_query("""
            SELECT
                c.subregion        AS subregion_sin_datos,
                c.cod_subprograma,
                i.subprograma, c.cod_indicador, i.nombre_indicador,
                c.subregion_origen AS subregion_referencia,
                c.id_contrato, c.referencia,
                c.nombre_entidad, c.departamento, c.ciudad,
                c.tipo_contrato, c.objeto_contrato,
                ROUND(c.valor_contrato, 0) AS valor_contrato_cop,
                c.anno_firma, c.estado_contrato, c.url_proceso
            FROM contratos c
            LEFT JOIN inventario i
                ON c.cod_subprograma = i.cod_subprograma
               AND c.subregion       = i.subregion
            WHERE c.capa = 3
            ORDER BY c.subregion, c.cod_subprograma, c.valor_contrato DESC
        """, conn)
        df_c3.to_excel(writer, sheet_name="06_Contratos_Otras_SR_Ajuste", index=False)

        # Hoja 7 - Resumen por subregion
        df_sr = pd.read_sql_query("""
            SELECT
                subregion,
                COUNT(*) AS total_subprogramas,
                SUM(CASE WHEN nivel_confianza='Alto'   THEN 1 ELSE 0 END) AS alta,
                SUM(CASE WHEN nivel_confianza='Medio'  THEN 1 ELSE 0 END) AS media,
                SUM(CASE WHEN nivel_confianza='Bajo'   THEN 1 ELSE 0 END) AS baja,
                SUM(CASE WHEN nivel_confianza='Solo otras subregiones'
                         THEN 1 ELSE 0 END) AS solo_referencia,
                SUM(CASE WHEN nivel_confianza='Sin datos'
                         THEN 1 ELSE 0 END) AS sin_datos,
                SUM(total_contratos)       AS total_contratos_locales,
                ROUND(SUM(valor_total), 0) AS inversion_total_cop,
                SUM(requiere_ajuste)       AS subprogramas_con_ajuste
            FROM inventario
            GROUP BY subregion
            ORDER BY total_contratos_locales DESC
        """, conn)
        df_sr.to_excel(writer, sheet_name="07_Resumen_Subregiones", index=False)

        # Hoja 8 - Log de ejecucion
        df_log = pd.read_sql_query(
            "SELECT * FROM log_queries ORDER BY timestamp", conn
        )
        df_log.to_excel(writer, sheet_name="08_Log", index=False)

    print(f"  Guardado: {excel_out}")


# =============================================================================
# PROCESO PRINCIPAL BATCH — ITERACION POR SUBREGION
# =============================================================================

def run_batch(excel_path=EXCEL_ENTRADA, hoja=HOJA_ENTRADA,
              db_path=DB_PATH, excel_out=EXCEL_SALIDA,
              token=SOCRATA_TOKEN, resume=True):
    """
    Proceso principal iterativo:
      Para cada subregion:
        Para cada subprograma dentro de la subregion:
          - Selecciona el indicador de mayor prioridad
          - Busca contratos en SECOP (3 capas)
          - Guarda resultados
    """
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.FileHandler(LOG_FILE, encoding="utf-8"),
            logging.StreamHandler(sys.stdout),
        ],
    )

    df     = cargar_excel(excel_path, hoja)
    kws    = cargar_o_generar_keywords(df, ANTHROPIC_KEY)
    client = SECOPClient(token)
    conn   = init_db(db_path)

    # Reanudacion: pares (subregion, cod_subprograma) ya procesados
    ejecutados = set()
    if resume:
        cur = conn.execute(
            "SELECT subregion || '|' || cod_subprograma FROM inventario"
        )
        ejecutados = {r[0] for r in cur.fetchall()}
        if ejecutados:
            print(f"\nReanudando: {len(ejecutados)} pares ya procesados (se saltaran)")

    subregiones_en_df = sorted(df["Subregion_norm"].unique())
    total_sr = len(subregiones_en_df)

    print(f"\n{'='*65}")
    print(f"  FASE 1 v4 - ITERACION POR SUBREGION")
    print(f"  Subregiones a procesar: {total_sr}")
    print(f"{'='*65}\n")

    for i_sr, subregion_norm in enumerate(subregiones_en_df, 1):
        print(f"\n{'─'*65}")
        print(f"  [{i_sr}/{total_sr}] SUBREGION: {subregion_norm}")
        print(f"{'─'*65}")

        # Verificar si la subregion existe en el diccionario geografico
        sr_data = SUBREGIONES.get(subregion_norm)
        if not sr_data:
            msg = f"Subregion '{subregion_norm}' no encontrada en diccionario geografico."
            logging.warning(msg)
            print(f"  AVISO: {msg}")
            conn.execute(
                "INSERT OR REPLACE INTO subregiones_sin_datos "
                "(subregion, motivo, timestamp) VALUES (?,?,?)",
                (subregion_norm, "Sin configuracion geografica",
                 datetime.now().isoformat())
            )
            conn.commit()
            continue

        municipios  = sr_data["municipios"]
        deptos      = sr_data["departamentos"]
        depto_ppal  = sr_data["departamento_principal"]

        # Filtrar filas de esta subregion
        df_sr = df[df["Subregion_norm"] == subregion_norm].copy()

        # Agrupar por subprograma
        subprogramas = sorted(df_sr["CodSubprograma"].unique())
        print(f"  Subprogramas: {len(subprogramas)}")

        for i_sp, cod_subprog in enumerate(subprogramas, 1):
            clave = f"{subregion_norm}|{cod_subprog}"
            if resume and clave in ejecutados:
                continue

            df_subprog = df_sr[df_sr["CodSubprograma"] == cod_subprog].copy()
            nombre_subprog = df_subprog["subprograma"].iloc[0] \
                             if "subprograma" in df_subprog.columns else cod_subprog

            # Seleccionar indicador de mayor prioridad
            cod_ind, nombre_ind, prioridad, n_ind_total = \
                seleccionar_indicador_por_prioridad(df_subprog)

            if not cod_ind:
                logging.warning(f"Sin indicador para {cod_subprog} en {subregion_norm}")
                continue

            ind_data = kws.get(cod_ind)
            if not ind_data:
                logging.warning(f"Sin keywords para {cod_ind} — saltando")
                continue

            kw_prim = ind_data.get("kw_primarias", [])
            kw_sec  = ind_data.get("kw_secundarias", [])
            kws_str = "; ".join(kw_prim[:3])

            print(
                f"    [{i_sp}/{len(subprogramas)}] {cod_subprog} | "
                f"Indicador seleccionado: {cod_ind} (prio={prioridad}, "
                f"{n_ind_total} candidatos)"
            )

            c1, c2, c3 = [], [], []

            # CAPA 1: subregion exacta
            q1 = build_query_c1(kw_prim, municipios)
            c1 = client.query(q1, limit=MAX_C1_C2)
            log_query(conn, subregion_norm, cod_subprog, cod_ind, 1, len(c1), q1)
            if c1:
                guardar_contratos_db(conn, c1, cod_ind, cod_subprog,
                                     subregion_norm, capa=1)
            time.sleep(PAUSA)

            # CAPA 2: departamento (si capa 1 tiene menos de 5)
            if len(c1) < 5:
                q2 = build_query_c2(kw_prim, kw_sec, deptos)
                c2_raw = client.query(q2, limit=MAX_C1_C2)
                ids_c1 = {c.get("id_contrato") for c in c1}
                c2 = [c for c in c2_raw if c.get("id_contrato") not in ids_c1]
                log_query(conn, subregion_norm, cod_subprog, cod_ind, 2, len(c2), q2)
                if c2:
                    guardar_contratos_db(conn, c2, cod_ind, cod_subprog,
                                         subregion_norm, capa=2)
                time.sleep(PAUSA)

            # CAPA 3: otras subregiones (solo si capas 1 y 2 vacias)
            if len(c1) == 0 and len(c2) == 0:
                q3 = build_query_c3(kw_prim, municipios, deptos)
                c3_raw = client.query(q3, limit=MAX_C3)
                ids_prev = {c.get("id_contrato") for c in c1 + c2}
                c3 = [c for c in c3_raw if c.get("id_contrato") not in ids_prev]
                log_query(conn, subregion_norm, cod_subprog, cod_ind, 3, len(c3), q3)
                if c3:
                    guardar_contratos_db(
                        conn, c3, cod_ind, cod_subprog, subregion_norm,
                        capa=3, subregion_origen="otras_subregiones_PDET"
                    )
                time.sleep(PAUSA)

            # CAPA 4: busqueda nacional (solo si capas 1, 2 y 3 retornan 0)
            c4 = []
            if len(c1) == 0 and len(c2) == 0 and len(c3) == 0:
                q4 = build_query_c4(kw_prim)
                c4_raw = client.query(q4, limit=MAX_C4)
                ids_prev = {c.get("id_contrato") for c in c1 + c2 + c3}
                c4 = [c for c in c4_raw if c.get("id_contrato") not in ids_prev]
                log_query(conn, subregion_norm, cod_subprog, cod_ind, 4, len(c4), q4)
                if c4:
                    guardar_contratos_db(
                        conn, c4, cod_ind, cod_subprog, subregion_norm,
                        capa=4, subregion_origen="nacional"
                    )
                    print(f"      🌐 Capa 4 nacional: {len(c4)} contratos encontrados como referente")
                time.sleep(PAUSA)

            # Indicar explicitamente si no hay datos en ninguna capa
            sin_datos_locales = (len(c1) == 0 and len(c2) == 0 and len(c3) == 0 and len(c4) == 0)
            if sin_datos_locales:
                print(f"      ⚠ Sin contratos en ninguna capa para {cod_subprog} en {subregion_norm}")

            calcular_inventario(
                conn, subregion_norm, cod_subprog, nombre_subprog,
                cod_ind, nombre_ind, prioridad, n_ind_total,
                depto_ppal, c1, c2, c3, c4, kws_str
            )

        print(f"  ✓ {subregion_norm} completada")

    exportar_excel(conn, excel_out)

    # Estadisticas finales
    n_inv   = conn.execute("SELECT COUNT(*) FROM inventario").fetchone()[0]
    n_ok    = conn.execute(
        "SELECT COUNT(*) FROM inventario WHERE nivel_confianza IN ('Alto','Medio')"
    ).fetchone()[0]
    n_sd    = conn.execute(
        "SELECT COUNT(*) FROM inventario WHERE sin_datos_contratos=1"
    ).fetchone()[0]
    n_c4    = conn.execute(
        "SELECT COUNT(*) FROM inventario WHERE nivel_confianza='Solo referencia nacional'"
    ).fetchone()[0]
    n_cont  = conn.execute(
        "SELECT COUNT(DISTINCT id_contrato) FROM contratos WHERE capa<=2"
    ).fetchone()[0]
    conn.close()

    print(f"\n{'='*65}")
    print(f"  FASE 1 v4 COMPLETADA")
    print(f"  Subprogramas procesados          : {n_inv}")
    print(f"  Con datos propios (Alto/Medio)   : {n_ok}")
    print(f"  Sin datos de contratos           : {n_sd}")
    print(f"  Solo referente nacional (capa 4) : {n_c4}")
    print(f"  Contratos unicos (capas 1 y 2)   : {n_cont}")
    print(f"  Base de datos SQLite             : {db_path}")
    print(f"  Excel de resultados              : {excel_out}")
    print(f"{'='*65}")


# =============================================================================
# MODO TEST — prueba una subregion completa
# =============================================================================

def run_test(subregion: str = "Alto Patia y Norte del Cauca",
             token: str = SOCRATA_TOKEN,
             excel_path: str = EXCEL_ENTRADA,
             hoja: str = HOJA_ENTRADA):
    """
    Prueba el proceso para UNA subregion: muestra los indicadores seleccionados
    por subprograma y ejecuta la busqueda SECOP para el primer subprograma.
    """
    df      = cargar_excel(excel_path, hoja)
    kws     = cargar_o_generar_keywords(df, ANTHROPIC_KEY)
    client  = SECOPClient(token)

    # Resolver alias
    sr_norm = ALIAS_MAP.get(subregion, subregion)
    sr_data = SUBREGIONES.get(sr_norm)

    if not sr_data:
        print(f"\nSubregion '{sr_norm}' no encontrada.")
        print(f"Disponibles: {list(SUBREGIONES.keys())}")
        return

    df_sr = df[df["Subregion_norm"] == sr_norm]
    if df_sr.empty:
        print(f"\nNo hay datos en el Excel para: {sr_norm}")
        return

    print(f"\n{'='*65}")
    print(f"  MODO TEST — Subregion: {sr_norm}")
    print(f"  Total subprogramas: {df_sr['CodSubprograma'].nunique()}")
    print(f"{'='*65}")

    for cod_subprog in sorted(df_sr["CodSubprograma"].unique()):
        df_sp = df_sr[df_sr["CodSubprograma"] == cod_subprog]
        cod_ind, nombre_ind, prio, n_total = seleccionar_indicador_por_prioridad(df_sp)
        print(
            f"  {cod_subprog} | Indicador: {cod_ind} | "
            f"Prioridad: {prio} | Candidatos: {n_total}"
        )
        print(f"    {nombre_ind[:80]}...")

    # Ejecutar busqueda SECOP para el primer subprograma
    primer_sp = sorted(df_sr["CodSubprograma"].unique())[0]
    df_p = df_sr[df_sr["CodSubprograma"] == primer_sp]
    cod_ind, nombre_ind, prio, _ = seleccionar_indicador_por_prioridad(df_p)
    ind_data = kws.get(cod_ind, {})
    kw_prim  = ind_data.get("kw_primarias", [])
    kw_sec   = ind_data.get("kw_secundarias", [])
    munis    = sr_data["municipios"]
    deptos   = sr_data["departamentos"]

    print(f"\n  Probando SECOP para: {primer_sp} | {cod_ind}")
    print(f"  KW primarias: {kw_prim}")

    capas_anteriores = []

    for capa, label, query in [
        (1, "Subregion exacta",       build_query_c1(kw_prim, munis)),
        (2, "Departamento",           build_query_c2(kw_prim, kw_sec, deptos)),
        (3, "Otras subregiones PDET", build_query_c3(kw_prim, munis, deptos)),
    ]:
        print(f"\n  CAPA {capa} - {label}")
        resultados = client.query(query, limit=5)
        capas_anteriores.extend(resultados)
        print(f"  Contratos encontrados: {len(resultados)}")
        for c in resultados[:3]:
            val    = float(c.get("valor_del_contrato", 0) or 0)
            estado = c.get("estado_contrato", "")
            ent    = c.get("nombre_entidad", "")
            ciudad = c.get("ciudad", "")
            obj    = (c.get("objeto_del_contrato", "") or "")[:75]
            url    = c.get("urlproceso", {})
            url    = url.get("url", "") if isinstance(url, dict) else ""
            print(f"    [{estado}] {ent} | {ciudad}")
            print(f"    {obj}...")
            print(f"    Valor: ${val:,.0f} COP")
            if url:
                print(f"    URL: {url[:70]}")
        time.sleep(PAUSA)

    # CAPA 4: nacional - solo si capas 1, 2 y 3 retornaron 0
    print(f"\n  CAPA 4 - Nacional (referente)")
    if len(capas_anteriores) == 0:
        q4 = build_query_c4(kw_prim)
        resultados_c4 = client.query(q4, limit=5)
        print(f"  Contratos encontrados: {len(resultados_c4)}")
        for c in resultados_c4[:3]:
            val    = float(c.get("valor_del_contrato", 0) or 0)
            estado = c.get("estado_contrato", "")
            ent    = c.get("nombre_entidad", "")
            ciudad = c.get("ciudad", "")
            obj    = (c.get("objeto_del_contrato", "") or "")[:75]
            url    = c.get("urlproceso", {})
            url    = url.get("url", "") if isinstance(url, dict) else ""
            print(f"    [{estado}] {ent} | {ciudad}")
            print(f"    {obj}...")
            print(f"    Valor: ${val:,.0f} COP")
            if url:
                print(f"    URL: {url[:70]}")
        time.sleep(PAUSA)
    else:
        print(f"  (Omitida — capas 1-3 ya encontraron {len(capas_anteriores)} contratos)")


# =============================================================================
# MODO EXPORTAR
# =============================================================================

def run_exportar(db_path: str = DB_PATH, excel_out: str = EXCEL_SALIDA):
    import os
    if not os.path.exists(db_path):
        print(f"ERROR: No se encontro la base de datos: {db_path}")
        return
    conn = sqlite3.connect(db_path)
    exportar_excel(conn, excel_out)
    conn.close()


# =============================================================================
# PUNTO DE ENTRADA
# =============================================================================

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Fase 1 v4 - Inventario SECOP por Subregion x Subprograma PDET"
    )
    parser.add_argument(
        "--modo", choices=["batch", "test", "exportar"], default="batch",
        help="batch=proceso completo | test=probar una subregion | exportar=solo Excel"
    )
    parser.add_argument(
        "--subregion", default="Alto Patia y Norte del Cauca",
        help="Nombre de la subregion para modo test"
    )
    parser.add_argument(
        "--token", default=None,
        help="App Token de Socrata (recomendado)"
    )
    parser.add_argument(
        "--excel-entrada", default=EXCEL_ENTRADA,
        help="Ruta al Excel de entrada"
    )
    parser.add_argument(
        "--hoja", default=HOJA_ENTRADA,
        help="Nombre de la hoja del Excel"
    )
    parser.add_argument(
        "--db", default=DB_PATH,
        help="Ruta de la base de datos SQLite"
    )
    parser.add_argument(
        "--excel-salida", default=EXCEL_SALIDA,
        help="Ruta del Excel de resultados"
    )
    parser.add_argument(
        "--no-resume", action="store_true",
        help="Reiniciar desde cero ignorando progreso guardado"
    )

    args  = parser.parse_args()
    token = args.token if args.token else SOCRATA_TOKEN

    if args.modo == "test":
        run_test(args.subregion, token, args.excel_entrada, args.hoja)
    elif args.modo == "exportar":
        run_exportar(args.db, args.excel_salida)
    else:
        run_batch(
            excel_path=args.excel_entrada,
            hoja=args.hoja,
            db_path=args.db,
            excel_out=args.excel_salida,
            token=token,
            resume=not args.no_resume,
        )
