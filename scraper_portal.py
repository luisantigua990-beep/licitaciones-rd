"""
Scraper en Tiempo Real — Portal Transaccional DGCP
====================================================
Monitorea comunidad.comprasdominicana.gob.do cada 3 minutos.
A diferencia de la API (Data Warehouse, ~8h de delay), este portal
refleja los procesos en tiempo real apenas las instituciones los publican.

Flujo:
  1. Raspa la tabla HTML del portal público
  2. Extrae código de referencia (= codigo_proceso)
  3. Compara contra Supabase — si es nuevo, notifica INMEDIATAMENTE
  4. Guarda el proceso básico en Supabase (la API enriquece después con UNSPSC)
"""

import os
import time
import requests
from datetime import datetime
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from supabase import create_client

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
PORTAL_URL = (
    "https://comunidad.comprasdominicana.gob.do/Public/Tendering/"
    "ContractNoticeManagement/Index?currentLanguage=es&Country=DO&Theme=DGCP"
)

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

def registrar_cron_log(job: str, status: str = "ok", detalle: dict = None, duracion_ms: int = None):
    """Registra ejecución del job en cron_log para auditoría."""
    try:
        supabase.table("cron_log").insert({
            "job": job,
            "status": status,
            "detalle": detalle or {},
            "duracion_ms": duracion_ms,
        }).execute()
    except Exception as e:
        print(f"⚠️  cron_log: {e}")


HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "es-DO,es;q=0.9",
}


# ============================================
# SCRAPING DEL PORTAL
# ============================================

def raspar_portal():
    """
    Descarga y parsea la tabla de procesos del portal público.
    Retorna lista de dicts con los campos visibles en la tabla.
    """
    try:
        resp = requests.get(PORTAL_URL, headers=HEADERS, timeout=30)
        resp.raise_for_status()
    except Exception as e:
        print(f"❌ Error accediendo al portal: {e}")
        return []

    soup = BeautifulSoup(resp.text, "html.parser")

    # La tabla de procesos tiene columnas: País | Unidad de Compras | Referencia |
    # Descripción | Fase actual | Fecha de publicación | Fecha ofertas | Total | Estado
    procesos = []
    rows = soup.select("table tr")

    for row in rows:
        cols = row.find_all("td")
        if len(cols) < 9:
            continue

        textos = [c.get_text(strip=True) for c in cols]

        # Verificar que la primera columna sea "DO" (República Dominicana)
        if textos[0] != "DO":
            continue

        referencia = textos[2].strip()
        if not referencia or referencia == "Referencia":
            continue

        # Extraer noticeUID del onclick de la fila
        # El portal usa JS concatenado: 'noticeUID=' + 'DO1.NTC.XXXXX' + '&'
        # Está en el onclick del elemento lnkDetailLink de cada fila
        import re as _re
        notice_uid = None
        # Buscar en todos los elementos con onclick en esta fila
        for el in row.find_all(onclick=True):
            onclick_val = el.get("onclick", "")
            # Patrón JS concatenado: 'noticeUID=' + 'DO1.NTC.1682341' + '&'
            m = _re.search(
                r"noticeUID='\s*\+\s*'(DO1\.NTC\.[\w\.]+)'",
                onclick_val
            )
            if m:
                notice_uid = m.group(1)
                break
        # También buscar en el HTML completo de la fila como fallback
        if not notice_uid:
            fila_html = str(row)
            m = _re.search(
                r"noticeUID='\s*\+\s*'(DO1\.NTC\.[\w\.]+)'",
                fila_html
            )
            if m:
                notice_uid = m.group(1)

        # Construir URL: con noticeUID si existe, fallback a lista filtrada
        PORTAL_BASE = "https://comunidad.comprasdominicana.gob.do"
        if notice_uid:
            url_proceso = f"{PORTAL_BASE}/Public/Tendering/OpportunityDetail/Index?noticeUID={notice_uid}"
        else:
            url_proceso = (
                f"{PORTAL_BASE}/Public/Tendering/ContractNoticeManagement/Index"
                f"?currentLanguage=es&Country=DO&Theme=DGCP&NoticeReference={referencia}"
            )

        # Parsear fecha de publicación (formato: DD/MM/YYYY HH:MM (UTC -4 horas))
        fecha_str = textos[5].replace("(UTC -4 horas)", "").strip()
        fecha_publicacion = None
        try:
            fecha_publicacion = datetime.strptime(fecha_str, "%d/%m/%Y %H:%M").isoformat()
        except Exception:
            pass

        # Parsear fecha de cierre de ofertas (columna 6)
        fecha_cierre_str = textos[6].replace("(UTC -4 horas)", "").strip()
        fecha_cierre = None
        try:
            fecha_cierre = datetime.strptime(fecha_cierre_str, "%d/%m/%Y %H:%M").isoformat()
        except Exception:
            pass

        # Parsear monto (puede tener "Pesos Dominicanos" o vacío)
        monto_raw = textos[7].replace("Pesos Dominicanos", "").replace(",", "").strip()
        monto = None
        try:
            monto = float(monto_raw)
        except Exception:
            pass

        procesos.append({
            "codigo_proceso": referencia,
            "unidad_compra": textos[1].strip(),
            "titulo": textos[3].strip(),
            "fase_actual": textos[4].strip(),
            "fecha_publicacion": fecha_publicacion,
            "fecha_fin_recepcion_ofertas": fecha_cierre,
            "monto_estimado": monto,
            "estado_proceso": textos[8].strip(),
            "url": url_proceso,
            "notice_uid": notice_uid,  # campo temporal para log
        })

    print(f"🌐 Portal: {len(procesos)} procesos encontrados")
    return procesos


# ============================================
# DETECCIÓN Y GUARDADO DE NUEVOS
# ============================================

def obtener_codigos_existentes_portal(codigos):
    """Verifica qué códigos ya existen en Supabase."""
    if not codigos:
        return set()
    existentes = set()
    for i in range(0, len(codigos), 50):
        bloque = codigos[i:i + 50]
        result = (
            supabase.table("procesos")
            .select("codigo_proceso")
            .in_("codigo_proceso", bloque)
            .execute()
        )
        existentes.update(r["codigo_proceso"] for r in result.data)
    return existentes


def guardar_proceso_basico(proceso):
    """
    Guarda el proceso con los datos del portal (sin UNSPSC).
    La API DGCP enriquecerá después con los artículos completos.
    """
    try:
        supabase.table("procesos").insert({
            "codigo_proceso": proceso["codigo_proceso"],
            "unidad_compra": proceso["unidad_compra"],
            "titulo": proceso["titulo"],
            "estado_proceso": proceso["estado_proceso"],
            "monto_estimado": proceso["monto_estimado"],
            "fecha_publicacion": proceso["fecha_publicacion"],
            "fecha_fin_recepcion_ofertas": proceso.get("fecha_fin_recepcion_ofertas"),
            "url": proceso["url"],
            "notificado": False,
            # Marcar como "pendiente enriquecimiento" para que la API lo complete
            "enriquecido_api": False,
        }).execute()
        return True
    except Exception as e:
        # Si ya existe (race condition con la API), ignorar
        if "duplicate" in str(e).lower() or "unique" in str(e).lower():
            return False
        print(f"⚠️  Error guardando {proceso['codigo_proceso']}: {e}")
        return False


# ============================================
# NOTIFICACIONES INMEDIATAS
# ============================================

def obtener_url_proceso(codigo_proceso, url_portal=None):
    """
    Devuelve la mejor URL posible para abrir el proceso directamente.
    Prioridad:
      1. URL del portal transaccional ya construida (comprasdominicana.gob.do)
      2. URL del campo 'url' de la API, solo si es del portal transaccional
      3. Fallback: URL del portal construida con el codigo
    Nunca devuelve una URL de datosabiertos.dgcp.gob.do (no util para el usuario).
    """
    PORTAL_BASE = "https://comunidad.comprasdominicana.gob.do"

    # 1. Si ya tenemos URL del portal, usarla
    if url_portal and "comprasdominicana.gob.do" in url_portal:
        return url_portal

    # 2. Intentar API DGCP
    API_BASE_URL = "https://datosabiertos.dgcp.gob.do/api-dgcp/v1"
    try:
        resp = requests.get(
            f"{API_BASE_URL}/procesos",
            params={"proceso": codigo_proceso, "limit": 1},
            timeout=10
        )
        resp.raise_for_status()
        data = resp.json()
        payload = data.get("payload")
        procesos = []
        if isinstance(payload, list):
            procesos = payload
        elif isinstance(payload, dict):
            procesos = payload.get("content", []) or []
        if procesos:
            url_api = procesos[0].get("url", "")
            # Solo usar si apunta al portal, no a datosabiertos
            if url_api and "comprasdominicana.gob.do" in url_api:
                return url_api
    except Exception:
        pass

    # 3. Fallback: construir URL del portal con el codigo del proceso
    return (
        f"{PORTAL_BASE}/Public/Tendering/ContractNoticeManagement/Index"
        f"?currentLanguage=es&Country=DO&Theme=DGCP&NoticeReference={codigo_proceso}"
    )

def obtener_articulos_rapido(codigo_proceso, url_portal=None):
    """
    Alias de compatibilidad — ahora intenta primero scraping del portal (inmediato)
    y hace fallback a la API de datos abiertos (tiene delay de 8h).
    """
    # Intentar scraping directo del portal con noticeUID
    articulos = scraper_articulos_portal(codigo_proceso, url_portal=url_portal)
    if articulos:
        return articulos

    # Fallback: API de datos abiertos
    API_BASE_URL = "https://datosabiertos.dgcp.gob.do/api-dgcp/v1"
    try:
        resp = requests.get(
            f"{API_BASE_URL}/procesos/articulos",
            params={"proceso": codigo_proceso, "limit": 1000},
            timeout=15
        )
        resp.raise_for_status()
        data = resp.json()
        payload = data.get("payload")
        if isinstance(payload, list):
            return payload
        elif isinstance(payload, dict):
            return payload.get("content", []) or []
        return []
    except Exception as e:
        print(f"⚠️  API fallback también falló para {codigo_proceso}: {e}")
        return []


def scraper_articulos_portal(codigo_proceso, url_portal=None):
    """
    Scrapea los artículos UNSPSC directamente del portal transaccional DGCP.
    No depende de la API de datos abiertos (que tiene delay de 8h por ETL).

    Estrategia:
    1. Obtener noticeUID desde la URL guardada en proceso o buscando en el portal
    2. Cargar la página de detalle con &isModal=true&asPopupView=true
    3. Parsear la tabla de artículos con BeautifulSoup

    Estructura HTML del portal:
    - Código UNSPSC: input[type="hidden"] dentro de VortalLookupContainer, value="41116001"
    - Descripción: span[data-prop="Desc"]
    - Cantidad: span[data-prop="Qtd"]
    - Unidad: span.VortalSpan dentro de PriceListLineTableQuantityCell (unidad)
    - Cuenta presupuestaria: span con id que termina en _AccountCode
    - Precio unitario: span[data-prop="PUPrc"] o similar
    - Precio total: span con NMkey BILTtl
    - Cada artículo es un <tr class="PriceListLine Item PriceListLine">
    """
    try:
        from bs4 import BeautifulSoup
        import re as _re
    except ImportError:
        print("⚠️  BeautifulSoup no instalado — pip install beautifulsoup4")
        return []

    PORTAL_BASE = "https://comunidad.comprasdominicana.gob.do"
    HEADERS_PORTAL = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "es-ES,es;q=0.9",
        "Referer": f"{PORTAL_BASE}/Public/Tendering/ContractNoticeManagement/Index",
    }

    # ── Obtener noticeUID ──────────────────────────────────────────────
    notice_uid = None

    # 1. Desde la URL pasada como parámetro
    if url_portal:
        m = _re.search(r"noticeUID=(DO1\.NTC\.[\w\.]+)", url_portal)
        if m:
            notice_uid = m.group(1)

    # 2. Desde Supabase (URL guardada al detectar el proceso)
    if not notice_uid:
        try:
            proc = supabase.table("procesos").select("url").eq("codigo_proceso", codigo_proceso).execute()
            url_db = proc.data[0].get("url", "") if proc.data else ""
            m = _re.search(r"noticeUID=(DO1\.NTC\.[\w\.]+)", url_db)
            if m:
                notice_uid = m.group(1)
        except Exception:
            pass

    if not notice_uid:
        print(f"   ⚠️  [{codigo_proceso}] Sin noticeUID — no se puede scrapear artículos del portal")
        return []

    # ── Cargar página de detalle ───────────────────────────────────────
    session = requests.Session()
    session.verify = False
    try:
        # Establecer cookies de sesión
        session.get(f"{PORTAL_BASE}/Public/Tendering/ContractNoticeManagement/Index",
                    headers=HEADERS_PORTAL, timeout=15)
    except Exception:
        pass

    url_detalle = (f"{PORTAL_BASE}/Public/Tendering/OpportunityDetail/Index"
                   f"?noticeUID={notice_uid}&isModal=true&asPopupView=true")
    try:
        resp = session.get(url_detalle, headers=HEADERS_PORTAL, timeout=25)
        resp.raise_for_status()
        html = resp.text
    except Exception as e:
        print(f"   ⚠️  [{codigo_proceso}] Error cargando detalle: {e}")
        return []

    if "UNSPSC" not in html and "CategoryCode" not in html:
        print(f"   ⚠️  [{codigo_proceso}] HTML sin tabla de artículos")
        return []

    # ── Parsear artículos con BeautifulSoup ───────────────────────────
    soup = BeautifulSoup(html, "html.parser")
    articulos = []

    # Cada artículo es un <tr> con clase "PriceListLine Item PriceListLine"
    filas = soup.find_all("tr", class_=lambda c: c and "PriceListLine" in " ".join(c) and "Item" in " ".join(c))

    for fila in filas:
        try:
            art = {}

            # Código UNSPSC — en input hidden dentro de VortalLookupContainer
            lookup = fila.find("div", class_="VortalLookupContainer") or                      fila.find("table", class_="VortalLookupContainer") or                      fila.find(id=_re.compile(r"CategoryCode$"))
            if lookup:
                inp = lookup.find("input", {"type": "hidden"})
                if inp and inp.get("value"):
                    codigo_unspsc = inp["value"].strip()
                    # Convertir a familia (primeros 8 dígitos), clase (primeros 8), subclase (8 dígitos)
                    # El portal devuelve el código de subclase (8 dígitos)
                    art["subclase_unspsc"] = codigo_unspsc
                    art["clase_unspsc"]    = codigo_unspsc[:6] + "00" if len(codigo_unspsc) >= 6 else None
                    art["familia_unspsc"]  = codigo_unspsc[:4] + "0000" if len(codigo_unspsc) >= 4 else None

                # Texto completo del código (ej: "41116001 - Reactivos analizadores...")
                span_txt = lookup.find("span", class_="EllipsisFullMessage") or                            lookup.find("span", {"title": _re.compile(r"\d{8}")})
                if span_txt:
                    art["descripcion_unspsc"] = span_txt.get_text(strip=True)

            # Descripción del artículo
            desc_span = fila.find("span", {"data-prop": "Desc"})
            if desc_span:
                art["descripcion_articulo"] = desc_span.get_text(strip=True)

            # Cantidad — extraída por índice de celda más abajo

            # Cuenta presupuestaria
            acc_span = fila.find("span", id=_re.compile(r"AccountCode$"))
            if acc_span:
                art["cuenta_presupuestaria"] = acc_span.get_text(strip=True)

            def parse_monto(td):
                """Extrae número: formato portal = '60,002,840.90' (coma=miles, punto=decimal)"""
                if not td:
                    return None
                txt = td.get_text(strip=True).replace("\xa0", "").strip()
                # Eliminar separadores de miles (comas) y dejar punto decimal
                txt = txt.replace(",", "")
                try:
                    return float(txt) if txt else None
                except Exception:
                    return None

            # Estructura confirmada del portal:
            # QuantityCell (168px) = cantidad | QuantityCell (60px) = unidad
            # PriceCell (168px) = precio_unitario (data-prop="ClnP")
            # PriceCell (168px) = precio_total   (data-prop="ClnPT")
            qty_cells = fila.find_all(
                "td", class_=lambda x: x and "PriceListLineTableQuantityCell" in " ".join(x)
            )
            price_cells = fila.find_all(
                "td", class_=lambda x: x and "PriceListLineTablePriceCell" in " ".join(x)
            )

            if len(qty_cells) >= 1:
                art["cantidad"] = parse_monto(qty_cells[0])
            if len(qty_cells) >= 2:
                art["unidad_medida"] = qty_cells[1].get_text(strip=True)
            if len(price_cells) >= 1:
                art["precio_unitario_estimado"] = parse_monto(price_cells[0])
            if len(price_cells) >= 2:
                art["precio_total_estimado"] = parse_monto(price_cells[1])

            # Solo agregar si tiene código UNSPSC
            if art.get("subclase_unspsc"):
                art["codigo_proceso"] = codigo_proceso
                articulos.append(art)

        except Exception as e:
            print(f"   ⚠️  Error parseando fila: {e}")
            continue

    print(f"   📦 [{codigo_proceso}] {len(articulos)} artículos scrapeados del portal (noticeUID={notice_uid})")
    return articulos


def extraer_notice_uid_del_portal(codigo_proceso):
    """
    Hace 1 request al portal para obtener el noticeUID real del proceso.
    Esto permite construir la URL directa al detalle:
      /Public/Tendering/OpportunityDetail/Index?noticeUID=DO1.NTC.XXXXXXX
    Se ejecuta UNA SOLA VEZ cuando el scraper detecta el proceso por primera vez.
    El portal invalida cookies si se hacen demasiados requests — aquí solo
    necesitamos extraer un dato estático del HTML, no descarga de archivos.
    """
    import re as _re
    PORTAL_BASE = "https://comunidad.comprasdominicana.gob.do"

    # URL de búsqueda del proceso en la lista pública
    url_busqueda = (
        f"{PORTAL_BASE}/Public/Tendering/ContractNoticeManagement/Index"
        f"?currentLanguage=es&Country=DO&Theme=DGCP&NoticeReference={codigo_proceso}"
    )

    try:
        resp = requests.get(url_busqueda, headers=HEADERS, timeout=15)
        resp.raise_for_status()
        html = resp.text

        # Buscar el link al detalle que contiene noticeUID
        # Formato: /Public/Tendering/OpportunityDetail/Index?noticeUID=DO1.NTC.XXXXXXX
        match = _re.search(
            r"/Public/Tendering/OpportunityDetail/Index\?noticeUID=(DO1\.NTC\.[\w\.]+)",
            html
        )
        if match:
            notice_uid = match.group(1)
            url_detalle = f"{PORTAL_BASE}/Public/Tendering/OpportunityDetail/Index?noticeUID={notice_uid}"
            print(f"   🔗 [{codigo_proceso}] noticeUID={notice_uid}")
            return url_detalle
        else:
            print(f"   ⚠️  [{codigo_proceso}] noticeUID no encontrado en HTML del portal")
    except Exception as e:
        print(f"   ⚠️  [{codigo_proceso}] Error extrayendo noticeUID: {e}")

    # Fallback: URL de la lista filtrada (menos directa pero funcional)
    return url_busqueda


def guardar_articulos_portal(codigo_proceso, articulos):
    """Guarda los artículos UNSPSC obtenidos en tiempo real."""
    if not articulos:
        return
    registros = [{
        "codigo_proceso": codigo_proceso,
        "familia_unspsc": a.get("familia_unspsc"),
        "clase_unspsc": a.get("clase_unspsc"),
        "subclase_unspsc": a.get("subclase_unspsc"),
        "descripcion_articulo": a.get("descripcion_articulo"),
        "descripcion_usuario": a.get("descripcion_usuario"),
        "cuenta_presupuestaria": a.get("cuenta_presupuestaria"),
        "cantidad": a.get("cantidad"),
        "unidad_medida": a.get("unidad_medida"),
        "precio_unitario_estimado": a.get("precio_unitario_estimado"),
        "precio_total_estimado": a.get("precio_total_estimado"),
    } for a in articulos]
    try:
        for i in range(0, len(registros), 100):
            supabase.table("articulos_proceso").insert(registros[i:i+100]).execute()
    except Exception as e:
        print(f"⚠️  Error guardando artículos de {codigo_proceso}: {e}")


def proceso_coincide_con_intereses(proceso, articulos, intereses_usuario):
    """
    Determina si un proceso debe notificarse a un usuario según sus intereses.
    intereses_usuario es la lista guardada en user_subscriptions.intereses_rubros.

    Soporta dos tipos de filtros:
      - Tipo de objeto: "Obras", "Bienes", "Servicios", "Consultoría"
      - Familia UNSPSC: código numérico de 8 dígitos (ej. "72100000")
    """
    if not intereses_usuario:
        # Sin filtros = quiere todo
        return True

    # Extraer familias UNSPSC del proceso
    familias_proceso = set()
    for a in articulos:
        if a.get("familia_unspsc"):
            familias_proceso.add(str(a["familia_unspsc"]))

    objeto = (proceso.get("objeto_proceso") or proceso.get("titulo") or "").lower()

    for interes in intereses_usuario:
        interes_str = str(interes).strip()

        # Filtro por tipo de objeto (Obras / Bienes / Servicios / Consultoría)
        if interes_str.lower() in ["obras", "bienes", "servicios", "consultoría", "consultoria"]:
            if interes_str.lower() in objeto:
                return True

        # Filtro por familia UNSPSC (coincidencia de prefijo para cubrir clase/subclase)
        elif interes_str.isdigit():
            for familia in familias_proceso:
                if familia.startswith(interes_str[:4]) or interes_str.startswith(familia[:4]):
                    return True

    return False



# ── SIGLAS DE INSTITUCIONES RD ──
SIGLAS_INSTITUCIONES = {
    # Ministerios
    "Ministerio de Obras Públicas y Comunicaciones": "MOPC",
    "Ministerio de Salud Pública y Asistencia Social": "MISPAS",
    "Ministerio de Educación": "MINERD",
    "Ministerio de la Vivienda, Hábitat y Edificaciones": "Min. Vivienda",
    "Ministerio de la Mujer": "Min. Mujer",
    "Ministerio de Medio Ambiente y Recursos Naturales": "Min. Ambiente",
    "Ministerio de Agricultura": "Min. Agricultura",
    "Ministerio de Hacienda": "Min. Hacienda",
    "Ministerio de Interior y Policía": "Min. Interior",
    "Ministerio de Defensa": "Min. Defensa",
    "Ministerio de Industria, Comercio y Mipymes": "MICM",
    "Ministerio de Turismo": "MITUR",
    "Ministerio de Trabajo": "Min. Trabajo",
    "Ministerio de Economía, Planificación y Desarrollo": "MEPyD",
    "Ministerio de Energía y Minas": "MEM",
    "Ministerio de Relaciones Exteriores": "MIREX",
    "Ministerio de Cultura": "Min. Cultura",
    "Ministerio de Deportes": "Min. Deportes",
    "Ministerio de la Presidencia": "Min. Presidencia",
    "Ministerio Administrativo de la Presidencia": "MAP",
    # Agua y saneamiento
    "Corporación del Acueducto y Alcantarillado de Santo Domingo": "CAASD",
    "Corporación del Acueducto y Alcantarillado de Santiago": "CORAASAN",
    "Instituto Nacional de Aguas Potables y Alcantarillados": "INAPA",
    "Corporación de Acueducto y Alcantarillado de La Romana": "CORAROM",
    "Corporación de Acueducto y Alcantarillado de Santiago": "CORAASAN",
    # Energía
    "Empresa Distribuidora de Electricidad del Norte": "EDENORTE",
    "Empresa Distribuidora de Electricidad del Sur": "EDESUR",
    "Empresa Distribuidora de Electricidad del Este": "EDEESTE",
    "Corporación Dominicana de Empresas Eléctricas Estatales": "CDEEE",
    "Empresa de Generación Hidroeléctrica Dominicana": "EGEHID",
    # Salud
    "Servicio Nacional de Salud": "SNS",
    "Sistema Nacional de Atención a Emergencias y Seguridad 9-1-1": "9-1-1",
    "Instituto Dominicano de Seguros Sociales": "IDSS",
    "Instituto Nacional de Bienestar Estudiantil": "INABIE",
    # Educación
    "Instituto Nacional de Formación Técnico Profesional": "INFOTEP",
    "Universidad Autónoma de Santo Domingo": "UASD",
    "Instituto Tecnológico de Santo Domingo": "INTEC",
    # Vivienda y urbanismo
    "Instituto Nacional de la Vivienda": "INVI",
    "Oficina para el Reordenamiento del Transporte": "OPRET",
    # Telecomunicaciones
    "Instituto Dominicano de las Telecomunicaciones": "INDOTEL",
    # Beneficios sociales
    "Sistema Único de Beneficiarios": "SIUBEN",
    "Administradora de Subsidios Sociales": "ADESS",
    "Programa de Medicamentos Esenciales": "PROMESE",
    # Ayuntamientos
    "Ayuntamiento del Distrito Nacional": "ADN",
    "Ayuntamiento Municipal de Santo Domingo": "AMSD",
    "Ayuntamiento Municipal de Santiago": "Ayto. Santiago",
    # Otros frecuentes
    "Dirección General de Contrataciones Públicas": "DGCP",
    "Dirección General de Presupuesto": "DIGEPRES",
    "Dirección General de Impuestos Internos": "DGII",
    "Dirección General de Aduanas": "DGA",
    "Tesorería Nacional": "TN",
    "Contraloría General de la República": "CGR",
    "Procuraduría General de la República": "PGR",
    "Policía Nacional": "PN",
    "Fuerzas Armadas de la República Dominicana": "FFAA",
    "Instituto Dominicano de Aviación Civil": "IDAC",
    "Autoridad Portuaria Dominicana": "APORDOM",
    "Instituto Agrario Dominicano": "IAD",
    "Banco Agrícola de la República Dominicana": "BAGRICOLA",
    "Banco de Reservas de la República Dominicana": "BANRESERVAS",
    "Corporación de Fomento Industrial": "CFI",
    "Centro de Exportación e Inversión de la República Dominicana": "CEI-RD",
    "Consejo Nacional de Zonas Francas de Exportación": "CNZFE",
    "Hospital General": "Hospital General",
    "Hospital Regional": "Hospital Regional",
}

def abreviar_entidad(nombre_completo):
    """
    Devuelve la sigla si la conocemos, si no recorta inteligentemente.
    """
    nombre = nombre_completo.strip()

    # Búsqueda exacta
    if nombre in SIGLAS_INSTITUCIONES:
        return SIGLAS_INSTITUCIONES[nombre]

    # Búsqueda parcial (el nombre puede venir truncado por el portal)
    for nombre_completo_key, sigla in SIGLAS_INSTITUCIONES.items():
        if nombre.lower() in nombre_completo_key.lower() or nombre_completo_key.lower() in nombre.lower():
            return sigla

    # Si no hay sigla, recortar a 22 chars máximo
    if len(nombre) <= 22:
        return nombre
    # Intentar usar solo las palabras principales (quitar artículos)
    palabras = [p for p in nombre.split() if p.lower() not in
                ("de", "del", "la", "el", "los", "las", "y", "e", "o", "a", "en")]
    abrev = " ".join(palabras)
    return abrev[:22] + ("…" if len(abrev) > 22 else "")

def notificar_proceso_inmediato(proceso, articulos):
    """
    Envía notificación push a los usuarios cuyo interés coincida con el proceso.
    Recibe los artículos UNSPSC ya obtenidos para poder filtrar.
    """
    try:
        from notifications import enviar_notificacion

        suscripciones = (
            supabase.table("user_subscriptions")
            .select("*")
            .eq("active", True)
            .execute()
            .data
        )

        if not suscripciones:
            return 0

        entidad_raw = proceso.get("unidad_compra", "")
        entidad = abreviar_entidad(entidad_raw)
        titulo_raw = proceso.get("titulo", "Nueva licitación")
        objeto = (proceso.get("objeto_proceso") or "").strip()
        monto = proceso.get("monto_estimado")
        codigo = proceso.get("codigo_proceso", "")

        # ── URL → siempre a la app, con el código del proceso ──
        APP_URL = os.getenv("APP_URL", "https://web-production-7b940.up.railway.app")
        url_notif = f"{APP_URL}?proceso={codigo}"

        # ── Calcular días al cierre ──
        dias_cierre = None
        fecha_cierre = proceso.get("fecha_fin_recepcion_ofertas")
        if fecha_cierre:
            try:
                from datetime import datetime
                cierre_dt = datetime.fromisoformat(str(fecha_cierre).replace("Z", ""))
                dias_cierre = (cierre_dt - datetime.utcnow()).days
            except Exception:
                pass

        # ── Formatear monto ──
        def fmt_monto(m):
            if not m:
                return None
            m = float(m)
            if m >= 1_000_000:
                return f"RD${m/1_000_000:.1f}M"
            if m >= 1_000:
                return f"RD${m/1_000:.0f}K"
            return f"RD${m:,.0f}"

        monto_fmt = fmt_monto(monto)

        # ── Generar mensajes con personalidad según contexto ──
        import random

        def extraer_tema(titulo_raw):
            """
            Detecta de qué trata el proceso a partir del título.
            Orden de prioridad: lo más específico primero.
            """
            t = titulo_raw.lower()
            temas = [
                # ── Suministros específicos primero (antes que la institución) ──
                (["reactivo", "reactivos"],                                           "reactivos de laboratorio"),
                (["medicamento", "fármaco", "farmaco", "insumo médico"],              "medicamentos"),
                (["combustible", "gasoil", "gasolina"],                               "combustible"),
                (["alimento", "racion", "ración", "comida"],                          "alimentos"),
                (["uniforme", "ropa", "calzado"],                                     "uniformes"),
                (["equipo médico", "equipo medico"],                                  "equipos médicos"),
                (["equipo", "maquinaria"],                                            "equipos/maquinaria"),
                (["vehículo", "vehiculo", "camión", "camion"],                        "vehículos"),
                (["software", "sistema informático", "sistema informatico"],          "software"),
                # ── Infraestructura vial ──
                (["carretera", "autopista", " vial ", "camino", "paviment"],          "carretera"),
                (["puente", "viaducto", "paso elevado"],                              "puente"),
                (["acera", "contén", "anden"],                                        "aceras y contenes"),
                # ── Agua y saneamiento ──
                (["acueducto", "agua potable", "tubería", "tuberia"],                 "acueducto"),
                (["alcantarillado", "saneamiento"],                                   "alcantarillado"),
                (["drenaje", "pluvial", "canal"],                                     "drenaje pluvial"),
                (["planta de tratamiento"],                                           "planta de tratamiento"),
                # ── Edificaciones ──
                (["escuela", "aula", "politécnico", "politecnico", "liceo"],          "escuela"),
                (["hospital", "clínica", "clinica", "centro de salud"],               "hospital/clínica"),
                (["policia", "policía", "destacamento", "cuartel"],                   "destacamento policial"),
                (["mercado"],                                                         "mercado"),
                (["parque", "plaza"],                                                 "parque/plaza"),
                (["vivienda", "residencial", "apartamento", "habitacional"],          "viviendas"),
                (["oficina", "edificio administrativo"],                              "edificio"),
                # ── Servicios ──
                (["consultoría", "consultoria", "supervisión", "supervision"],        "consultoría"),
                (["limpieza", "aseo"],                                                "limpieza"),
                (["seguridad", "vigilancia"],                                         "seguridad"),
                (["mantenimiento"],                                                   "mantenimiento"),
                (["tecnología", "tecnologia"],                                        "tecnología"),
            ]
            for palabras, tema in temas:
                if any(p in t for p in palabras):
                    return tema
            return None

        def descripcion_notificacion(titulo_raw, tema):
            """
            Estrategia: el título siempre da más contexto que el tema.
            - Corto (<=65):  título completo en título y cuerpo
            - Largo (>65):   primeros 62 chars + "..." en título, título completo en cuerpo
            - Tema: solo se usa si el título tiene menos de 4 palabras reales (caso raro)
            """
            titulo_limpio = titulo_raw.strip().title()
            palabras_reales = [p for p in titulo_raw.split() if len(p) > 2]

            if len(palabras_reales) < 4 and tema:
                # Título muy corto/codificado — el tema es más descriptivo
                return tema.capitalize(), titulo_limpio
            elif len(titulo_raw) <= 65:
                return titulo_limpio, titulo_limpio
            else:
                # Título largo: recortar con "..." pero siempre del título real
                return titulo_limpio[:62] + "…", titulo_limpio[:70] + ("…" if len(titulo_raw) > 70 else "")

        def generar_titulo_jocoso(objeto, monto_fmt, entidad, titulo_raw, dias_cierre):
            """
            Solo genera el TÍTULO jocoso de la notificación.
            El cuerpo siempre será el título del proceso — así el usuario
            sabe de qué se trata antes de abrir la app.
            """
            tema = extraer_tema(titulo_raw)
            desc_corta, _ = descripcion_notificacion(titulo_raw, tema)

            # ── URGENCIA ──
            if dias_cierre is not None and dias_cierre <= 3:
                if dias_cierre <= 0:
                    frases = [
                        "🚨 ¡Cierra HOY!",
                        "⏰ ¡Último día!",
                        "🔴 ¡Hoy es el día!",
                    ]
                elif dias_cierre == 1:
                    frases = [
                        "⚡ ¡Mañana cierra!",
                        "🔔 ¡Queda 1 día!",
                        "⏳ ¡Último chance mañana!",
                    ]
                else:
                    frases = [
                        f"⏳ ¡{dias_cierre} días y cierra!",
                        f"🚀 ¡No te confíes! {dias_cierre} días",
                        f"🎯 Quedan {dias_cierre} días — ¿ya empezaste?",
                    ]
                return f"{random.choice(frases)} {desc_corta}"

            # ── OBRAS grandes ──
            if objeto == "Obras" and monto and float(monto) >= 50_000_000:
                frases = [
                    f"🔥 Esta obra puede cambiar tu año:",
                    f"🏆 {monto_fmt} en juego —",
                    f"💰 ¡Proyecto grande!",
                ]
                return f"{random.choice(frases)} {desc_corta}"

            # ── OBRAS normales ──
            if objeto == "Obras":
                frases = [
                    "🔨 ¡Hay trabajo!",
                    "🏗️ Acaba de salir:",
                    "📋 Nueva obra disponible:",
                ]
                return f"{random.choice(frases)} {desc_corta}"

            # ── BIENES ──
            if objeto == "Bienes":
                frases = [
                    "📦 ¡Oportunidad de suministro!",
                    "🛒 Acaba de salir:",
                    "📦 Nueva compra disponible:",
                ]
                return f"{random.choice(frases)} {desc_corta}"

            # ── SERVICIOS ──
            if objeto == "Servicios":
                frases = [
                    "🛠️ ¡Contrato disponible!",
                    "💼 Acaba de publicarse:",
                    "🤝 Nueva oportunidad:",
                ]
                return f"{random.choice(frases)} {desc_corta}"

            # ── GENÉRICO ──
            frases = [
                "⚡ ¡Acaba de salir!",
                "🏛️ Nueva licitación:",
                "📢 ¡No te lo pierdas!",
            ]
            return f"{random.choice(frases)} {desc_corta}"

        enviadas = 0
        omitidas = 0
        for sub in suscripciones:
            intereses = sub.get("intereses_rubros") or []

            if not proceso_coincide_con_intereses(proceso, articulos, intereses):
                omitidas += 1
                continue

            # Título = frase jocosa + nombre del proceso
            # Cuerpo = institución + monto  (complementa, no repite)
            notif_titulo = generar_titulo_jocoso(
                objeto, monto_fmt, entidad, titulo_raw, dias_cierre
            )
            partes_cuerpo = [entidad]
            if monto_fmt:
                partes_cuerpo.append(monto_fmt)
            if dias_cierre is not None and dias_cierre >= 0:
                partes_cuerpo.append(f"Cierre: {dias_cierre}d")
            notif_cuerpo = " · ".join(partes_cuerpo)

            ok = enviar_notificacion(
                subscription_info={
                    "endpoint": sub["endpoint"],
                    "keys": {"auth": sub["auth"], "p256dh": sub["p256dh"]},
                },
                titulo=notif_titulo,
                cuerpo=notif_cuerpo,
                url=url_notif,
            )
            if ok:
                enviadas += 1

        if omitidas:
            print(f"   ↳ {omitidas} suscriptores omitidos (no coincide con sus intereses)")
        return enviadas

    except Exception as e:
        print(f"❌ Error enviando notificación inmediata: {e}")
        return 0


# ============================================
# FUNCIÓN PRINCIPAL DEL SCRAPER
# ============================================

def ejecutar_scraper_portal():
    """
    Ciclo principal:
      1. Raspar portal → detectar procesos nuevos
      2. Para cada nuevo: obtener artículos UNSPSC de la API inmediatamente
      3. Guardar proceso + artículos en Supabase
      4. Filtrar por intereses de cada usuario → notificar solo a quienes corresponde
    """
    t0 = time.time()
    print(f"\n🌐 Scraper Portal | {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    procesos = raspar_portal()
    if not procesos:
        print("📭 Sin procesos en el portal")
        return []

    codigos = [p["codigo_proceso"] for p in procesos]
    existentes = obtener_codigos_existentes_portal(codigos)
    nuevos = [p for p in procesos if p["codigo_proceso"] not in existentes]

    if not nuevos:
        print(f"ℹ️  Sin procesos nuevos (revisados {len(procesos)})")
        registrar_cron_log("scraper_portal", "ok", {
            "procesos_revisados": len(procesos), "procesos_nuevos": 0
        }, int((time.time()-t0)*1000))
        return []

    print(f"🆕 {len(nuevos)} procesos nuevos — obteniendo artículos UNSPSC...")

    total_notificaciones = 0
    for proceso in nuevos:
        codigo = proceso["codigo_proceso"]

        # 1. Guardar proceso básico en Supabase
        guardado = guardar_proceso_basico(proceso)
        if not guardado:
            continue  # Ya existía (race condition), saltar

        # 2. URL con noticeUID ya viene en proceso["url"] desde raspar_portal()
        # No se necesita request extra — el noticeUID se extrajo del HTML de la lista
        notice_uid = proceso.pop("notice_uid", None)
        if notice_uid:
            print(f"   🔗 [{codigo}] noticeUID={notice_uid}")
        else:
            # Si no vino en la lista (raro), loguear para diagnóstico
            print(f"   ⚠️  [{codigo}] noticeUID no encontrado en fila de lista")

        # 3. Obtener artículos UNSPSC directamente del portal (ya tenemos noticeUID)
        # scraper_articulos_portal usa la URL con noticeUID guardada en proceso["url"]
        articulos = obtener_articulos_rapido(codigo, url_portal=proceso.get("url"))
        if articulos:
            guardar_articulos_portal(codigo, articulos)
            # Marcar como enriquecido para no reintentar innecesariamente
            try:
                supabase.table("procesos").update({"enriquecido_api": True}).eq("codigo_proceso", codigo).execute()
            except Exception:
                pass
            print(f"   📦 [{codigo}] {len(articulos)} artículos UNSPSC scrapeados del portal")
        else:
            print(f"   ⏳ [{codigo}] Sin artículos aún — se reintentará en próximo ciclo")

        # 3. Notificar solo a usuarios con intereses coincidentes
        n = notificar_proceso_inmediato(proceso, articulos)
        total_notificaciones += n

        monto = proceso.get("monto_estimado")
        monto_str = f"RD${monto:,.0f}" if monto else "N/A"
        print(f"   🔔 [{codigo}] {proceso['titulo'][:50]}... | {monto_str} → {n} notifs")

        # Pequeña pausa para no saturar la API DGCP
        time.sleep(0.5)

    duracion = int((time.time() - t0) * 1000)
    print(f"✅ Scraper completado: {len(nuevos)} nuevos, {total_notificaciones} notificaciones ({duracion}ms)\n")
    registrar_cron_log("scraper_portal", "ok", {
        "procesos_revisados": len(procesos),
        "procesos_nuevos": len(nuevos),
        "notificaciones_enviadas": total_notificaciones,
    }, duracion)
    return nuevos


if __name__ == "__main__":
    ejecutar_scraper_portal()
