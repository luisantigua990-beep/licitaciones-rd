"""
API de Licitaciones RD — Backend FastAPI
=========================================
- Sirve datos de procesos y artículos a la PWA
- Ejecuta el monitor automáticamente cada 10 minutos
- Análisis de pliegos con IA de Gemini en segundo plano
"""

import os
import threading
import time
import urllib3
import json
import io
from PyPDF2 import PdfReader
from bs4 import BeautifulSoup
import requests
from datetime import datetime
from contextlib import asynccontextmanager
from fastapi.staticfiles import StaticFiles

from fastapi import FastAPI, Query, HTTPException, BackgroundTasks, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from dotenv import load_dotenv
from supabase import create_client
from google import genai

from monitor import ejecutar_monitor
from notifications import enviar_notificacion

# Silenciamos los warnings del SSL de la DGCP
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
SUPABASE_SERVICE_KEY = os.getenv("SUPABASE_SERVICE_KEY", SUPABASE_KEY)

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
# Cliente admin para tablas con RLS estricto (cron_log, analisis_pliego)
supabase_admin = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)

# ============================================
# CONFIGURACIÓN DE INTELIGENCIA ARTIFICIAL
# ============================================
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
cliente_gemini = None

if GEMINI_API_KEY:
    cliente_gemini = genai.Client(api_key=GEMINI_API_KEY)
else:
    print("⚠️ ADVERTENCIA: No se encontró GEMINI_API_KEY en las variables de entorno.")


BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# ============================================
# MONITOR AUTOMÁTICO EN SEGUNDO PLANO
# ============================================

def monitor_loop():
    """Ejecuta el monitor de la API DGCP cada 8 horas (ciclo del Data Warehouse)."""
    INTERVALO_HORAS = 8
    while True:
        try:
            print(f"\n⏰ Ejecutando monitor API DGCP...")
            ejecutar_monitor()
        except Exception as e:
            print(f"❌ Error en monitor API: {e}")

        print(f"💤 Próxima sincronización API en {INTERVALO_HORAS} horas...")
        time.sleep(INTERVALO_HORAS * 3600)


def scraper_loop():
    """
    Scraper del portal transaccional en tiempo real, cada 3 minutos.
    Detecta procesos nuevos inmediatamente, sin esperar el ciclo de 8h de la API.
    """
    INTERVALO_MINUTOS = 3
    # Esperar 30 segundos al inicio para que el monitor API arranque primero
    time.sleep(30)
    while True:
        try:
            from scraper_portal import ejecutar_scraper_portal
            ejecutar_scraper_portal()
        except Exception as e:
            print(f"❌ Error en scraper portal: {e}")

        time.sleep(INTERVALO_MINUTOS * 60)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Inicia el monitor API y el scraper en tiempo real al arrancar."""
    # Monitor API DGCP (cada 8h, enriquece con UNSPSC)
    hilo_monitor = threading.Thread(target=monitor_loop, daemon=True)
    hilo_monitor.start()
    print("✅ Monitor API DGCP iniciado (cada 8 horas)")

    # Scraper portal transaccional (cada 3 min, tiempo real)
    hilo_scraper = threading.Thread(target=scraper_loop, daemon=True)
    hilo_scraper.start()
    print("✅ Scraper portal en tiempo real iniciado (cada 3 minutos)")

    yield
    print("🛑 Servidor detenido")


# ============================================
# CREAR APP FASTAPI
# ============================================

app = FastAPI(
    title="API Licitaciones RD",
    description="Sistema de notificaciones de procesos de compras públicas - República Dominicana",
    version="1.0.0",
    lifespan=lifespan
)

# Permitir conexiones desde cualquier origen (para la PWA)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/api/sync-status")
def sync_status():
    """Estado de sincronización con la API DGCP."""
    from monitor import obtener_estado_sync
    estado = obtener_estado_sync()
    if not estado:
        return {
            "disponible": False,
            "mensaje": "Sin datos de sincronización aún. El monitor aún no ha ejecutado.",
            "ciclo_horas": 8
        }
    return {"disponible": True, **estado}


@app.get("/test")
def test_page():
    return FileResponse(os.path.join(BASE_DIR, "static", "test_push.html"))

@app.get("/sw.js")
def service_worker():
    return FileResponse(os.path.join(BASE_DIR, "static", "sw.js"), media_type="application/javascript")


# ============================================
# ENDPOINTS — PROCESOS
# ============================================

app.mount("/frontend", StaticFiles(directory=os.path.join(BASE_DIR, "frontend"), html=True), name="frontend")

@app.get("/")
def inicio():
    return FileResponse(os.path.join(BASE_DIR, "frontend", "index.html"))


@app.get("/api/procesos")
def listar_procesos(
    page: int = Query(1, ge=1, description="Página"),
    limit: int = Query(20, ge=1, le=100, description="Registros por página"),
    objeto: str = Query(None, description="Filtrar por objeto: Obras, Bienes, Servicios, Consultoría"),
    modalidad: str = Query(None, description="Filtrar por modalidad"),
    unidad_compra: int = Query(None, description="Filtrar por código de unidad de compra"),
    monto_min: float = Query(None, description="Monto mínimo estimado"),
    monto_max: float = Query(None, description="Monto máximo estimado"),
    busqueda: str = Query(None, description="Buscar en título y descripción"),
    solo_activos: bool = Query(True, description="Solo procesos abiertos"),
):
    """Lista procesos con filtros."""
    try:
        query = supabase.table("procesos").select("*", count="exact")
        
        if solo_activos:
            query = query.eq("estado_proceso", "Proceso publicado")
            query = query.gt("fecha_fin_recepcion_ofertas", datetime.now().isoformat())
        
        if objeto:
            query = query.eq("objeto_proceso", objeto)
        if modalidad:
            query = query.eq("modalidad", modalidad)
        if unidad_compra:
            query = query.eq("codigo_unidad_compra", unidad_compra)
        if monto_min:
            query = query.gte("monto_estimado", monto_min)
        if monto_max:
            query = query.lte("monto_estimado", monto_max)
        if busqueda:
            query = query.or_(f"titulo.ilike.%{busqueda}%,descripcion.ilike.%{busqueda}%")
        
        # Paginación
        offset = (page - 1) * limit
        query = query.order("fecha_publicacion", desc=True)
        query = query.range(offset, offset + limit - 1)
        
        result = query.execute()
        
        return {
            "procesos": result.data,
            "total": result.count,
            "page": page,
            "limit": limit,
            "pages": (result.count + limit - 1) // limit if result.count else 0
        }
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/procesos/{codigo_proceso}")
def detalle_proceso(codigo_proceso: str):
    """Obtiene el detalle de un proceso con sus artículos."""
    try:
        proc = supabase.table("procesos") \
            .select("*") \
            .eq("codigo_proceso", codigo_proceso) \
            .execute()
        
        if not proc.data:
            raise HTTPException(status_code=404, detail="Proceso no encontrado")
        
        arts = supabase.table("articulos_proceso") \
            .select("*") \
            .eq("codigo_proceso", codigo_proceso) \
            .execute()
        
        return {
            "proceso": proc.data[0],
            "articulos": arts.data
        }
    
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ============================================
# ENDPOINTS — CATÁLOGO UNSPSC
# ============================================

@app.get("/api/catalogo/segmentos")
def listar_segmentos():
    try:
        result = supabase.rpc("get_segmentos", {}).execute()
        if not result.data:
            result = supabase.table("catalogo_unspsc") \
                .select("segmento, descripcion_segmento") \
                .execute()
            segmentos = {}
            for item in result.data:
                seg = item["segmento"]
                if seg not in segmentos:
                    segmentos[seg] = item["descripcion_segmento"]
            return {"segmentos": [{"codigo": k, "descripcion": v} for k, v in sorted(segmentos.items())]}
        return {"segmentos": result.data}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/catalogo/familias/{segmento}")
def listar_familias(segmento: str):
    try:
        result = supabase.table("catalogo_unspsc") \
            .select("familia, descripcion_familia") \
            .eq("segmento", segmento) \
            .execute()
        familias = {}
        for item in result.data:
            fam = item["familia"]
            if fam not in familias:
                familias[fam] = item["descripcion_familia"]
        return {"familias": [{"codigo": k, "descripcion": v} for k, v in sorted(familias.items())]}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/catalogo/clases/{familia}")
def listar_clases(familia: str):
    try:
        result = supabase.table("catalogo_unspsc") \
            .select("clase, descripcion_clase") \
            .eq("familia", familia) \
            .execute()
        clases = {}
        for item in result.data:
            cls = item["clase"]
            if cls not in clases:
                clases[cls] = item["descripcion_clase"]
        return {"clases": [{"codigo": k, "descripcion": v} for k, v in sorted(clases.items())]}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/unspsc/buscar")
def buscar_unspsc(q: str = ""):
    if not q or len(q) < 2:
        return []
    result = supabase.table("catalogo_unspsc")\
        .select("familia, descripcion_familia")\
        .or_(f"descripcion_familia.ilike.%{q}%,descripcion_subclase.ilike.%{q}%,sinonimos_subclase.ilike.%{q}%")\
        .limit(50)\
        .execute()
    seen = set()
    unique = []
    for r in (result.data or []):
        if r["familia"] not in seen:
            seen.add(r["familia"])
            unique.append(r)
    return unique[:15]


# ============================================
# ENDPOINTS — FILTROS Y MATCHING
# ============================================

@app.get("/api/procesos/por-rubros")
def procesos_por_rubros(
    segmentos: str = Query(None, description="Códigos de segmento separados por coma"),
    familias: str = Query(None, description="Códigos de familia separados por coma"),
    clases: str = Query(None, description="Códigos de clase separados por coma"),
    page: int = Query(1, ge=1),
    limit: int = Query(20, ge=1, le=100),
):
    try:
        art_query = supabase.table("articulos_proceso").select("codigo_proceso")
        
        if clases:
            lista_clases = [c.strip() for c in clases.split(",")]
            art_query = art_query.in_("clase_unspsc", lista_clases)
        elif familias:
            lista_familias = [f.strip() for f in familias.split(",")]
            art_query = art_query.in_("familia_unspsc", lista_familias)
        elif segmentos:
            lista_segmentos = [s.strip() for s in segmentos.split(",")]
            condiciones = [f"familia_unspsc.like.{seg[:2]}%" for seg in lista_segmentos]
            art_query = art_query.or_(",".join(condiciones))
        else:
            raise HTTPException(status_code=400, detail="Debe especificar al menos un filtro de rubro")
        
        art_result = art_query.execute()
        codigos = list(set(a["codigo_proceso"] for a in art_result.data))
        
        if not codigos:
            return {"procesos": [], "total": 0, "page": page, "limit": limit, "pages": 0}
        
        offset = (page - 1) * limit
        proc_query = supabase.table("procesos") \
            .select("*", count="exact") \
            .in_("codigo_proceso", codigos[:200]) \
            .eq("estado_proceso", "Proceso publicado") \
            .gt("fecha_fin_recepcion_ofertas", datetime.now().isoformat()) \
            .order("fecha_publicacion", desc=True) \
            .range(offset, offset + limit - 1)
        
        result = proc_query.execute()
        
        return {
            "procesos": result.data,
            "total": result.count,
            "page": page,
            "limit": limit,
            "pages": (result.count + limit - 1) // limit if result.count else 0
        }
    
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ============================================
# ENDPOINTS — ESTADÍSTICAS
# ============================================

@app.get("/api/stats")
def estadisticas():
    try:
        procesos = supabase.table("procesos").select("id", count="exact").execute()
        activos = supabase.table("procesos") \
            .select("id", count="exact") \
            .eq("estado_proceso", "Proceso publicado") \
            .gt("fecha_fin_recepcion_ofertas", datetime.now().isoformat()) \
            .execute()
        articulos = supabase.table("articulos_proceso").select("id", count="exact").execute()
        
        return {
            "total_procesos": procesos.count,
            "procesos_activos": activos.count,
            "total_articulos": articulos.count,
            "ultima_actualizacion": datetime.now().isoformat()
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ============================================
# ENDPOINTS — NOTIFICACIONES WEB PUSH
# ============================================

@app.get("/api/vapid-public-key")
def get_vapid_public_key():
    return {"publicKey": os.getenv("VAPID_PUBLIC_KEY")}


@app.post("/api/notificaciones/suscribirse")
async def suscribirse(payload: dict):
    try:
        data = {
            "user_id": payload.get("user_id", "anonimo"),
            "endpoint": payload["subscription"]["endpoint"],
            "auth": payload["subscription"]["keys"]["auth"],
            "p256dh": payload["subscription"]["keys"]["p256dh"],
            "intereses_rubros": payload.get("rubros", []),
            "active": True
        }
        supabase.table("user_subscriptions").upsert(data, on_conflict="endpoint").execute()
        return {"ok": True, "message": "Suscripción guardada"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/notificaciones/desuscribirse")
async def desuscribirse(payload: dict):
    endpoint = payload.get("endpoint")
    supabase.table("user_subscriptions").update({"active": False}).eq("endpoint", endpoint).execute()
    return {"ok": True}

@app.post("/api/admin/forzar-monitor")
async def forzar_monitor():
    from monitor import obtener_todas_las_paginas, guardar_procesos_nuevos, procesar_articulos_de_nuevos, notificar_procesos_nuevos
    procesos = obtener_todas_las_paginas(fecha_desde="2026-02-20", fecha_hasta="2026-02-24")
    nuevos = guardar_procesos_nuevos(procesos)
    if nuevos:
        procesar_articulos_de_nuevos(nuevos)
        notificar_procesos_nuevos(nuevos)
    return {"nuevos": len(nuevos)}


@app.put("/api/notificaciones/intereses")
async def actualizar_intereses(payload: dict):
    endpoint = payload.get("endpoint")
    rubros = payload.get("rubros", [])
    supabase.table("user_subscriptions").update({"intereses_rubros": rubros}).eq("endpoint", endpoint).execute()
    return {"ok": True}


@app.post("/api/notificaciones/enviar-prueba")
async def enviar_prueba(payload: dict):
    endpoint = payload.get("endpoint")
    try:
        result = supabase.table("user_subscriptions").select("*").eq("endpoint", endpoint).single().execute()
        sub = result.data
        subscription_info = {
            "endpoint": sub["endpoint"],
            "keys": {"auth": sub["auth"], "p256dh": sub["p256dh"]}
        }
        ok = enviar_notificacion(
            subscription_info,
            titulo="🔔 LicitacionLab Test",
            cuerpo="Notificaciones funcionando correctamente.",
            url="/"
        )
        return {"ok": ok}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/admin/notificar-seguimiento")
async def notificar_seguimiento():
    from notifications import enviar_notificacion
    from datetime import date, timedelta

    ahora   = datetime.now()
    hoy     = date.today()

    seguimientos = supabase.table("seguimiento_procesos") \
        .select("user_id, proceso_codigo, estado") \
        .not_.in_("estado", ["adjudicado", "no-adj"]) \
        .execute().data or []

    if not seguimientos:
        return {"notificaciones_enviadas": 0}

    codigos = list(set(s["proceso_codigo"] for s in seguimientos))
    procesos_map = {}
    for i in range(0, len(codigos), 50):
        bloque = codigos[i:i+50]
        res = supabase.table("procesos") \
            .select("codigo_proceso,titulo,unidad_compra,fecha_fin_recepcion_ofertas,fecha_apertura_ofertas,fecha_estimada_adjudicacion,fecha_enmienda") \
            .in_("codigo_proceso", bloque).execute()
        for p in (res.data or []):
            procesos_map[p["codigo_proceso"]] = p

    user_ids = list(set(s["user_id"] for s in seguimientos))
    subs_map = {}
    for uid in user_ids:
        subs = supabase.table("user_subscriptions") \
            .select("endpoint,auth,p256dh") \
            .eq("user_id", uid).eq("active", True).execute().data or []
        if subs:
            subs_map[uid] = subs

    enviadas = 0
    FECHAS_LABELS = {
        "fecha_enmienda":                 ("❓", "cierre de preguntas"),
        "fecha_fin_recepcion_ofertas":    ("📝", "presentación de ofertas"),
        "fecha_apertura_ofertas":         ("📂", "apertura de sobres"),
        "fecha_estimada_adjudicacion":    ("🏆", "adjudicación estimada"),
    }

    for seg in seguimientos:
        uid    = seg["user_id"]
        codigo = seg["proceso_codigo"]
        p      = procesos_map.get(codigo)
        subs   = subs_map.get(uid, [])

        if not p or not subs:
            continue

        titulo   = (p.get("titulo") or "")[:50]
        entidad  = (p.get("unidad_compra") or "")[:30]

        for campo, (icono, nombre_fecha) in FECHAS_LABELS.items():
            val = p.get(campo)
            if not val:
                continue

            try:
                dt  = datetime.fromisoformat(str(val).replace("Z", ""))
                dia = dt.date()
            except Exception:
                continue

            dias = (dia - hoy).days

            if dias not in [0, 1, 3, 7]:
                continue

            if dias == 0:
                urgencia = f"🚨 ¡HOY es el {nombre_fecha}!"
            elif dias == 1:
                urgencia = f"⏰ ¡Mañana: {nombre_fecha}!"
            elif dias == 3:
                urgencia = f"⚡ En 3 días: {nombre_fecha}"
            else:
                urgencia = f"📅 En 7 días: {nombre_fecha}"

            if campo == "fecha_fin_recepcion_ofertas" and dias <= 3:
                urgencia = urgencia.replace("⚡", "🔴").replace("⏰", "🔴").replace("🚨", "🔴")

            APP_URL = os.getenv("APP_URL", "https://web-production-7b940.up.railway.app")

            for sub in subs:
                ok = enviar_notificacion(
                    subscription_info={"endpoint": sub["endpoint"], "keys": {"auth": sub["auth"], "p256dh": sub["p256dh"]}},
                    titulo=urgencia,
                    cuerpo=f"{titulo} · {entidad}",
                    url=f"{APP_URL}?proceso={codigo}"
                )
                if ok:
                    enviadas += 1

    return {"notificaciones_enviadas": enviadas, "seguimientos_revisados": len(seguimientos)}

@app.get("/api/admin/cron-log")
def ver_cron_log(job: str = None, limit: int = 50):
    query = supabase.table("cron_log").select("*").order("ejecutado_at", desc=True).limit(limit)
    if job:
        query = query.eq("job", job)
    result = query.execute()
    return {"logs": result.data, "total": len(result.data)}


# ═══════════════════════════════════════════════════════════════
# ANÁLISIS DE PLIEGOS CON INTELIGENCIA ARTIFICIAL (GEMINI)
# ═══════════════════════════════════════════════════════════════

PROMPT_MAESTRO = """
Eres un perito experto en licitaciones de República Dominicana (Ley 47-25). 
Analiza el pliego adjunto y extrae los requisitos técnicos, financieros y legales.
Devuelve el resultado ESTRICTAMENTE usando esta estructura JSON:
{
  "alertas_fraude": [{"riesgo": "Alto/Medio", "hallazgo": "..."}],
  "requisitos_experiencia": {"obras_similares": "...", "montos_facturados": "..."},
  "requisitos_financieros": {"indice_liquidez": "...", "indice_endeudamiento": "..."},
  "garantias_exigidas": [{"tipo": "...", "monto_o_porcentaje": "..."}],
  "personal_y_equipos": {"personal_clave": [{"posicion": "...", "titulo": "...", "anos_experiencia": "..."}], "equipos_minimos": ["..."]},
  "checklist_legal": [{"documento": "...", "es_subsanable": true}]
}
"""

def extraer_notice_uid(url_portal: str) -> str:
    """Extrae el noticeUID de la URL del portal."""
    import re
    match = re.search(r'noticeUID=([^&]+)', url_portal)
    return match.group(1) if match else None

def descargar_y_extraer_texto_pdf(url_documentos: str) -> str:
    """
    Descarga el pliego del portal transaccional de DGCP.
    Estrategia multi-patrón:
      1. Obtener cookie de sesión (PublicSessionCookie)
      2. Probar 3 variantes de URL del popup
      3. Buscar documentFileId con 4 patrones de extracción distintos
      4. Descargar PDF y extraer texto
    Si todo falla → lanza excepción para que ejecutar_analisis_gemini marque error.
    """
    import re as _re

    url_limpia = url_documentos.replace("gob.do//", "gob.do/")

    headers_base = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,*/*;q=0.9",
        "Accept-Language": "es-ES,es;q=0.9",
        "sec-fetch-dest": "document",
        "sec-fetch-mode": "navigate",
        "sec-fetch-site": "none",
    }

    headers_popup = {
        **headers_base,
        "Accept": "text/html, */*; q=0.01",
        "Referer": "https://comunidad.comprasdominicana.gob.do/Public/Tendering/ContractNoticeManagement/Index",
        "X-Requested-With": "XMLHttpRequest",
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-origin",
    }

    session = requests.Session()
    session.verify = False

    # ── Paso 1: cookie de sesión ──────────────────────────────
    print("🍪 Obteniendo sesión del portal...")
    try:
        session.get(
            "https://comunidad.comprasdominicana.gob.do/Public/Tendering/ContractNoticeManagement/Index",
            headers=headers_base, timeout=15,
        )
        print(f"🍪 Cookies: {[c.name for c in session.cookies]}")
    except Exception as e:
        print(f"⚠️ Error obteniendo sesión: {e}")

    sep = "&" if "?" in url_limpia else "?"

    # ── Paso 2: probar variantes de URL ──────────────────────
    urls_a_probar = [
        (url_limpia + sep + "isModal=true&asPopupView=true", headers_popup),
        (url_limpia + sep + "sPopupView=true",               headers_popup),
        (url_limpia,                                          headers_base),
    ]

    def extraer_patrones(html: str) -> list:
        """
        Prueba múltiples patrones de regex.
        El portal DGCP genera los links como concatenación JS:
          'documentFileId=' + '12346973' + '&mkey=95ffd475_...'
        por eso los patrones directos no funcionaban.
        """
        regexes = [
            # ✅ Patrón JS concatenado — formato real del portal DGCP
            r"documentFileId='\s*\+\s*'(\d+)'\s*\+\s*'&(?:amp;)?mkey=([\w\-]+)",
            # Patrón URL directa (por si cambian el portal)
            r"documentFileId=(\d+)&(?:amp;)?mkey=([\w\-]+)",
            # Patrón JSON embebido  {"documentFileId":123,"mkey":"abc"}
            r'"documentFileId"\s*:\s*(\d+)[^}]*?"mkey"\s*:\s*"([\w\-]+)"',
            # Patrón data attributes  data-file-id="123" data-mkey="abc"
            r'data-(?:file-id|fileid|documentfileid)=["\'](\d+)["\'][^>]*?data-mkey=["\']([^"\']+)["\']',
        ]
        for regex in regexes:
            matches = _re.findall(regex, html, _re.IGNORECASE)
            if matches:
                print(f"✅ Patrón encontrado ({len(matches)} docs): {regex[:60]}…")
                return matches
        return []

    patrones = []
    html_final = ""
    url_exitosa = ""
    hdrs_exitosos = {}

    for url_intento, hdrs in urls_a_probar:
        print(f"🕵️ Probando: {url_intento[:90]}")
        try:
            resp = session.get(url_intento, headers=hdrs, timeout=20)
            html = resp.text
            print(f"📄 {len(html)} chars, status {resp.status_code}")

            patrones = extraer_patrones(html)
            if patrones:
                html_final    = html
                url_exitosa   = url_intento
                hdrs_exitosos = hdrs
                # ⚠️ IMPORTANTE: NO hacer más requests después de encontrar patrones.
                # El portal genera un mkey único por sesión — requests adicionales
                # invalidan el mkey actual. Descargamos inmediatamente abajo.
                break
            else:
                # Log de diagnóstico si no hay matches
                for kw in ["DownloadFile", "documentFileId", "mkey", "Pliego", "FileId"]:
                    idx = html.find(kw)
                    if idx >= 0:
                        print(f"🔍 '{kw}' pos {idx}: {repr(html[max(0,idx-50):idx+100])}")
        except Exception as e:
            print(f"⚠️ Error en {url_intento[:60]}: {e}")

    if not patrones:
        raise Exception(
            "0 documentos encontrados tras 3 intentos con 4 patrones distintos. "
            "El portal puede requerir Playwright (render JS). "
            f"URL base: {url_limpia}"
        )

    # ── Paso 3: identificar el pliego por contexto ───────────
    BASE_DOWNLOAD = "https://comunidad.comprasdominicana.gob.do/Public/Tendering/OpportunityDetail/DownloadFile"

    # Keywords que indican que NO es el pliego principal
    KEYWORDS_EXCLUIR = [
        "acta de inicio",
        "acta administrativa",
        "acta de aprobacion",
        "acta de aprobación",
        "solicitud de compra",
        "solicitud compra",
        "convocatoria",
        "requerimiento",
        "resolucion de inicio",
        "resolución de inicio",
        "formularios",
        "anexo",
        "dictamen juridico",
        "dictamen jurídico",
        "compromiso etico",
        "compromiso ético",
        "declaracion jurada",
        "declaración jurada",
        "cronograma",
        "diario libre",
        "listin diario",
        "listín diario",
        "publicacion en periodicos",
        "publicación en periódicos",
    ]

    # Grupos de prioridad — se evalúan en orden, el primer grupo con match gana
    # Dentro de cada grupo se toma el primer documento que haga match
    PRIORIDADES = [
        # Prioridad 1: Pliego de Condiciones explícito
        ["pliego de condiciones", "bases de la contratacion", "bases de la contratación"],
        # Prioridad 2: TDR / Términos de Referencia (servicios/consultoría)
        ["terminos de referencia", "términos de referencia", "tdr", "términos de referencia (tdr)"],
        # Prioridad 3: Especificaciones Técnicas (último recurso antes del fallback)
        ["especificaciones tecnicas", "especificaciones técnicas", "ficha tecnica", "ficha técnica"],
        # Prioridad 4: cualquier mención de pliego o condiciones
        ["pliego", "condiciones generales"],
    ]

    def buscar_por_prioridad(patrones, html):
        """
        Para cada grupo de prioridad, escanea TODOS los documentos antes de bajar al siguiente.
        Así 'Pliego de Condiciones' siempre gana sobre 'Especificaciones Técnicas'
        aunque aparezca más abajo en el HTML.
        """
        for grupo in PRIORIDADES:
            for file_id, mkey in patrones:
                idx = html.find(str(file_id))
                contexto = html[max(0, idx - 600):idx + 300].lower()
                if any(x in contexto for x in KEYWORDS_EXCLUIR):
                    continue
                if any(x in contexto for x in grupo):
                    print(f"✅ Pliego identificado (grupo '{grupo[0]}'): documentFileId={file_id}")
                    return file_id, mkey
            # Si ningún doc del grupo hizo match, intentar el siguiente grupo
        return None, None

    file_id_sel, mkey_sel = buscar_por_prioridad(patrones, html_final)

    # Log inventario de documentos encontrados para diagnóstico
    print(f"📋 Inventario ({len(patrones)} docs):")
    for file_id, mkey in patrones:
        idx = html_final.find(str(file_id))
        contexto = html_final[max(0, idx - 800):idx + 100]
        # Buscar nombre del archivo en el contexto (entre comillas o en spans)
        import re as _re_inv
        nombre = _re_inv.search(r'>([\w\s\-\.\_]+\.(?:pdf|zip|xlsx|docx|doc))<', contexto, _re_inv.IGNORECASE)
        if not nombre:
            nombre = _re_inv.search(r'"([\w\s\-\.\_]+\.(?:pdf|zip|xlsx|docx|doc))"', contexto, _re_inv.IGNORECASE)
        nombre_str = nombre.group(1).strip() if nombre else "sin nombre"
        seleccionado = " ← SELECCIONADO" if file_id == file_id_sel else ""
        print(f"   {file_id}: {nombre_str[:70]}{seleccionado}")

    if file_id_sel:
        enlace_pliego = f"{BASE_DOWNLOAD}?documentFileId={file_id_sel}&mkey={mkey_sel}"
    else:
        # Fallback: primer documento que no esté excluido
        enlace_pliego = None
        for file_id, mkey in patrones:
            idx = html_final.find(str(file_id))
            contexto = html_final[max(0, idx - 600):idx + 300].lower()
            if not any(x in contexto for x in KEYWORDS_EXCLUIR):
                enlace_pliego = f"{BASE_DOWNLOAD}?documentFileId={file_id}&mkey={mkey}"
                print(f"⚠️ Sin match por keywords, usando primer doc no excluido: documentFileId={file_id}")
                break

    if not enlace_pliego:
        # Último recurso absoluto
        file_id, mkey = patrones[-1]
        enlace_pliego = f"{BASE_DOWNLOAD}?documentFileId={file_id}&mkey={mkey}"
        print(f"⚠️ Usando último documento como fallback: documentFileId={file_id}")

    # ── Paso 4: descargar PDF con el mkey de la sesión activa ─
    # El portal no devuelve el PDF directo — responde con un redirect JS a:
    # /Public/Archive/RetrieveFile/Index?DocumentId=XXXXX&...
    # Hay que seguir ese redirect manualmente con la misma sesión.
    headers_download = {**hdrs_exitosos, "Referer": url_exitosa}
    print(f"📥 Descargando: {enlace_pliego}")

    resp_pdf = session.get(enlace_pliego, headers=headers_download, timeout=60)
    print(f"📦 Respuesta inicial: {resp_pdf.status_code}, {len(resp_pdf.content)} bytes, Content-Type: {resp_pdf.headers.get('Content-Type', '?')}")

    # Detectar redirect JS y seguirlo
    import re as _re2
    if len(resp_pdf.content) < 2000 and b"window.location.href" in resp_pdf.content:
        js_body = resp_pdf.content.decode("utf-8", errors="ignore")
        match_redirect = _re2.search(r"window\.location\.href\s*=\s*['\"]([^'\"]+)", js_body)
        if match_redirect:
            url_relativa = match_redirect.group(1)
            # Puede venir con o sin dominio
            if url_relativa.startswith("http"):
                url_real = url_relativa
            else:
                url_real = "https://comunidad.comprasdominicana.gob.do" + url_relativa
            print(f"🔀 Redirect JS detectado → {url_real}")
            headers_retrieve = {
                **hdrs_exitosos,
                "Referer": enlace_pliego,
                "sec-fetch-dest": "document",
                "sec-fetch-mode": "navigate",
                "X-Requested-With": "",  # el retrieve no es XHR
            }
            resp_pdf = session.get(url_real, headers=headers_retrieve, timeout=60)
            print(f"📦 Respuesta RetrieveFile: {resp_pdf.status_code}, {len(resp_pdf.content)} bytes, Content-Type: {resp_pdf.headers.get('Content-Type', '?')}")
        else:
            print(f"⚠️ Redirect JS no parseable: {repr(js_body[:300])}")

    if resp_pdf.status_code != 200:
        raise Exception(f"Error HTTP {resp_pdf.status_code} al descargar PDF")
    if len(resp_pdf.content) < 500:
        print(f"⚠️ Respuesta pequeña final: {repr(resp_pdf.content[:300])}")
        raise Exception(f"Respuesta demasiado pequeña ({len(resp_pdf.content)} bytes) tras seguir redirect")

    print(f"📄 PDF: {len(resp_pdf.content):,} bytes. Extrayendo texto...")
    pdf_bytes = resp_pdf.content
    pdf_file = io.BytesIO(pdf_bytes)
    lector_pdf = PdfReader(pdf_file)

    texto_completo = ""
    for i in range(min(len(lector_pdf.pages), 80)):
        texto = lector_pdf.pages[i].extract_text()
        if texto:
            texto_completo += texto + "\n"

    print(f"📝 {len(texto_completo):,} caracteres extraídos del PDF")

    # Si el PDF está escaneado (imágenes), usar los bytes crudos para que
    # Gemini Vision haga el OCR directamente — mucho más preciso
    if len(texto_completo.strip()) < 500:
        print(f"⚠️ PDF escaneado detectado ({len(texto_completo)} chars) — enviando bytes a Gemini Vision")
        import base64 as _b64
        pdf_b64 = _b64.b64encode(pdf_bytes).decode("utf-8")
        # Devolvemos un dict especial que ejecutar_analisis_gemini usará
        return {"__pdf_bytes_b64": pdf_b64, "__texto": texto_completo}

    return texto_completo


def enviar_email_analisis(proceso_id: str, analisis: dict):
    """
    Envía el resumen del análisis de pliego por email al usuario que siguió el proceso.
    Usa Resend como proveedor. Requiere RESEND_API_KEY en Railway.
    """
    RESEND_API_KEY = os.getenv("RESEND_API_KEY")
    if not RESEND_API_KEY:
        print("⚠️ RESEND_API_KEY no configurada — email no enviado")
        return

    try:
        # 1. Buscar el user_id que siguió este proceso
        seg = supabase_admin.table("seguimiento_procesos")\
            .select("user_id")\
            .eq("proceso_codigo", proceso_id)\
            .execute()

        if not seg.data:
            print(f"⚠️ No hay seguimiento para {proceso_id}, no se envía email")
            return

        # Obtener emails de todos los usuarios que siguen este proceso
        user_ids = list(set(s["user_id"] for s in seg.data))
        emails_destino = []
        nombres_destino = []

        for uid in user_ids:
            try:
                user_resp = supabase_admin.auth.admin.get_user(uid)
                email = user_resp.user.email if user_resp.user else None
                nombre = (user_resp.user.user_metadata or {}).get("nombre") or \
                         (user_resp.user.user_metadata or {}).get("full_name") or \
                         email.split("@")[0] if email else "Usuario"
                if email:
                    emails_destino.append(email)
                    nombres_destino.append(nombre)
            except Exception as e:
                print(f"⚠️ No se pudo obtener email de user {uid}: {e}")

        if not emails_destino:
            print(f"⚠️ No se encontraron emails para {proceso_id}")
            return

        # 2. Obtener datos del proceso
        proc = supabase.table("procesos")\
            .select("titulo, unidad_compra, monto_estimado, fecha_fin_recepcion_ofertas, objeto_proceso")\
            .eq("codigo_proceso", proceso_id)\
            .execute()
        proceso = proc.data[0] if proc.data else {}

        # 3. Construir HTML del email
        html_body = construir_html_email(proceso_id, proceso, analisis)

        # 4. Enviar a cada usuario
        APP_URL = os.getenv("APP_URL", "https://web-production-7b940.up.railway.app")
        FROM_EMAIL = os.getenv("RESEND_FROM", "LicitacionLab <notificaciones@licitacionlab.com>")

        for email, nombre in zip(emails_destino, nombres_destino):
            payload = {
                "from": FROM_EMAIL,
                "to": [email],
                "subject": f"📋 Análisis listo: {proceso_id}",
                "html": html_body.replace("{{nombre}}", nombre),
            }
            resp = requests.post(
                "https://api.resend.com/emails",
                headers={
                    "Authorization": f"Bearer {RESEND_API_KEY}",
                    "Content-Type": "application/json",
                },
                json=payload,
                timeout=15,
            )
            if resp.status_code in (200, 201):
                print(f"📧 Email enviado a {email}")
            else:
                print(f"⚠️ Error enviando email a {email}: {resp.status_code} {resp.text[:200]}")

    except Exception as e:
        # El email no debe romper el flujo principal
        print(f"⚠️ Error en envío de email para {proceso_id}: {e}")


def construir_html_email(proceso_id: str, proceso: dict, analisis: dict) -> str:
    """Genera el HTML del email con el resumen del análisis."""
    APP_URL = os.getenv("APP_URL", "https://web-production-7b940.up.railway.app")

    titulo         = proceso.get("titulo", proceso_id)[:80]
    entidad        = proceso.get("unidad_compra", "")
    objeto         = proceso.get("objeto_proceso", "")
    monto          = proceso.get("monto_estimado")
    fecha_cierre   = proceso.get("fecha_fin_recepcion_ofertas", "")
    monto_str      = f"RD$ {monto:,.2f}" if monto else "No especificado"

    if fecha_cierre:
        try:
            fecha_cierre = datetime.fromisoformat(str(fecha_cierre).replace("Z","")).strftime("%d/%m/%Y")
        except Exception:
            pass

    # Alertas de fraude
    alertas        = analisis.get("alertas_fraude") or []
    alertas_html   = ""
    for a in (alertas if isinstance(alertas, list) else []):
        riesgo     = a.get("riesgo", "")
        hallazgo   = a.get("hallazgo", "")
        color      = "#dc2626" if "alto" in riesgo.lower() else "#d97706"
        alertas_html += f"""
        <tr>
          <td style="padding:6px 10px;border-bottom:1px solid #f3f4f6;">
            <span style="background:{color};color:white;padding:2px 8px;border-radius:12px;font-size:11px;font-weight:bold;">{riesgo}</span>
          </td>
          <td style="padding:6px 10px;border-bottom:1px solid #f3f4f6;font-size:13px;color:#374151;">{hallazgo}</td>
        </tr>"""

    # Checklist legal
    checklist      = analisis.get("checklist_legal") or []
    checklist_html = ""
    for item in (checklist if isinstance(checklist, list) else []):
        doc        = item.get("documento", "")
        subsanable = item.get("es_subsanable", True)
        icono      = "⚠️" if not subsanable else "📄"
        color_bg   = "#fef2f2" if not subsanable else "#f9fafb"
        checklist_html += f"""
        <tr style="background:{color_bg};">
          <td style="padding:6px 10px;border-bottom:1px solid #f3f4f6;font-size:13px;">{icono} {doc}</td>
          <td style="padding:6px 10px;border-bottom:1px solid #f3f4f6;font-size:11px;color:#6b7280;">
            {'❌ No subsanable' if not subsanable else '✅ Subsanable'}
          </td>
        </tr>"""

    # Garantías
    garantias      = analisis.get("garantias_exigidas") or []
    garantias_html = ""
    for g in (garantias if isinstance(garantias, list) else []):
        garantias_html += f"<li style='margin:4px 0;font-size:13px;'><strong>{g.get('tipo','')}</strong>: {g.get('monto_o_porcentaje','')}</li>"

    # Requisitos financieros
    req_fin        = analisis.get("requisitos_financieros") or {}
    req_exp        = analisis.get("requisitos_experiencia") or {}

    return f"""<!DOCTYPE html>
<html>
<head><meta charset="utf-8"></head>
<body style="margin:0;padding:0;background:#f3f4f6;font-family:'Helvetica Neue',Arial,sans-serif;">
<table width="100%" cellpadding="0" cellspacing="0" style="background:#f3f4f6;padding:32px 0;">
<tr><td align="center">
<table width="600" cellpadding="0" cellspacing="0" style="background:white;border-radius:12px;overflow:hidden;box-shadow:0 2px 8px rgba(0,0,0,0.08);">

  <!-- Header -->
  <tr>
    <td style="background:linear-gradient(135deg,#1e40af,#3b82f6);padding:28px 32px;">
      <h1 style="margin:0;color:white;font-size:20px;">📋 LicitacionLab</h1>
      <p style="margin:6px 0 0;color:#bfdbfe;font-size:13px;">Análisis de pliego completado</p>
    </td>
  </tr>

  <!-- Saludo -->
  <tr><td style="padding:24px 32px 0;">
    <p style="margin:0;font-size:15px;color:#374151;">Hola <strong>{{{{nombre}}}}</strong>,</p>
    <p style="margin:8px 0 0;font-size:14px;color:#6b7280;">El análisis de IA está listo para el siguiente proceso:</p>
  </td></tr>

  <!-- Info proceso -->
  <tr><td style="padding:16px 32px;">
    <table width="100%" style="background:#f8fafc;border-radius:8px;border:1px solid #e2e8f0;" cellpadding="0" cellspacing="0">
      <tr><td style="padding:16px;">
        <p style="margin:0;font-size:16px;font-weight:bold;color:#1e293b;">{titulo}</p>
        <p style="margin:4px 0 0;font-size:13px;color:#64748b;">{entidad} · {objeto}</p>
        <table style="margin-top:12px;" cellpadding="0" cellspacing="0">
          <tr>
            <td style="padding-right:24px;"><span style="font-size:11px;color:#94a3b8;text-transform:uppercase;">Código</span><br><strong style="font-size:13px;color:#1e293b;">{proceso_id}</strong></td>
            <td style="padding-right:24px;"><span style="font-size:11px;color:#94a3b8;text-transform:uppercase;">Monto estimado</span><br><strong style="font-size:13px;color:#1e293b;">{monto_str}</strong></td>
            <td><span style="font-size:11px;color:#94a3b8;text-transform:uppercase;">Cierre de ofertas</span><br><strong style="font-size:13px;color:#dc2626;">{fecha_cierre}</strong></td>
          </tr>
        </table>
      </td></tr>
    </table>
  </td></tr>

  <!-- Alertas -->
  {'<tr><td style="padding:0 32px 16px;"><h3 style="margin:0 0 8px;font-size:14px;color:#dc2626;">🚨 Alertas de Fraude</h3><table width="100%" style="border:1px solid #fee2e2;border-radius:8px;overflow:hidden;" cellpadding="0" cellspacing="0">' + alertas_html + '</table></td></tr>' if alertas_html else ''}

  <!-- Checklist -->
  {'<tr><td style="padding:0 32px 16px;"><h3 style="margin:0 0 8px;font-size:14px;color:#1e293b;">📑 Documentos requeridos</h3><table width="100%" style="border:1px solid #e5e7eb;border-radius:8px;overflow:hidden;" cellpadding="0" cellspacing="0">' + checklist_html + '</table><p style="margin:6px 0 0;font-size:11px;color:#9ca3af;">⚠️ Los documentos NO subsanables son críticos — su ausencia puede descalificarte.</p></td></tr>' if checklist_html else ''}

  <!-- Garantías -->
  {'<tr><td style="padding:0 32px 16px;"><h3 style="margin:0 0 8px;font-size:14px;color:#1e293b;">🔒 Garantías exigidas</h3><ul style="margin:0;padding-left:20px;background:#f8fafc;border:1px solid #e5e7eb;border-radius:8px;padding:12px 12px 12px 28px;">' + garantias_html + '</ul></td></tr>' if garantias_html else ''}

  <!-- Requisitos -->
  <tr><td style="padding:0 32px 16px;">
    <h3 style="margin:0 0 8px;font-size:14px;color:#1e293b;">📊 Requisitos clave</h3>
    <table width="100%" style="border:1px solid #e5e7eb;border-radius:8px;overflow:hidden;" cellpadding="0" cellspacing="0">
      <tr style="background:#f8fafc;"><td colspan="2" style="padding:8px 12px;font-size:11px;font-weight:bold;color:#6b7280;text-transform:uppercase;">Experiencia</td></tr>
      <tr><td style="padding:6px 12px;font-size:12px;color:#6b7280;width:40%;">Obras similares</td><td style="padding:6px 12px;font-size:13px;color:#374151;">{req_exp.get('obras_similares','No especificado')}</td></tr>
      <tr style="background:#f8fafc;"><td style="padding:6px 12px;font-size:12px;color:#6b7280;">Montos facturados</td><td style="padding:6px 12px;font-size:13px;color:#374151;">{req_exp.get('montos_facturados','No especificado')}</td></tr>
      <tr><td colspan="2" style="padding:8px 12px;font-size:11px;font-weight:bold;color:#6b7280;text-transform:uppercase;background:#f8fafc;">Financiero</td></tr>
      <tr><td style="padding:6px 12px;font-size:12px;color:#6b7280;">Índice de liquidez</td><td style="padding:6px 12px;font-size:13px;color:#374151;">{req_fin.get('indice_liquidez','No especificado')}</td></tr>
      <tr style="background:#f8fafc;"><td style="padding:6px 12px;font-size:12px;color:#6b7280;">Índice de endeudamiento</td><td style="padding:6px 12px;font-size:13px;color:#374151;">{req_fin.get('indice_endeudamiento','No especificado')}</td></tr>
    </table>
  </td></tr>

  <!-- CTA -->
  <tr><td style="padding:8px 32px 32px;" align="center">
    <a href="{APP_URL}?proceso={proceso_id}" style="display:inline-block;background:#1e40af;color:white;text-decoration:none;padding:12px 28px;border-radius:8px;font-size:14px;font-weight:bold;">
      Ver análisis completo en LicitacionLab →
    </a>
  </td></tr>

  <!-- Footer -->
  <tr>
    <td style="background:#f8fafc;padding:16px 32px;border-top:1px solid #e5e7eb;">
      <p style="margin:0;font-size:11px;color:#9ca3af;text-align:center;">
        LicitacionLab · Sistema de monitoreo de licitaciones públicas RD<br>
        Este análisis es generado por IA y debe ser verificado por un profesional.
      </p>
    </td>
  </tr>

</table>
</td></tr>
</table>
</body>
</html>"""


def ejecutar_analisis_gemini(proceso_id: str):
    """Descarga el pliego, lo lee y guarda el JSON en Supabase en segundo plano"""
    try:
        print(f"🚀 Iniciando IA para: {proceso_id}")

        # ── CACHE: si ya está completado, no reprocesar ──────
        existente = supabase_admin.table("analisis_pliego")\
            .select("estado, alertas_fraude, requisitos_experiencia, requisitos_financieros, garantias_exigidas, personal_y_equipos, checklist_legal")\
            .eq("proceso_id", proceso_id)\
            .eq("estado", "completado")\
            .execute()
        if existente.data:
            print(f"⚡ Cache hit — {proceso_id} ya analizado, enviando email igual.")
            analisis_cacheado = existente.data[0]
            enviar_email_analisis(proceso_id, analisis_cacheado)
            return
        
        # 1. ACTUALIZAR ESTADO EN SUPABASE
        supabase_admin.table("analisis_pliego").upsert({
            "proceso_id": proceso_id, 
            "estado": "procesando"
        }).execute()

        # 2. SCRAPING Y EXTRACCIÓN DEL PDF
        proc_data = supabase.table("procesos").select("url").eq("codigo_proceso", proceso_id).execute()
        
        if not proc_data.data or not proc_data.data[0].get("url"):
            raise Exception(f"No se encontró la URL del portal en la base de datos para {proceso_id}")
            
        url_portal = proc_data.data[0]["url"]
        
        # Llamamos al brazo robótico mejorado
        resultado_pdf = descargar_y_extraer_texto_pdf(url_portal)

        # Detectar si el PDF estaba escaneado (Gemini Vision lo procesará)
        if isinstance(resultado_pdf, dict) and "__pdf_bytes_b64" in resultado_pdf:
            import base64 as _b64
            pdf_bytes = _b64.b64decode(resultado_pdf["__pdf_bytes_b64"])
            texto_pliego = resultado_pdf["__texto"]
            usar_vision = True
            print(f"🔬 Modo Vision activado — PDF escaneado, {len(pdf_bytes):,} bytes")
        else:
            texto_pliego = resultado_pdf
            usar_vision = False
            if len(texto_pliego.strip()) < 100:
                raise Exception("El PDF parece estar vacío o es un documento escaneado sin texto.")
            if len(texto_pliego.strip()) < 500:
                print(f"⚠️ PDF con poco texto ({len(texto_pliego)} chars) — Gemini intentará igual.")

        # 3. LLAMADA A GEMINI CON RETRY AUTOMÁTICO
        if not cliente_gemini:
            raise Exception("No se configuró Gemini. Revisa tu GEMINI_API_KEY en Railway.")

        print("🤖 Pasando el texto del pliego a Gemini...")

        MODELO = "gemini-2.5-flash"
        MAX_REINTENTOS = 3
        datos_json = None
        for intento in range(MAX_REINTENTOS):
            try:
                if usar_vision:
                    # PDF escaneado: enviar bytes crudos para OCR nativo de Gemini
                    contents = [
                        PROMPT_MAESTRO,
                        {"inline_data": {"mime_type": "application/pdf", "data": _b64.b64encode(pdf_bytes).decode()}}
                    ]
                else:
                    contents = [PROMPT_MAESTRO, texto_pliego]

                respuesta = cliente_gemini.models.generate_content(
                    model=MODELO,
                    contents=contents
                )
                # Log para diagnóstico — ver qué devuelve Gemini exactamente
                texto_raw = respuesta.text if respuesta.text else ""
                print(f"🧠 Gemini raw ({len(texto_raw)} chars): {repr(texto_raw[:300])}")

                texto_respuesta = texto_raw.strip()
                if not texto_respuesta:
                    raise Exception("Gemini devolvió respuesta vacía")

                # Limpiar bloques ```json ... ```
                if "```" in texto_respuesta:
                    partes = texto_respuesta.split("```")
                    for parte in partes:
                        if parte.startswith("json"):
                            texto_respuesta = parte[4:].strip()
                            break
                        elif parte.strip().startswith("{"):
                            texto_respuesta = parte.strip()
                            break

                # Extraer solo el bloque JSON {...} ignorando texto antes y después
                idx_inicio = texto_respuesta.find("{")
                idx_fin    = texto_respuesta.rfind("}")
                if idx_inicio == -1 or idx_fin == -1:
                    raise Exception(f"No se encontró JSON válido en la respuesta de Gemini: {repr(texto_respuesta[:200])}")
                if idx_inicio > 0:
                    print(f"⚠️ Recortando {idx_inicio} chars antes del JSON")
                texto_respuesta = texto_respuesta[idx_inicio:idx_fin + 1]
                datos_json = json.loads(texto_respuesta)
                break  # éxito
            except Exception as e:
                msg = str(e)
                if "429" in msg or "RESOURCE_EXHAUSTED" in msg:
                    import re as _re3
                    m = _re3.search(r"retry[^\d]*(\d+)", msg, _re3.IGNORECASE)
                    espera = int(m.group(1)) + 2 if m else 15
                    espera = min(espera, 15)
                    print(f"⏳ Gemini 429 (burst) — esperando {espera}s ({intento+1}/{MAX_REINTENTOS})...")
                    time.sleep(espera)
                else:
                    raise  # error no recuperable

        if datos_json is None:
            raise Exception(f"Gemini 429 persistente tras {MAX_REINTENTOS} reintentos — revisa cuota en Google AI Studio.")
        
        # 4. GUARDAR RESULTADOS EN LA BÓVEDA
        supabase_admin.table("analisis_pliego").upsert({
            "proceso_id": proceso_id,
            "estado": "completado",
            **datos_json
        }).execute()
        
        print(f"✅ Análisis de {proceso_id} completado y guardado.")

        # 5. ENVIAR EMAIL AL USUARIO
        enviar_email_analisis(proceso_id, datos_json)

    except Exception as e:
        print(f"❌ Error de IA en {proceso_id}: {str(e)}")
        supabase_admin.table("analisis_pliego").upsert({
            "proceso_id": proceso_id,
            "estado": "error"
        }).execute()

@app.get("/api/admin/test-pliego")
def test_pliego(codigo: str = Query(..., description="Ej: DO1.NTC.1234567")):
    """
    Diagnóstico del scraper de pliegos.
    Ejecuta el flujo completo y devuelve un reporte detallado del HTML recibido
    para identificar el formato exacto de los links de descarga.

    Ejemplo: GET /api/admin/test-pliego?codigo=DO1.NTC.1234567
    """
    import re as _re

    # 1. Buscar URL del proceso en Supabase
    proc = supabase.table("procesos").select("url, titulo").eq("codigo_proceso", codigo).execute()
    if not proc.data or not proc.data[0].get("url"):
        raise HTTPException(status_code=404, detail=f"Proceso '{codigo}' no encontrado o sin URL")

    url_portal = proc.data[0]["url"]
    titulo     = proc.data[0].get("titulo", "Sin título")

    headers_base = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,*/*;q=0.9",
        "Accept-Language": "es-ES,es;q=0.9",
        "sec-fetch-dest": "document",
        "sec-fetch-mode": "navigate",
        "sec-fetch-site": "none",
    }

    headers_popup = {
        **headers_base,
        "Accept": "text/html, */*; q=0.01",
        "Referer": "https://comunidad.comprasdominicana.gob.do/Public/Tendering/ContractNoticeManagement/Index",
        "X-Requested-With": "XMLHttpRequest",
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-origin",
    }

    session = requests.Session()
    session.verify = False

    diag = {
        "codigo": codigo,
        "titulo": titulo,
        "url_portal": url_portal,
        "cookies_obtenidas": [],
        "intentos": [],
    }

    # Obtener cookie
    try:
        session.get(
            "https://comunidad.comprasdominicana.gob.do/Public/Tendering/ContractNoticeManagement/Index",
            headers=headers_base, timeout=15,
        )
        diag["cookies_obtenidas"] = [c.name for c in session.cookies]
    except Exception as e:
        diag["error_cookie"] = str(e)

    url_limpia = url_portal.replace("gob.do//", "gob.do/")
    sep = "&" if "?" in url_limpia else "?"

    urls_a_probar = [
        (url_limpia + sep + "isModal=true&asPopupView=true", headers_popup),
        (url_limpia + sep + "sPopupView=true",               headers_popup),
        (url_limpia,                                          headers_base),
    ]

    regexes = {
        "patron_js_concatenado":  r"documentFileId='\s*\+\s*'(\d+)'\s*\+\s*'&(?:amp;)?mkey=([\w\-]+)",
        "patron_url_directa":     r"documentFileId=(\d+)&(?:amp;)?mkey=([\w\-]+)",
        "patron_json_embebido":   r'"documentFileId"\s*:\s*(\d+)[^}]*?"mkey"\s*:\s*"([\w\-]+)"',
        "patron_data_attr":       r'data-(?:file-id|fileid|documentfileid)=["\'](\d+)["\'][^>]*?data-mkey=["\']([^"\']+)["\']',
    }

    for url_intento, hdrs in urls_a_probar:
        intento: dict = {"url": url_intento}
        try:
            resp = session.get(url_intento, headers=hdrs, timeout=20)
            html = resp.text
            intento["status"]     = resp.status_code
            intento["html_chars"] = len(html)

            # Buscar con cada patrón
            matches_por_patron = {}
            for nombre, regex in regexes.items():
                m = _re.findall(regex, html, _re.IGNORECASE)
                matches_por_patron[nombre] = {"cantidad": len(m), "primeros": m[:3]}
            intento["patrones"] = matches_por_patron

            # Keywords de diagnóstico
            keywords = {}
            for kw in ["DownloadFile", "documentFileId", "mkey", "Pliego", "pliego",
                       "FileId", "fileId", "download", "application/pdf", "adjunto"]:
                idx = html.find(kw)
                if idx >= 0:
                    keywords[kw] = repr(html[max(0, idx-60):idx+120])
            intento["keywords_encontradas"] = keywords

            # Muestras del HTML para análisis manual
            intento["html_inicio"]    = html[:800]
            intento["html_zona_4000"] = html[4000:4600]
            intento["html_final"]     = html[-400:]

            total_matches = sum(v["cantidad"] for v in matches_por_patron.values())
            intento["exito"] = total_matches > 0

        except Exception as e:
            intento["error"] = str(e)

        diag["intentos"].append(intento)
        if intento.get("exito"):
            break

    total_global = sum(
        sum(p["cantidad"] for p in i.get("patrones", {}).values())
        for i in diag["intentos"]
    )
    diag["conclusion"] = (
        "✅ Documentos encontrados — el scraper debería funcionar"
        if total_global > 0
        else "❌ 0 documentos en todos los intentos — portal usa render JS puro, necesita Playwright"
    )

    return diag


@app.post("/api/webhook/analizar-pliego")
async def webhook_analisis_pliego(request: Request, background_tasks: BackgroundTasks):
    """Endpoint llamado por Supabase (Webhook) al insertar un nuevo seguimiento"""
    payload = await request.json()
    
    registro = payload.get("record", {})
    proceso_id = registro.get("proceso_codigo")
    
    if proceso_id:
        # Enviamos la tarea al fondo para no bloquear el sistema
        background_tasks.add_task(ejecutar_analisis_gemini, proceso_id)
        return {"status": "success", "message": f"IA encolada para {proceso_id}"}
    
    return {"status": "error", "message": "Payload inválido o sin proceso_codigo"}
