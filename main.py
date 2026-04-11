"""
API de Licitaciones RD — Backend FastAPI
=========================================
- Sirve datos de procesos y artículos a la PWA
- Ejecuta el monitor automáticamente cada 10 minutos
- Análisis de pliegos con IA de Gemini en segundo plano
"""

import os
import asyncio
import threading
import time
import urllib3
import json
import io
from pypdf import PdfReader
from bs4 import BeautifulSoup
import requests
from datetime import datetime, timedelta
from contextlib import asynccontextmanager
from fastapi.staticfiles import StaticFiles


from fastapi import FastAPI, Query, HTTPException, BackgroundTasks, Request, Depends, Header
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import FileResponse, StreamingResponse
from dotenv import load_dotenv
from supabase import create_client
from google import genai

from monitor import ejecutar_monitor
from notifications import enviar_notificacion
from router_agentes import agentes_router, social_router
from router_closer import closer_router

# reportlab se importa dentro de _generar_pdf_analisis_bytes (lazy import)

def enviar_push_y_limpiar(sub: dict, titulo: str, cuerpo: str, url: str = "/") -> bool:
    """Envía notificación y desactiva la suscripción si devuelve 410 (expirada)."""
    resultado = enviar_notificacion(
        subscription_info={"endpoint": sub["endpoint"], "keys": {"auth": sub["auth"], "p256dh": sub["p256dh"]}},
        titulo=titulo, cuerpo=cuerpo, url=url
    )
    if resultado == "410":
        try:
            supabase_admin.table("user_subscriptions") \
                .update({"active": False}) \
                .eq("endpoint", sub["endpoint"]).execute()
            print(f"🧹 Suscripción 410 desactivada: {sub['endpoint'][:50]}...")
        except Exception as e:
            print(f"⚠️ No se pudo desactivar suscripción 410: {e}")
        return False
    return resultado is True

# --- AGREGADOS PARA INTEGRACIÓN CON N8N ---
from pydantic import BaseModel
from typing import List
# ------------------------------------------

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
# PROTECCIÓN ENDPOINTS ADMIN
# ============================================
ADMIN_SECRET = os.getenv("ADMIN_SECRET", "")

def verificar_admin(x_admin_key: str = Header(None)):
    """
    Dependencia que protege todos los endpoints /api/admin/*.
    Requiere el header: X-Admin-Key: <ADMIN_SECRET>
    Configura ADMIN_SECRET en Railway como variable de entorno.
    """
    if not ADMIN_SECRET:
        raise HTTPException(status_code=500, detail="ADMIN_SECRET no configurado en Railway")
    if x_admin_key != ADMIN_SECRET:
        raise HTTPException(status_code=403, detail="Acceso denegado")


# ============================================
# CONFIGURACIÓN DE INTELIGENCIA ARTIFICIAL
# ============================================
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
cliente_gemini = None

# Semáforo: máximo 5 análisis de pliego simultáneos.
# Evita que 50 usuarios pidan análisis al mismo tiempo y saturen RAM + Gemini.
_semaforo_analisis = asyncio.Semaphore(5)

# ── CACHÉ EN MEMORIA — endpoint /api/procesos ──────────────────
# Reduce queries a Supabase cuando múltiples usuarios navegan simultáneamente.
# TTL de 3 minutos — coincide con el ciclo del scraper.
import hashlib as _hashlib
from concurrent.futures import ThreadPoolExecutor as _ThreadPoolExecutor
_cache_procesos: dict = {}          # {cache_key: {"data": ..., "ts": timestamp}}
_CACHE_TTL = 180                    # segundos
# Threadpool con límite de 3 hilos para análisis Gemini — protege la RAM
_executor_gemini = _ThreadPoolExecutor(max_workers=3, thread_name_prefix="gemini")

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
    TIMEOUT_SCRAPER   = 120  # máximo 2 minutos por ejecución
    time.sleep(30)
    while True:
        try:
            from scraper_portal import ejecutar_scraper_portal
            import threading
            resultado = [None]
            error     = [None]
            def _run():
                try:
                    resultado[0] = ejecutar_scraper_portal()
                except Exception as e:
                    error[0] = e
            hilo = threading.Thread(target=_run, daemon=True)
            hilo.start()
            hilo.join(timeout=TIMEOUT_SCRAPER)
            if hilo.is_alive():
                print(f"⚠️ Scraper portal timeout ({TIMEOUT_SCRAPER}s) — saltando ciclo")
            elif error[0]:
                print(f"❌ Error en scraper portal: {error[0]}")
        except Exception as e:
            print(f"❌ Error en scraper portal: {e}")

        time.sleep(INTERVALO_MINUTOS * 60)


def ejecutar_analisis_gemini_sin_email(proceso_id: str):
    """
    Variante del análisis Gemini para uso interno (reproceso automático).
    NO envía email al completar — el email solo se manda cuando el usuario
    lo solicita manualmente desde la app.
    """
    try:
        print(f"🔄 Reprocesando (sin email): {proceso_id}")
        # Si ya está completado, no hacer nada — no reenviar email
        existente = supabase_admin.table("analisis_pliego")\
            .select("estado")\
            .eq("proceso_id", proceso_id)\
            .eq("estado", "completado")\
            .execute()
        if existente.data:
            print(f"⚡ {proceso_id} ya completado, saltando reproceso.")
            return
        # Correr el análisis completo pero sin enviar email al final
        ejecutar_analisis_gemini(proceso_id, enviar_email=False)
    except Exception as e:
        print(f"❌ Error en reproceso sin email {proceso_id}: {e}")


def reprocesar_pendientes_loop():
    """
    Cada 30 minutos reintenta los procesos marcados como pendiente_analisis.
    Ocurre cuando Gemini estuvo con 503 o el PDF era muy grande.
    También rescata registros 'procesando' con más de 2h de antigüedad
    (huérfanos por reinicios de Railway).
    NO envía emails — solo completa el análisis en BD.
    """
    time.sleep(180)  # esperar 3 min al arranque
    while True:
        try:
            # ── RESCATE DE HUÉRFANOS ──────────────────────────────────────
            # Si un análisis lleva más de 2h en 'procesando', Railway lo reinició
            # y nunca terminó. Lo reseteamos a pendiente_analisis para reintentarlo.
            from datetime import timezone
            hace_2h = (datetime.now(timezone.utc) - timedelta(hours=2)).isoformat()
            huerfanos = supabase_admin.table("analisis_pliego") \
                .select("proceso_id") \
                .eq("estado", "procesando") \
                .lt("actualizado_en", hace_2h) \
                .execute()
            if huerfanos.data:
                ids_huerfanos = [r["proceso_id"] for r in huerfanos.data]
                print(f"🚑 Rescatando {len(ids_huerfanos)} análisis huérfanos (procesando > 2h)...")
                supabase_admin.table("analisis_pliego") \
                    .update({"estado": "pendiente_analisis", "actualizado_en": datetime.now(timezone.utc).isoformat()}) \
                    .in_("proceso_id", ids_huerfanos) \
                    .execute()

            # ── REPROCESO DE PENDIENTES ───────────────────────────────────
            pendientes = supabase_admin.table("analisis_pliego") \
                .select("proceso_id") \
                .eq("estado", "pendiente_analisis") \
                .limit(5) \
                .execute()
            if pendientes.data:
                print(f"🔄 Reprocesando {len(pendientes.data)} pliego(s) pendientes...")
                for row in pendientes.data:
                    pid = row["proceso_id"]
                    try:
                        print(f"🔄 Reintentando análisis: {pid}")
                        ejecutar_analisis_gemini_sin_email(pid)
                    except Exception as e:
                        print(f"❌ Error reprocesando {pid}: {e}")
                    time.sleep(10)
        except Exception as e:
            print(f"❌ Error en loop reprocesar_pendientes: {e}")
        time.sleep(30 * 60)  # 30 minutos


def nurturing_loop():
    """Ejecuta la secuencia de nurturing de emails una vez al día."""
    time.sleep(120)
    while True:
        try:
            inicio = time.time()
            ejecutar_nurturing()
            print(f"✅ Nurturing completado en {time.time()-inicio:.1f}s")
        except Exception as e:
            print(f"❌ Error en nurturing: {e}")
        time.sleep(24 * 3600)


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

    # Nurturing de emails (una vez al día)
    hilo_nurturing = threading.Thread(target=nurturing_loop, daemon=True)
    hilo_nurturing.start()
    print("✅ Nurturing de emails iniciado (cada 24 horas)")

    # Reproceso automático de pliegos pendiente_analisis (cada 30 min)
    hilo_pendientes = threading.Thread(target=reprocesar_pendientes_loop, daemon=True)
    hilo_pendientes.start()
    print("✅ Reproceso de pliegos pendientes iniciado (cada 30 minutos)")

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
# Comprimir respuestas > 1KB — reduce tráfico de red 60-80% en listas de procesos
app.add_middleware(GZipMiddleware, minimum_size=1000)

# ── Agentes de growth marketing ──────────────────────────
app.include_router(agentes_router)
app.include_router(social_router)
app.include_router(closer_router)


@app.get("/health")
def health_check():
    """Health check para Railway — verifica que los hilos críticos estén vivos."""
    hilos_vivos = {t.name: t.is_alive() for t in threading.enumerate()}
    return {
        "status": "ok",
        "timestamp": datetime.utcnow().isoformat(),
        "threads": len(threading.enumerate()),
    }


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
    institucion: str = Query(None, description="Filtrar por nombre de institución"),
    familia_unspsc: str = Query(None, description="Filtrar por código de familia UNSPSC"),
):
    """Lista procesos con filtros."""
    try:
        # ── Caché: generar clave única por combinación de parámetros ──
        cache_key = _hashlib.md5(
            f"{page}{limit}{objeto}{modalidad}{unidad_compra}{monto_min}{monto_max}{busqueda}{solo_activos}{institucion}{familia_unspsc}".encode()
        ).hexdigest()
        ahora = time.time()
        if cache_key in _cache_procesos:
            entrada = _cache_procesos[cache_key]
            if ahora - entrada["ts"] < _CACHE_TTL:
                return entrada["data"]
            else:
                del _cache_procesos[cache_key]
        # Si hay filtro UNSPSC, primero obtener los códigos de proceso que coinciden
        codigos_unspsc = None
        if familia_unspsc:
            try:
                fam = familia_unspsc.strip()
                # Búsqueda exacta si tiene 8 dígitos (código completo de familia)
                # Búsqueda por prefijo si tiene menos (ej. "72" → todos los 72xxxxxx)
                if len(fam) >= 8:
                    art_result = supabase.table("articulos_proceso") \
                        .select("codigo_proceso") \
                        .eq("familia_unspsc", fam) \
                        .execute()
                else:
                    art_result = supabase.table("articulos_proceso") \
                        .select("codigo_proceso") \
                        .or_(f"familia_unspsc.like.{fam}%,clase_unspsc.like.{fam}%,subclase_unspsc.like.{fam}%") \
                        .execute()
                codigos_unspsc = list(set(a["codigo_proceso"] for a in art_result.data))
                if not codigos_unspsc:
                    return {"procesos": [], "total": 0, "page": page, "limit": limit, "pages": 0}
            except Exception as e:
                print(f"⚠️ Error filtrando por UNSPSC: {e}")

        query = supabase.table("procesos").select("*", count="exact")

        # Aplicar filtro UNSPSC si corresponde
        if codigos_unspsc is not None:
            # Supabase .in_() tiene límite de ~100 items — paginar si hace falta
            query = query.in_("codigo_proceso", codigos_unspsc[:500])

        import re as _re_busq

        # Detectar si la búsqueda es un código de proceso o código UNSPSC numérico
        es_codigo_proceso = busqueda and (
            bool(_re_busq.search(r'\d{4}', busqueda)) and '-' in busqueda
            or (len(busqueda) > 10 and ' ' not in busqueda.strip())
        )
        es_codigo_unspsc = busqueda and bool(_re_busq.match(r'^\d+$', busqueda.strip()))

        # Si la búsqueda es numérica, buscar también como código UNSPSC
        codigos_por_busqueda_unspsc = None
        if es_codigo_unspsc:
            try:
                fam = busqueda.strip()
                art_result = supabase.table("articulos_proceso") \
                    .select("codigo_proceso") \
                    .or_(f"familia_unspsc.like.{fam}%,clase_unspsc.like.{fam}%,subclase_unspsc.like.{fam}%") \
                    .execute()
                codigos_por_busqueda_unspsc = list(set(a["codigo_proceso"] for a in art_result.data))
                print(f"🔍 Búsqueda UNSPSC '{fam}': {len(codigos_por_busqueda_unspsc)} procesos encontrados")
            except Exception as e:
                print(f"⚠️ Error buscando UNSPSC desde búsqueda general: {e}")

        if solo_activos:
            # Si es código de proceso específico, no filtrar por activos
            if not es_codigo_proceso:
                query = query.eq("estado_proceso", "Proceso publicado")
                query = query.gt("fecha_fin_recepcion_ofertas", datetime.now().isoformat())

        if objeto:
            query = query.ilike("objeto_proceso", f"%{objeto}%")
        if modalidad:
            query = query.eq("modalidad", modalidad)
        if unidad_compra:
            query = query.eq("codigo_unidad_compra", unidad_compra)
        if monto_min:
            query = query.gte("monto_estimado", monto_min)
        if monto_max:
            query = query.lte("monto_estimado", monto_max)
        if busqueda:
            if codigos_por_busqueda_unspsc is not None:
                # Búsqueda numérica → filtrar por códigos UNSPSC encontrados
                if not codigos_por_busqueda_unspsc:
                    return {"procesos": [], "total": 0, "page": page, "limit": limit, "pages": 0}
                # Combinar con codigos_unspsc si también hay filtro UNSPSC activo
                codigos_finales = codigos_por_busqueda_unspsc
                if codigos_unspsc is not None:
                    codigos_finales = list(set(codigos_por_busqueda_unspsc) & set(codigos_unspsc))
                query = query.in_("codigo_proceso", codigos_finales[:500])
            else:
                # Búsqueda de texto → titulo, descripcion, codigo_proceso
                query = query.or_(f"titulo.ilike.%{busqueda}%,descripcion.ilike.%{busqueda}%,codigo_proceso.ilike.%{busqueda}%")

        if institucion:
            query = query.ilike("unidad_compra", f"%{institucion}%")

        # Paginación
        offset = (page - 1) * limit
        query = query.order("fecha_publicacion", desc=True)
        query = query.range(offset, offset + limit - 1)

        result = query.execute()

        respuesta = {
            "procesos": result.data,
            "total": result.count,
            "page": page,
            "limit": limit,
            "pages": (result.count + limit - 1) // limit if result.count else 0
        }
        # Guardar en caché — limpiar entradas viejas si hay más de 100
        if len(_cache_procesos) > 100:
            _cache_procesos.clear()
        _cache_procesos[cache_key] = {"data": respuesta, "ts": time.time()}
        return respuesta

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
        
        # Traer análisis Gemini si existe — ordenar para que completado tenga prioridad
        analisis_data = None
        try:
            analisis = supabase_admin.table("analisis_pliego") \
                .select("estado, checklist_categorizado, checklist_legal, resumen_ejecutivo, evaluacion_competitividad, alertas_fraude, plazos_clave, restricciones_participacion, requisitos_experiencia, requisitos_financieros, garantias_exigidas, personal_y_equipos, tipo_proceso") \
                .eq("proceso_id", codigo_proceso) \
                .order("estado", desc=False) \
                .execute()
            if analisis.data:
                # Preferir la fila con estado completado si hay varias
                completados = [r for r in analisis.data if r.get("estado") == "completado"]
                analisis_data = completados[0] if completados else analisis.data[0]
                # Normalizar checklist — puede estar en varios campos según cuándo se analizó
                if not analisis_data.get("checklist_categorizado"):
                    # Caso 1: solo tiene checklist_legal (análisis antiguo)
                    if analisis_data.get("checklist_legal"):
                        analisis_data["checklist_categorizado"] = {"legal": analisis_data["checklist_legal"]}
                # Asegurar que checklist_categorizado tenga la estructura correcta
                ck = analisis_data.get("checklist_categorizado")
                if ck and isinstance(ck, dict):
                    # Si tiene subclave anidada extra (ej: {"checklist_documentos": {...}})
                    if "checklist_documentos" in ck and not any(k in ck for k in ["legal","tecnica","financiera"]):
                        analisis_data["checklist_categorizado"] = ck["checklist_documentos"]
        except Exception:
            pass

        return {
            "proceso": proc.data[0],
            "articulos": arts.data,
            "analisis": analisis_data
        }
    
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/plazos-bulk")
def plazos_bulk(codigos: str):
    """
    Devuelve plazos_clave del análisis IA para múltiples procesos en una sola llamada.
    codigos: string separado por comas, ej: INAPA-001,MOPC-002
    """
    try:
        lista = [c.strip() for c in codigos.split(',') if c.strip()]
        if not lista:
            return {}
        res = supabase_admin.table("analisis_pliego") \
            .select("proceso_id, plazos_clave") \
            .in_("proceso_id", lista) \
            .execute()
        return {r["proceso_id"]: r["plazos_clave"] for r in (res.data or []) if r.get("plazos_clave")}
    except Exception as e:
        return {}


# ══════════════════════════════════════════════════════════════
# NURTURING DE EMAILS — secuencia de 4 emails post-registro
# ══════════════════════════════════════════════════════════════

def _resend_send(subject: str, html: str, to_email: str) -> bool:
    """Envía un email via Resend con retry ante rate limit. Retorna True si fue exitoso."""
    RESEND_API_KEY = os.getenv("RESEND_API_KEY")
    FROM_EMAIL     = os.getenv("RESEND_FROM", "LicitacionLab <notificaciones@licitacionlab.com>")
    if not RESEND_API_KEY:
        return False
    for intento in range(3):
        try:
            resp = requests.post(
                "https://api.resend.com/emails",
                headers={"Authorization": f"Bearer {RESEND_API_KEY}", "Content-Type": "application/json"},
                json={"from": FROM_EMAIL, "to": [to_email], "subject": subject, "html": html},
                timeout=20,
            )
            if resp.status_code in (200, 201):
                return True
            if resp.status_code == 429:
                # Rate limit — esperar y reintentar
                espera = (intento + 1) * 5
                print(f"⚠️ Resend rate limit — esperando {espera}s (intento {intento+1}/3)")
                time.sleep(espera)
                continue
            print(f"⚠️ Resend error {resp.status_code}: {resp.text[:100]}")
            return False
        except Exception as e:
            print(f"⚠️ Resend error: {e}")
            if intento < 2:
                time.sleep(3)
    return False


def _html_wrap(contenido: str, nombre: str) -> str:
    """Envuelve el contenido en el template base del email."""
    APP_URL   = os.getenv("APP_URL", "https://web-production-7b940.up.railway.app")
    WS_NUMBER = os.getenv("WHATSAPP_NUMBER", "18098154457")
    import urllib.parse
    ws_url = f"https://wa.me/{WS_NUMBER}?text={urllib.parse.quote('Hola, tengo una pregunta sobre LicitacionLab.')}"
    return f"""<!DOCTYPE html>
<html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"></head>
<body style="margin:0;padding:0;background:#f3f4f6;font-family:'Helvetica Neue',Arial,sans-serif;">
<table width="100%" cellpadding="0" cellspacing="0" style="background:#f3f4f6;padding:24px 0;">
<tr><td align="center">
<table width="600" cellpadding="0" cellspacing="0" style="background:white;border-radius:12px;overflow:hidden;box-shadow:0 2px 12px rgba(0,0,0,0.1);max-width:100%;">
  <tr><td style="background:linear-gradient(135deg,#0D1B2A,#172F46);padding:24px 28px;">
    <h1 style="margin:0;color:#2ECC71;font-size:18px;font-weight:800;">LicitacionLab</h1>
    <p style="margin:4px 0 0;color:#8FAEC9;font-size:11px;">Inteligencia en Compras Públicas · República Dominicana</p>
  </td></tr>
  <tr><td style="padding:28px;">
    <p style="margin:0 0 6px;font-size:15px;color:#1e293b;">Hola <strong>{nombre}</strong>,</p>
    {contenido}
  </td></tr>
  <tr><td style="background:#f8fafc;padding:16px 28px;border-top:1px solid #e5e7eb;">
    <p style="margin:0;font-size:11px;color:#9ca3af;text-align:center;line-height:1.7;">
      LicitacionLab · Monitoreo de licitaciones públicas · República Dominicana<br>
      <a href="{APP_URL}" style="color:#2ECC71;text-decoration:none;">Abrir plataforma</a> &nbsp;·&nbsp;
      <a href="{ws_url}" style="color:#9ca3af;">Contáctanos por WhatsApp</a>
    </p>
  </td></tr>
</table></td></tr></table>
</body></html>"""


def _html_proceso_row(p: dict) -> str:
    """Genera una fila HTML para un proceso en el email."""
    APP_URL  = os.getenv("APP_URL", "https://web-production-7b940.up.railway.app")
    titulo   = (p.get("titulo") or "Sin título")[:80]
    entidad  = p.get("unidad_compra") or "—"
    monto    = p.get("monto_estimado")
    monto_str = f"RD$ {float(monto):,.0f}" if monto else "No especificado"
    cierre   = ""
    if p.get("fecha_fin_recepcion_ofertas"):
        try:
            cierre = datetime.fromisoformat(str(p["fecha_fin_recepcion_ofertas"]).replace("Z","")).strftime("%d/%m/%Y")
        except Exception:
            cierre = str(p["fecha_fin_recepcion_ofertas"])[:10]
    url = f"{APP_URL}?proceso={p.get('codigo_proceso','')}"
    return f"""
<tr><td style="padding:12px 0;border-bottom:1px solid #f1f5f9;">
  <p style="margin:0 0 4px;font-size:13px;font-weight:bold;color:#1e293b;line-height:1.4;">{titulo}</p>
  <p style="margin:0 0 6px;font-size:12px;color:#64748b;">{entidad}</p>
  <table cellpadding="0" cellspacing="0"><tr>
    <td style="padding-right:16px;"><span style="font-size:10px;color:#94a3b8;text-transform:uppercase;display:block;">Monto estimado</span>
    <strong style="font-size:12px;color:#2ECC71;">{monto_str}</strong></td>
    <td><span style="font-size:10px;color:#94a3b8;text-transform:uppercase;display:block;">Cierre de ofertas</span>
    <strong style="font-size:12px;color:#dc2626;">{cierre}</strong></td>
  </tr></table>
  <a href="{url}" style="display:inline-block;margin-top:8px;padding:6px 14px;background:#eff6ff;border:1px solid #bfdbfe;border-radius:6px;font-size:12px;font-weight:700;color:#1d4ed8;text-decoration:none;">Ver proceso →</a>
</td></tr>"""


def _obtener_procesos_para_usuario(user_id: str) -> list:
    """
    Obtiene 3 procesos activos para mostrar en el Email 2.
    Si el usuario tiene intereses configurados, filtra por ellos.
    Si no, devuelve los 3 de mayor monto activos.
    """
    try:
        # Ver si tiene intereses en user_subscriptions
        subs = supabase_admin.table("user_subscriptions") \
            .select("intereses_rubros") \
            .eq("user_id", user_id) \
            .eq("active", True) \
            .limit(1).execute()

        rubros = []
        if subs.data:
            rubros = subs.data[0].get("intereses_rubros") or []

        query = supabase.table("procesos") \
            .select("codigo_proceso,titulo,unidad_compra,monto_estimado,objeto_proceso,fecha_fin_recepcion_ofertas") \
            .eq("estado_proceso", "Activo") \
            .order("monto_estimado", desc=True) \
            .limit(3)

        if rubros:
            # Filtrar por rubro (objeto_proceso contiene el tipo)
            rubro = rubros[0]  # usar el primero
            query = supabase.table("procesos") \
                .select("codigo_proceso,titulo,unidad_compra,monto_estimado,objeto_proceso,fecha_fin_recepcion_ofertas") \
                .eq("estado_proceso", "Activo") \
                .ilike("objeto_proceso", f"%{rubro}%") \
                .order("monto_estimado", desc=True) \
                .limit(3)

        res = query.execute()
        return res.data or []
    except Exception as e:
        print(f"⚠️ Error obteniendo procesos para nurturing: {e}")
        return []


def _email2_html(nombre: str, procesos: list, tiene_perfil: bool) -> str:
    APP_URL = os.getenv("APP_URL", "https://web-production-7b940.up.railway.app")
    filas   = "".join(_html_proceso_row(p) for p in procesos[:3])
    perfil_nota = "" if tiene_perfil else f"""
<div style="margin-top:16px;padding:12px 14px;background:#fffbeb;border:1px solid #fcd34d;border-radius:8px;">
  <p style="margin:0;font-size:12px;color:#92400e;">
    <strong>Tip:</strong> Estas son las licitaciones de mayor monto activas ahora mismo.
    <a href="{APP_URL}" style="color:#1d4ed8;">Configura tus intereses en Mi Perfil</a>
    para recibir alertas filtradas exactamente para tu empresa.
  </p>
</div>"""
    contenido = f"""
<p style="margin:0 0 16px;font-size:14px;color:#475569;line-height:1.6;">
  Han pasado dos días desde que te registraste. Mientras tanto, el sistema
  ha estado monitoreando el portal DGCP por ti. Aquí están las oportunidades
  activas más relevantes ahora mismo:
</p>
<table width="100%" cellpadding="0" cellspacing="0">{filas}</table>
{perfil_nota}
<p style="margin:20px 0 0;font-size:13px;color:#475569;line-height:1.6;">
  En la plataforma hay más procesos filtrados en tiempo real. No esperes
  a que se acerque la fecha — los mejores oferentes empiezan a prepararse
  con semanas de anticipación.
</p>
<p style="margin:16px 0 0;" align="center">
  <a href="{APP_URL}" style="display:inline-block;background:#2ECC71;color:#0D1B2A;text-decoration:none;padding:13px 28px;border-radius:8px;font-size:14px;font-weight:800;">
    Ver todas las oportunidades activas →
  </a>
</p>
<p style="margin:20px 0 0;font-size:13px;color:#94a3b8;">
  — El equipo de LicitacionLab
</p>"""
    return _html_wrap(contenido, nombre)


def _email3_html(nombre: str) -> str:
    APP_URL = os.getenv("APP_URL", "https://web-production-7b940.up.railway.app")
    contenido = f"""
<p style="margin:0 0 16px;font-size:14px;color:#475569;line-height:1.6;">
  El mayor problema con las licitaciones en República Dominicana no es
  encontrarlas — es entenderlas a tiempo.
</p>
<p style="margin:0 0 16px;font-size:14px;color:#475569;line-height:1.6;">
  Un pliego puede tener 200 páginas de requisitos técnicos, condiciones
  contractuales y especificaciones. Leerlo bien toma días. Y cuando hay
  varios procesos activos al mismo tiempo, es imposible hacerlo todo.
</p>
<div style="background:#f0fdf4;border:1px solid #bbf7d0;border-radius:10px;padding:18px 20px;margin:16px 0;">
  <p style="margin:0 0 10px;font-size:13px;font-weight:800;color:#166534;">Cuando sigues un proceso en LicitacionLab, el sistema analiza el pliego automáticamente y te entrega:</p>
  <table width="100%" cellpadding="0" cellspacing="0">
    <tr><td style="padding:5px 0;font-size:13px;color:#1e293b;">✅ <strong>Checklist de documentos</strong> requeridos, separados por Legal, Técnica y Financiera</td></tr>
    <tr><td style="padding:5px 0;font-size:13px;color:#1e293b;">🚨 <strong>Alertas de irregularidades</strong> — detecta si el proceso presenta indicios de estar dirigido o amarrado según la Ley 47-25</td></tr>
    <tr><td style="padding:5px 0;font-size:13px;color:#1e293b;">📊 <strong>Evaluación de competitividad</strong> — qué tan viable es el proceso para tu empresa</td></tr>
    <tr><td style="padding:5px 0;font-size:13px;color:#1e293b;">📅 <strong>Cronograma completo</strong> de fechas clave: consultas, apertura, adjudicación</td></tr>
    <tr><td style="padding:5px 0;font-size:13px;color:#1e293b;">💼 <strong>Requisitos de experiencia y financieros</strong> mínimos que exige el pliego</td></tr>
  </table>
</div>
<p style="margin:0 0 16px;font-size:14px;color:#475569;line-height:1.6;">
  Lo que antes tomaba 2 días de tu equipo, ahora toma 30 segundos.
  Tú decides si vale la pena profundizar — con información, no con suposiciones.
</p>
<p style="margin:0 0 6px;font-size:13px;color:#475569;">
  El análisis completo te llega también a este correo. Si no lo ves en tu bandeja,
  revisa la carpeta de <strong>correos no deseados</strong>.
</p>
<p style="margin:16px 0 0;" align="center">
  <a href="{APP_URL}" style="display:inline-block;background:#2ECC71;color:#0D1B2A;text-decoration:none;padding:13px 28px;border-radius:8px;font-size:14px;font-weight:800;">
    Seguir un proceso y ver el análisis →
  </a>
</p>
<p style="margin:20px 0 0;font-size:13px;color:#94a3b8;">
  — El equipo de LicitacionLab<br>
  <span style="font-size:12px;">¿Ya lo probaste? Cuéntanos qué te pareció — respondemos en 15 minutos.</span>
</p>"""
    return _html_wrap(contenido, nombre)


def _email4_html(nombre: str) -> str:
    APP_URL   = os.getenv("APP_URL", "https://web-production-7b940.up.railway.app")
    WS_NUMBER = os.getenv("WHATSAPP_NUMBER", "18098154457")
    import urllib.parse
    ws_url = f"https://wa.me/{WS_NUMBER}?text={urllib.parse.quote(f'Hola, soy {nombre} y quiero compartir mi experiencia con LicitacionLab.')}"
    contenido = f"""
<p style="margin:0 0 16px;font-size:14px;color:#475569;line-height:1.6;">
  LicitacionLab está en beta y tu opinión vale más que cualquier análisis de mercado.
</p>
<p style="margin:0 0 16px;font-size:14px;color:#475569;line-height:1.6;">
  Llevas 10 días en la plataforma. Queremos saber directamente de ti:
</p>
<div style="background:#f8fafc;border:1px solid #e2e8f0;border-radius:10px;padding:18px 20px;margin:16px 0;">
  <p style="margin:0 0 10px;font-size:13px;color:#1e293b;line-height:1.6;">
    · ¿Encontraste licitaciones relevantes para tu empresa?<br>
    · ¿El análisis del pliego te resultó útil?<br>
    · ¿Qué le agregarías o cambiarías?<br>
    · ¿Hay algo que no funciona como esperabas?
  </p>
</div>
<p style="margin:0 0 16px;font-size:14px;color:#475569;line-height:1.6;">
  No hace falta que sea largo. Con una línea nos ayudas a construir
  una mejor herramienta para todo el sector.
</p>
<p style="margin:16px 0 0;" align="center">
  <a href="{ws_url}" style="display:inline-block;background:#25d366;color:white;text-decoration:none;padding:13px 28px;border-radius:8px;font-size:14px;font-weight:800;">
    💬 Enviar feedback por WhatsApp
  </a>
</p>
<p style="margin:12px 0 0;font-size:12px;color:#94a3b8;text-align:center;">
  O simplemente responde este email directamente.
</p>
<p style="margin:20px 0 0;font-size:13px;color:#94a3b8;">
  — El equipo de LicitacionLab<br>
  <span style="font-size:12px;">Gracias por ser parte de la beta.</span>
</p>"""
    return _html_wrap(contenido, nombre)


def ejecutar_nurturing():
    """
    Corre una vez al día. Para cada usuario en user_nurturing,
    determina qué email debe recibir según los días transcurridos
    desde su registro y si ya recibió el anterior.
    """
    RESEND_API_KEY = os.getenv("RESEND_API_KEY")
    if not RESEND_API_KEY:
        print("⚠️ Nurturing: sin RESEND_API_KEY, saltando.")
        return

    # Secuencia: { numero_email: dias_desde_registro }
    SECUENCIA = {2: 2, 3: 5, 4: 10}

    try:
        usuarios = supabase_admin.table("user_nurturing") \
            .select("*") \
            .eq("activo", True) \
            .execute().data or []

        hoy = datetime.utcnow()
        enviados = 0

        for u in usuarios:
            try:
                fecha_reg = datetime.fromisoformat(str(u["fecha_registro"]).replace("Z", ""))
                dias = (hoy - fecha_reg).days
                ultimo = u.get("ultimo_email", 0)
                email  = u["email"]
                nombre = u.get("nombre") or "Usuario"
                uid    = u["user_id"]

                # Determinar si toca algún email
                proximo = None
                for num_email, dia_objetivo in sorted(SECUENCIA.items()):
                    if num_email > ultimo and dias >= dia_objetivo:
                        proximo = num_email
                        break

                if not proximo:
                    continue

                # Generar y enviar el email correspondiente
                if proximo == 2:
                    procesos     = _obtener_procesos_para_usuario(uid)
                    tiene_perfil = bool(procesos and
                        supabase_admin.table("user_subscriptions")
                            .select("intereses_rubros").eq("user_id", uid)
                            .eq("active", True).limit(1).execute().data)
                    subject = "3 oportunidades activas que quizás no viste todavía"
                    html    = _email2_html(nombre, procesos, tiene_perfil)

                elif proximo == 3:
                    subject = "El pliego tiene 200 páginas. Nosotros lo leemos en 30 segundos."
                    html    = _email3_html(nombre)

                elif proximo == 4:
                    subject = f"{nombre}, 10 días en LicitacionLab — ¿qué te ha parecido?"
                    html    = _email4_html(nombre)

                else:
                    continue

                ok = _resend_send(subject, html, email)
                if ok:
                    supabase_admin.table("user_nurturing").update({
                        "ultimo_email": proximo,
                        "fecha_ultimo": hoy.isoformat()
                    }).eq("user_id", uid).execute()
                    enviados += 1
                    print(f"📧 Nurturing Email {proximo} → {email}")
                else:
                    print(f"⚠️ Nurturing fallo Email {proximo} → {email}")

            except Exception as e:
                print(f"⚠️ Nurturing error usuario {u.get('user_id')}: {e}")

        print(f"✅ Nurturing completado: {enviados} emails enviados de {len(usuarios)} usuarios")

    except Exception as e:
        print(f"❌ Error en ejecutar_nurturing: {e}")


@app.post("/api/bienvenida")
async def enviar_bienvenida(request: Request):
    """Envía correo de bienvenida al nuevo usuario tras el registro."""
    RESEND_API_KEY = os.getenv("RESEND_API_KEY")
    FROM_EMAIL     = os.getenv("RESEND_FROM", "LicitacionLab <notificaciones@licitacionlab.com>")
    APP_URL        = os.getenv("APP_URL", "https://web-production-7b940.up.railway.app")
    WS_NUMBER      = os.getenv("WHATSAPP_NUMBER", "18098154457")
    import urllib.parse
    ws_url = f"https://wa.me/{WS_NUMBER}?text={urllib.parse.quote('Hola, acabo de registrarme en LicitacionLab y tengo una pregunta.')}"

    if not RESEND_API_KEY:
        return {"ok": False, "detail": "RESEND_API_KEY no configurada"}

    try:
        body = await request.json()
        email  = body.get("email", "")
        nombre = body.get("nombre", "Usuario")
        if not email:
            return {"ok": False, "detail": "email requerido"}

        html = f"""<!DOCTYPE html>
<html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"></head>
<body style="margin:0;padding:0;background:#f3f4f6;font-family:'Helvetica Neue',Arial,sans-serif;">
<table width="100%" cellpadding="0" cellspacing="0" style="background:#f3f4f6;padding:24px 0;">
<tr><td align="center">
<table width="600" cellpadding="0" cellspacing="0" style="background:white;border-radius:12px;overflow:hidden;box-shadow:0 2px 12px rgba(0,0,0,0.1);max-width:100%;">

  <tr><td style="background:linear-gradient(135deg,#0D1B2A,#172F46);padding:28px;">
    <h1 style="margin:0;color:#2ECC71;font-size:20px;font-weight:800;">LicitacionLab</h1>
    <p style="margin:4px 0 0;color:#8FAEC9;font-size:12px;">Inteligencia en Compras Públicas · República Dominicana</p>
  </td></tr>

  <tr><td style="padding:28px 28px 12px;">
    <p style="margin:0;font-size:16px;color:#1e293b;">Hola <strong>{nombre}</strong>, bienvenido 👋</p>
    <p style="margin:10px 0 0;font-size:14px;color:#475569;line-height:1.6;">
      Tu cuenta en LicitacionLab ya está lista. Aquí te explico rápidamente cómo sacarle el máximo provecho:
    </p>
  </td></tr>

  <tr><td style="padding:12px 28px;">
    <table width="100%" cellpadding="0" cellspacing="0">
      <tr><td style="padding:10px 14px;background:#f8fafc;border-radius:8px;border-left:4px solid #2ECC71;margin-bottom:8px;">
        <p style="margin:0;font-size:13px;font-weight:bold;color:#1e293b;">1. Busca licitaciones</p>
        <p style="margin:4px 0 0;font-size:12px;color:#64748b;">Usa la barra de búsqueda por título, código o categoría UNSPSC. Filtra por Obras, Bienes o Servicios.</p>
      </td></tr>
    </table>
    <table width="100%" cellpadding="0" cellspacing="0" style="margin-top:8px;">
      <tr><td style="padding:10px 14px;background:#f8fafc;border-radius:8px;border-left:4px solid #2ECC71;">
        <p style="margin:0;font-size:13px;font-weight:bold;color:#1e293b;">2. Sigue los procesos que te interesan</p>
        <p style="margin:4px 0 0;font-size:12px;color:#64748b;">Presiona "Seguir proceso" para agregar a Mis Procesos. Recibirás alertas automáticas antes de cada fecha clave.</p>
      </td></tr>
    </table>
    <table width="100%" cellpadding="0" cellspacing="0" style="margin-top:8px;">
      <tr><td style="padding:10px 14px;background:#f8fafc;border-radius:8px;border-left:4px solid #2ECC71;">
        <p style="margin:0;font-size:13px;font-weight:bold;color:#1e293b;">3. Analiza el pliego automáticamente</p>
        <p style="margin:4px 0 0;font-size:12px;color:#64748b;">Al seguir un proceso, recibirás en este correo el análisis completo: checklist de documentos, alertas de irregularidades, requisitos y más. Si no lo ves, revisa tu carpeta de <strong>correos no deseados</strong>.</p>
      </td></tr>
    </table>
  </td></tr>

  <tr><td style="padding:16px 28px 24px;" align="center">
    <a href="{APP_URL}" style="display:inline-block;background:#2ECC71;color:#0D1B2A;text-decoration:none;padding:14px 32px;border-radius:8px;font-size:15px;font-weight:800;letter-spacing:0.3px;margin-bottom:12px;">
      Abrir LicitacionLab
    </a>
    <br>
    <a href="{ws_url}" style="display:inline-block;background:#25d366;color:white;text-decoration:none;padding:11px 24px;border-radius:8px;font-size:13px;font-weight:700;margin-top:8px;">
      💬 ¿Dudas? Escríbenos por WhatsApp — respondemos en 15 min
    </a>
  </td></tr>

  <tr><td style="background:#f8fafc;padding:14px 28px;border-top:1px solid #e5e7eb;">
    <p style="margin:0;font-size:11px;color:#9ca3af;text-align:center;line-height:1.6;">
      LicitacionLab · Monitoreo de licitaciones públicas · República Dominicana<br>
      Recibiste este correo porque te registraste en LicitacionLab.
    </p>
  </td></tr>

</table></td></tr></table>
</body></html>"""

        resp = requests.post(
            "https://api.resend.com/emails",
            headers={"Authorization": f"Bearer {RESEND_API_KEY}", "Content-Type": "application/json"},
            json={"from": FROM_EMAIL, "to": [email], "subject": "Bienvenido a LicitacionLab", "html": html},
            timeout=15,
        )
        if resp.status_code in (200, 201):
            print(f"📧 Bienvenida enviada a {email}")
            # Registrar en user_nurturing para la secuencia
            user_id = body.get("user_id", "")
            if user_id:
                try:
                    supabase_admin.table("user_nurturing").upsert({
                        "user_id":        user_id,
                        "email":          email,
                        "nombre":         nombre,
                        "ultimo_email":   1,
                        "fecha_registro": datetime.utcnow().isoformat(),
                        "activo":         True,
                    }, on_conflict="user_id").execute()
                except Exception as e:
                    print(f"⚠️ No se pudo registrar en nurturing: {e}")
            return {"ok": True}
        else:
            print(f"⚠️ Error bienvenida {email}: {resp.status_code}")
            return {"ok": False}
    except Exception as e:
        print(f"⚠️ Error bienvenida: {e}")
        return {"ok": False}


@app.post("/api/procesos/{codigo_proceso}/analizar")
async def solicitar_analisis_proceso(codigo_proceso: str, background_tasks: BackgroundTasks):
    """
    Solicita análisis IA de un proceso desde el frontend.
    Verifica que el proceso exista, crea/resetea el registro en analisis_pliego
    y encola el análisis con semáforo para no saturar recursos.
    """
    try:
        # Verificar que el proceso existe
        proc = supabase.table("procesos") \
            .select("codigo_proceso, titulo") \
            .eq("codigo_proceso", codigo_proceso) \
            .execute()
        if not proc.data:
            raise HTTPException(status_code=404, detail="Proceso no encontrado")

        # Crear o resetear el registro de análisis — SOLO si no está ya completado
        existente_check = supabase_admin.table("analisis_pliego") \
            .select("estado") \
            .eq("proceso_id", codigo_proceso) \
            .execute()
        estado_actual = existente_check.data[0].get("estado") if existente_check.data else None

        if estado_actual == "completado":
            # Ya está analizado — recuperar datos y enviar email directo, sin tocar Gemini
            print(f"⚡ Cache hit — {codigo_proceso} ya analizado, enviando email sin reprocesar")
            def _enviar_cache():
                full = supabase_admin.table("analisis_pliego")\
                    .select("estado, checklist_categorizado, resumen_ejecutivo, evaluacion_competitividad, alertas_fraude, plazos_clave, restricciones_participacion, requisitos_experiencia, requisitos_financieros, garantias_exigidas, personal_y_equipos, tipo_proceso")\
                    .eq("proceso_id", codigo_proceso)\
                    .execute()
                analisis_cacheado = full.data[0] if full.data else {}
                if analisis_cacheado.get("checklist_categorizado") and not analisis_cacheado.get("checklist_documentos"):
                    analisis_cacheado["checklist_documentos"] = analisis_cacheado["checklist_categorizado"]
                enviar_email_analisis(codigo_proceso, analisis_cacheado)
            background_tasks.add_task(_enviar_cache)
            return {"ok": True, "status": "cache_hit"}

        supabase_admin.table("analisis_pliego").upsert({
            "proceso_id": codigo_proceso,
            "estado": "pendiente"
        }).execute()

        # Encolar con semáforo igual que el webhook
        async def _analizar_con_semaforo():
            async with _semaforo_analisis:
                loop = asyncio.get_event_loop()
                await asyncio.wait_for(loop.run_in_executor(_executor_gemini, ejecutar_analisis_gemini, codigo_proceso), timeout=600)

        background_tasks.add_task(_analizar_con_semaforo)
        return {"status": "encolado", "proceso_id": codigo_proceso, "mensaje": "Análisis IA iniciado"}

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))



# ============================================
# ENDPOINTS — CHECKLIST DE DOCUMENTOS
# ============================================

@app.get("/api/checklist/{codigo_proceso}")
def get_checklist(codigo_proceso: str, user_id: str):
    """Obtiene el estado del checklist de un usuario para un proceso."""
    try:
        result = supabase_admin.table("checklist_estado") \
            .select("*") \
            .eq("codigo_proceso", codigo_proceso) \
            .eq("user_id", user_id) \
            .execute()
        items = {r["item_key"]: r["completado"] for r in (result.data or [])}
        return {"items": items}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/checklist/{codigo_proceso}")
async def save_checklist_item(codigo_proceso: str, request: Request):
    """Guarda/actualiza el estado de un ítem del checklist."""
    try:
        body = await request.json()
        user_id   = body.get("user_id")
        item_key  = body.get("item_key")   # ej: "legal_0", "tecnica_2"
        completado = body.get("completado", False)

        if not user_id or not item_key:
            raise HTTPException(status_code=400, detail="Falta user_id o item_key")

        supabase_admin.table("checklist_estado").upsert({
            "codigo_proceso": codigo_proceso,
            "user_id": user_id,
            "item_key": item_key,
            "completado": completado,
        }, on_conflict="codigo_proceso,user_id,item_key").execute()

        return {"ok": True}
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

    import re as _re_q
    es_codigo = bool(_re_q.match(r'^\d+$', q.strip()))

    if es_codigo:
        # Búsqueda por prefijo de código: familia, clase o subclase
        fam = q.strip()
        result = supabase.table("catalogo_unspsc")\
            .select("familia, descripcion_familia")\
            .or_(f"familia.like.{fam}%,clase.like.{fam}%,subclase.like.{fam}%")\
            .limit(50)\
            .execute()
    else:
        # Búsqueda por texto en descripción
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
        
        now_iso = datetime.now().isoformat()
        filtro_activo = {"estado_proceso": "eq.Proceso publicado"}
        
        activos = supabase.table("procesos") \
            .select("id", count="exact") \
            .eq("estado_proceso", "Proceso publicado") \
            .gt("fecha_fin_recepcion_ofertas", now_iso) \
            .execute()
        
        obras = supabase.table("procesos") \
            .select("id", count="exact") \
            .eq("estado_proceso", "Proceso publicado") \
            .gt("fecha_fin_recepcion_ofertas", now_iso) \
            .ilike("objeto_proceso", "%Obras%") \
            .execute()
        
        bienes = supabase.table("procesos") \
            .select("id", count="exact") \
            .eq("estado_proceso", "Proceso publicado") \
            .gt("fecha_fin_recepcion_ofertas", now_iso) \
            .ilike("objeto_proceso", "%Bienes%") \
            .execute()
        
        servicios = supabase.table("procesos") \
            .select("id", count="exact") \
            .eq("estado_proceso", "Proceso publicado") \
            .gt("fecha_fin_recepcion_ofertas", now_iso) \
            .ilike("objeto_proceso", "%Servicios%") \
            .execute()
        
        articulos = supabase.table("articulos_proceso").select("id", count="exact").execute()
        
        return {
            "total_procesos": procesos.count,
            "procesos_activos": activos.count,
            "obras_activas": obras.count,
            "bienes_activos": bienes.count,
            "servicios_activos": servicios.count,
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
        supabase_admin.table("user_subscriptions").upsert(data, on_conflict="endpoint").execute()
        return {"ok": True, "message": "Suscripción guardada"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/notificaciones/actualizar-userid")
async def actualizar_userid_suscripcion(payload: dict):
    """Corrige el user_id de una suscripción existente identificada por endpoint."""
    try:
        endpoint = payload.get("endpoint")
        user_id  = payload.get("user_id")
        if not endpoint or not user_id or user_id == "user":
            return {"ok": False, "detail": "Datos inválidos"}
        supabase_admin.table("user_subscriptions") \
            .update({"user_id": user_id}) \
            .eq("endpoint", endpoint).execute()
        return {"ok": True}
    except Exception as e:
        return {"ok": False, "detail": str(e)}


@app.post("/api/notificaciones/desuscribirse")
async def desuscribirse(payload: dict):
    endpoint = payload.get("endpoint")
    supabase_admin.table("user_subscriptions").update({"active": False}).eq("endpoint", endpoint).execute()
    return {"ok": True}

@app.post("/api/admin/forzar-monitor")
async def forzar_monitor(_: None = Depends(verificar_admin)):
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
    supabase_admin.table("user_subscriptions").update({"intereses_rubros": rubros}).eq("endpoint", endpoint).execute()
    return {"ok": True}


@app.post("/api/notificaciones/enviar-prueba")
async def enviar_prueba(payload: dict):
    endpoint = payload.get("endpoint")
    try:
        result = supabase_admin.table("user_subscriptions").select("*").eq("endpoint", endpoint).single().execute()
        sub = result.data
        subscription_info = {
            "endpoint": sub["endpoint"],
            "keys": {"auth": sub["auth"], "p256dh": sub["p256dh"]}
        }
        ok = enviar_push_y_limpiar(
            sub,
            titulo="🔔 LicitacionLab Test",
            cuerpo="Notificaciones funcionando correctamente.",
            url="/"
        )
        return {"ok": ok}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/admin/notificar-seguimiento")
async def notificar_seguimiento(_: None = Depends(verificar_admin)):
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
        subs = supabase_admin.table("user_subscriptions") \
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
                ok = enviar_push_y_limpiar(
                    sub,
                    titulo=urgencia,
                    cuerpo=f"{titulo} · {entidad}",
                    url=f"{APP_URL}?proceso={codigo}"
                )
                if ok:
                    enviadas += 1

    return {"notificaciones_enviadas": enviadas, "seguimientos_revisados": len(seguimientos)}

@app.get("/api/admin/cron-log")
def ver_cron_log(job: str = None, limit: int = 50, _: None = Depends(verificar_admin)):
    query = supabase.table("cron_log").select("*").order("ejecutado_at", desc=True).limit(limit)
    if job:
        query = query.eq("job", job)
    result = query.execute()
    return {"logs": result.data, "total": len(result.data)}


# ═══════════════════════════════════════════════════════════════
# ANÁLISIS DE PLIEGOS CON INTELIGENCIA ARTIFICIAL (GEMINI)
# ═══════════════════════════════════════════════════════════════

PROMPT_MAESTRO = """
Eres un perito experto en licitaciones públicas de República Dominicana con profundo conocimiento
de la Ley 47-25 de Compras y Contrataciones Públicas y su reglamento de aplicación.

Analiza el pliego adjunto con ojo crítico y profesional. Tu análisis debe ser ÚTIL para una empresa
oferente que evalúa si puede y le conviene participar.

INSTRUCCIONES IMPORTANTES:

1. Identifica el tipo de proceso: Obras, Bienes, Servicios o Consultoría.

2. Las "alertas_fraude" deben ser hallazgos REALES de indicadores de manipulación, NO restricciones
   normales de participación. Busca específicamente:
   - Requisitos desproporcionados que limitan competencia artificialmente (Art. 14, 47-25)
   - Especificaciones técnicas diseñadas para un proveedor específico
   - Plazos de presentación muy cortos sin justificación
   - Criterios de evaluación subjetivos o con pesos inusuales
   - Exigencia de marcas, modelos o proveedores específicos sin justificación técnica
   - Requisitos de experiencia con montos exactos que solo una empresa puede cumplir
   - Contrataciones directas disfrazadas de licitación (Art. 146, 47-25)

3. Las "restricciones_participacion" son las condiciones normales de elegibilidad del pliego.

4. REGLA CRÍTICA SOBRE "es_subsanable":
   - Lógica: si el pliego dice explícitamente "No subsanable" → es_subsanable=FALSE.
   - Si dice "Subsanable" O no dice nada → es_subsanable=TRUE (beneficio de la duda).
   - Las garantías económicas (seriedad, fiel cumplimiento, anticipo) son SIEMPRE es_subsanable=FALSE
     sin importar lo que diga el pliego, porque la ley no las permite subsanar.
   - Ejemplos:
     * "Registro Mercantil (Subsanable)" → es_subsanable: true
     * "Compromiso Ético" (sin indicación) → es_subsanable: true
     * "Declaración Jurada notarizada" (sin indicación) → es_subsanable: true
     * "Estatutos sociales (No subsanable)" → es_subsanable: false
     * "Garantía de Seriedad de la Oferta" → es_subsanable: false (siempre, sin excepción)

5. REGLA CRÍTICA SOBRE PLAZOS — CRONOGRAMA DE ACTIVIDADES:
   El pliego siempre incluye un "Cronograma de Actividades" o tabla de fechas. DEBES extraer de ahí:
   - "Presentación de Credenciales/Ofertas técnicas y Ofertas Económicas" → presentacion_ofertas
   - "Apertura de Credenciales/Ofertas técnicas" → apertura_ofertas
   - "Presentación de aclaraciones" o "Plazo máximo para expedir Circulares/Enmiendas" → consultas
   - "Acto de Adjudicación" → incluirlo en el resumen_ejecutivo si está disponible
   - "Reunión Aclaratoria" o "Visita al Sitio" → visita_sitio
   SIEMPRE extrae las fechas en formato DD/MM/YYYY HH:MM tal como aparecen en el cronograma.
   Si hay un campo "vigencia_oferta_dias", búscalo en el cuerpo del pliego (no en el cronograma).

6. Para los checklist_documentos, NO OMITAS NINGÚN documento que aparezca en el pliego.
   Revisa exhaustivamente TODAS las secciones: "documentos requeridos", "credenciales",
   "oferta técnica", "oferta económica", "habilitación", "capacidad legal", "capacidad técnica",
   "capacidad financiera" y similares. Incluye TODOS, aunque sean muchos.
   Clasifícalos correctamente:
   - legal: Registro Mercantil, RNC, cédula, estatutos, poderes, compromisos éticos, declaraciones juradas
   - tecnica: metodología, cronograma, experiencia en proyectos, fichas técnicas, RPE/RNCE, certificaciones
   - financiera: estados financieros, balance general, declaración ISR, índices financieros, líneas de crédito

7. EXPERIENCIA DE LA EMPRESA — extrae con MÁXIMO DETALLE:
   - Años de experiencia mínimos exigidos a la empresa
   - Cantidad y tipo de proyectos/contratos similares requeridos (ej: "3 contratos de obras viales > RD$50M")
   - Montos mínimos de contratos anteriores ejecutados
   - Si se exige que los contratos hayan sido con entidades del Estado o también privados
   - Sector o especialidad específica requerida (agua potable, vías, edificaciones, IT, etc.)
   - Periodo de tiempo en que deben haberse ejecutado (últimos 5 años, últimos 10 años, etc.)

8. PERSONAL CLAVE — extrae CADA posición por separado con todos sus requisitos:
   - Título académico exacto (Ingeniero Civil, Arquitecto, Licenciado en Administración, etc.)
   - Años de experiencia general y específica en el área
   - Si requiere registro profesional (RPE, CODIA, COLLEGESRO, etc.)
   - Si es dedicación exclusiva o puede trabajar en otros proyectos simultáneamente
   - Si es subsanable o no (aplica regla del punto 4)
   - Cualquier certificación adicional requerida

9. EQUIPOS Y MAQUINARIA — lista CADA equipo por separado con especificaciones:
   - Tipo y descripción del equipo (no agrupar, listar uno a uno)
   - Cantidad mínima requerida
   - Antigüedad máxima permitida si se especifica
   - Si debe ser propio o puede ser alquilado/comprometido
   - Capacidad o potencia mínima si se especifica

Devuelve ÚNICAMENTE un JSON válido con esta estructura exacta (sin texto antes ni después):
{
  "tipo_proceso": "Obras|Bienes|Servicios|Consultoría",
  "resumen_ejecutivo": "2-3 oraciones describiendo qué se contrata, por cuánto y aspectos más relevantes para un oferente",
  "alertas_fraude": [
    {
      "riesgo": "Alto|Medio|Bajo",
      "categoria": "Requisito desproporcional|Especificación dirigida|Plazo sospechoso|Criterio subjetivo|Conflicto de interés|Otro",
      "hallazgo": "Descripción específica del indicador con referencia a la sección del pliego",
      "articulo_ley": "Artículo de la Ley 47-25 que podría estar siendo vulnerado (si aplica)"
    }
  ],
  "restricciones_participacion": [
    {
      "tipo": "Impedimento legal|Inhabilidad|Conflicto de interés normativo",
      "descripcion": "Descripción de la restricción según el pliego"
    }
  ],
  "requisitos_experiencia": {
    "anos_experiencia_empresa": "Años mínimos de existencia/operación de la empresa",
    "contratos_similares": "Cantidad y tipo de contratos similares requeridos con montos mínimos",
    "sector_especialidad": "Sector o especialidad específica exigida (obras viales, plomería, IT, etc.)",
    "periodo_referencia": "Período en que deben haberse ejecutado (ej: últimos 5 años)",
    "ambito": "Si aplica solo contratos estatales o también privados",
    "montos_facturados": "Facturación anual mínima o acumulada exigida si aplica",
    "otros": "Cualquier otro requisito de experiencia no cubierto arriba"
  },
  "requisitos_financieros": {
    "indice_liquidez": "Valor mínimo requerido o N/A",
    "indice_endeudamiento": "Valor máximo permitido o N/A",
    "patrimonio_minimo": "Monto mínimo de patrimonio o N/A",
    "otros": "Otros índices o requisitos financieros"
  },
  "garantias_exigidas": [
    {"tipo": "Garantía de seriedad|Garantía de fiel cumplimiento|Garantía de anticipo|Otro", "monto_o_porcentaje": "...", "es_subsanable": false}
  ],
  "personal_y_equipos": {
    "personal_clave": [
      {
        "posicion": "Nombre exacto del cargo (ej: Director de Obra, Coordinador Técnico)",
        "titulo": "Título académico requerido (ej: Ingeniero Civil colegiado)",
        "anos_experiencia_general": "Años de experiencia profesional general",
        "anos_experiencia_especifica": "Años en el área específica del proyecto",
        "registro_profesional": "RPE, CODIA, COLLEGESRO u otro requerido, o N/A",
        "dedicacion": "Exclusiva|Parcial|No especificado",
        "es_subsanable": true,
        "nota": "Cualquier requisito adicional relevante"
      }
    ],
    "equipos_minimos": [
      {
        "equipo": "Nombre y tipo del equipo (ej: Excavadora sobre orugas)",
        "cantidad": "Cantidad mínima requerida",
        "especificaciones": "Capacidad, potencia u otras especificaciones técnicas requeridas",
        "antiguedad_maxima": "Años máximos de antigüedad permitidos o N/A",
        "modalidad": "Propio|Alquilado|Comprometido|Cualquiera"
      }
    ]
  },
  "checklist_documentos": {
    "legal": [{"documento": "Nombre exacto del documento", "es_subsanable": false, "nota": "Aclaraciones del pliego si las hay"}],
    "tecnica": [{"documento": "Nombre exacto del documento", "es_subsanable": false, "nota": ""}],
    "financiera": [{"documento": "Nombre exacto del documento", "es_subsanable": false, "nota": ""}]
  },
  "plazos_clave": {
    "visita_sitio": "DD/MM/YYYY HH:MM o N/A",
    "consultas": "DD/MM/YYYY HH:MM o N/A",
    "presentacion_ofertas": "DD/MM/YYYY HH:MM",
    "apertura_ofertas": "DD/MM/YYYY HH:MM o N/A",
    "vigencia_oferta_dias": "número de días o N/A"
  },
  "evaluacion_competitividad": {
    "nivel_dificultad": "Alta|Media|Baja",
    "razon": "Por qué es difícil o fácil para una empresa mediana del sector",
    "recomendacion": "Participar solo|Participar en consorcio|Evaluar con cuidado|No recomendado"
  }
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
        "convocatoria publicada",   # solo si dice "publicada", no la convocatoria del pliego
        "resolucion de inicio",
        "resolución de inicio",
        "dictamen juridico",
        "dictamen jurídico",
        "compromiso etico",
        "compromiso ético",
        "declaracion jurada",
        "declaración jurada",
        "cronograma de actividades",
        "diario libre",
        "listin diario",
        "listín diario",
        "publicacion en periodicos",
        "publicación en periódicos",
        "volumen de oferta",
        "volumen oferta",
        "oferta economica",         # formulario de oferta económica ≠ pliego
        "oferta económica",
        "formulario de presentacion",
        "formulario de oferta",
        "sncc_f0",                  # formularios SNCC (oferta económica, presentación, etc.)
        "sncc-prov",                # formularios de proveedores
        "convocatoria.pdf",         # convocatoria suelta ≠ pliego
        "fondos.pdf",               # certificado de apropiación presupuestaria
        "solicitud de compras",
        "certificacion",            # certificaciones varias
        "certificación",
        "acto de aprobacion",       # acta administrativa, no es el pliego
        "acto de aprobación",
        "acta de aprobacion",
        "acta de aprobación",
        "solicitud de compra",
        "compromiso etico",         # formulario ético, no pliego
        "compromiso ético",
        "diligencia debida",        # formulario de debida diligencia
        "conflicto de interes",
        "conflicto de interés",
        "formulario de entrega",
        "entrega de muestras",
    ]

    # Grupos de prioridad — se evalúan en orden, el primer grupo con match gana
    PRIORIDADES = [
        # Prioridad 1: Pliego de Condiciones (cualquier variante, incluyendo nombres truncados)
        [
            "pliego de condiciones",
            "pliego condiciones",
            "pliego estándar",          # ← cubre "pliego estándar de condiciones para..."
            "pliego estandar",          # ← sin tilde
            "bases de la contratacion",
            "bases de la contratación",
            "bases de contratacion",
            "pliego emendado",
            "pliego enmendado",
            "pliego corregido",
            "pliego actualizado",
            "condiciones especiales",
            "contratacion menor",       # ej: "PLIEGO CONTRATACION MENOR 2026-CM-0006.pdf"
            "contratación menor",
            "pliego contratacion",
            "pliego contratación",
        ],
        # Prioridad 2: Especificaciones Técnicas
        [
            "especificaciones tecnicas",
            "especificaciones técnicas",
            "especificaciones tecnicas.pdf",
            "ficha tecnica",
            "ficha técnica",
            "especificaciones generales",
        ],
        # Prioridad 3: cualquier PDF que diga "pliego" en el nombre
        ["pliego"],
        # Prioridad 4: TDR / Términos de Referencia (servicios/consultoría)
        # — DESPUÉS de pliego genérico para no desplazar pliegos con nombre atípico
        [
            "terminos de referencia",
            "términos de referencia",
            "tdr",
            "términos de referencia (tdr)",
        ],
        # Prioridad 5: condiciones o bases (último recurso)
        ["condiciones generales", "bases tecnicas"],
    ]

    def construir_mapa_filas(html: str) -> tuple:
        """
        Parsea la tabla de documentos del portal DGCP por filas <tr>.

        Estrategia dual nombre + tipo:
          - Nombre del archivo: criterio principal de selección.
          - Tipo del documento: criterio de desempate cuando el nombre está truncado
            o es genérico. El portal a veces trunca nombres largos en la celda visual,
            ej: "pliego estándar de condiciones para" (sin el resto del título).
            En ese caso el tipo "Terms and Conditions" / "Bases de la Contratación"
            confirma cuál es el pliego real, aunque "especificaciones tecnicas.pdf"
            también exista en la lista con nombre completo.

        Devuelve tuple: (mapa_nombres, mapa_tipos)
          mapa_nombres: {file_id: nombre_archivo_lower}
          mapa_tipos:   {file_id: tipo_documento_lower}
        """
        import re as _re
        mapa_nombres = {}
        mapa_tipos = {}

        filas = _re.findall(
            r'<tr[^>]+id=["\']grdGridDocumentList_tr\d+["\'][^>]*>(.*?)</tr>',
            html, _re.IGNORECASE | _re.DOTALL
        )

        for fila in filas:
            m_id = _re.search(
                r"documentFileId='\s*\+\s*'(\d+)'|documentFileId=(\d+)",
                fila, _re.IGNORECASE
            )
            if not m_id:
                continue
            file_id = m_id.group(1) or m_id.group(2)

            # Extraer nombre del archivo (thColumnDocumentName)
            m_nombre = _re.search(
                r'thColumnDocumentName[^>]*>(.*?)</td>',
                fila, _re.IGNORECASE | _re.DOTALL
            )
            if m_nombre:
                nombre_raw = _re.sub(r'<[^>]+>', '', m_nombre.group(1)).strip()
                mapa_nombres[file_id] = nombre_raw.lower()
            else:
                mapa_nombres[file_id] = ""

            # Extraer tipo del documento (thColumnDocumentType) — para desempate
            m_tipo = _re.search(
                r'thColumnDocumentType[^>]*>(.*?)</td>',
                fila, _re.IGNORECASE | _re.DOTALL
            )
            if m_tipo:
                tipo_raw = _re.sub(r'<[^>]+>', '', m_tipo.group(1)).strip()
                mapa_tipos[file_id] = tipo_raw.lower()
            else:
                mapa_tipos[file_id] = ""

        if mapa_nombres:
            print(f"📋 Tabla TR parseada ({len(mapa_nombres)} docs): { {k: v[:35] or '[sin nombre]' for k, v in mapa_nombres.items()} }")
            # Log tipos para diagnóstico
            tipos_log = {k: v[:40] for k, v in mapa_tipos.items() if v}
            if tipos_log:
                print(f"📋 Tipos documentos: {tipos_log}")
        else:
            print("⚠️ No se encontraron filas grdGridDocumentList en el HTML")

        return mapa_nombres, mapa_tipos

    def buscar_por_prioridad(patrones, html):
        """
        Selecciona el pliego correcto usando nombre del archivo como criterio
        principal y tipo del documento como criterio de desempate.

        FASE 1 — Parser TR: extrae nombre Y tipo de cada <tr>.
          Busca keyword del grupo en el nombre. Si hay empate o el nombre
          está truncado, el tipo rompe el empate:
            - "terms and conditions", "bases de la contratación",
              "especificaciones / fichas técnicas / pliego de condiciones"
              → confirman que es el pliego principal.

        FASE 2 — Fallback ventana: si no hubo filas TR, busca el nombre
          en ventana corta hacia atrás del fileId en el HTML.

        FASE 3 — Desempate por tipo: si varios docs matchean el mismo grupo
          de prioridad, preferir el que tenga tipo de pliego confirmado.

        FASE 4 — Último recurso: primer doc cuyo nombre no esté en KEYWORDS_EXCLUIR.
        """
        import re as _re_fase

        # Tipos de documento que confirman que es el pliego principal
        TIPOS_PLIEGO = [
            "terms and conditions",
            "bases de la contratacion",
            "bases de la contratación",
            "bases de contratacion",
            "pliego de condiciones",
            "condiciones especiales de contratacion",
            "condiciones especiales de contratación",
            # NOTA: "especificaciones / fichas técnicas / pliego de condiciones" se excluye
            # porque ese mismo texto aparece en "Acto de Aprobación Especificaciones..."
            # que es un documento administrativo, no el pliego real.
        ]

        # Tipos que deben excluirse aunque contengan palabras de pliego
        TIPOS_ACTO_EXCLUIR = [
            "acto de aprobacion",
            "acto de aprobación",
            "acta de aprobacion",
            "acta de aprobación",
            "solicitud compra",
            "solicitud de compra",
        ]

        # FASE 1: parsear tabla TR (nombre + tipo)
        nombres_map, tipos_map = construir_mapa_filas(html)

        # Si el parser TR no encontró nada, usar ventana de contexto (solo nombre)
        if not nombres_map:
            PAT_NOM = r'>([\w\s\-\.\_\(\)]+\.(?:pdf|zip|xlsx|docx|doc))<'
            PAT_NOM2 = r'"([\w\s\-\.\_\(\)]+\.(?:pdf|zip|xlsx|docx|doc))"'
            for file_id, _ in patrones:
                idx = html.find(str(file_id))
                if idx == -1:
                    nombres_map[file_id] = ""
                    continue
                ctx = html[max(0, idx - 500):idx]
                m = _re_fase.search(PAT_NOM, ctx, _re_fase.IGNORECASE) or \
                    _re_fase.search(PAT_NOM2, ctx, _re_fase.IGNORECASE)
                nombres_map[file_id] = m.group(1).strip().lower() if m else ""
            print(f"📋 Fallback ventana ({len(nombres_map)} docs): { {k: v[:35] for k, v in nombres_map.items()} }")

        # Asegurar que todos los patrones estén en el mapa
        for file_id, mkey in patrones:
            if file_id not in nombres_map:
                nombres_map[file_id] = ""
            if file_id not in tipos_map:
                tipos_map[file_id] = ""

        def tipo_es_pliego(file_id):
            """True si el tipo confirma pliego principal, excluyendo actos administrativos."""
            tipo = tipos_map.get(file_id, "")
            # Primero excluir actos administrativos aunque contengan palabras de pliego
            if any(t in tipo for t in TIPOS_ACTO_EXCLUIR):
                return False
            return any(t in tipo for t in TIPOS_PLIEGO)

        # Buscar por nombre del archivo según grupos de prioridad
        for grupo in PRIORIDADES:
            docs_match = [
                (fid, mk) for fid, mk in patrones
                if nombres_map.get(fid)
                and not any(x in nombres_map[fid] for x in KEYWORDS_EXCLUIR)
                and any(x in nombres_map[fid] for x in grupo)
            ]
            if docs_match:
                # FASE 3: si hay varios matches, preferir el que tenga tipo confirmado
                docs_con_tipo = [(fid, mk) for fid, mk in docs_match if tipo_es_pliego(fid)]
                ganador_id, ganador_mk = (docs_con_tipo[0] if docs_con_tipo else docs_match[0])
                nombre = nombres_map[ganador_id]
                tipo_log = tipos_map.get(ganador_id, "")
                if docs_con_tipo and len(docs_match) > 1:
                    print(f"✅ Pliego por NOMBRE+TIPO (grupo '{grupo[0]}'): documentFileId={ganador_id} → '{nombre[:70]}' [tipo: {tipo_log[:50]}]")
                else:
                    print(f"✅ Pliego por NOMBRE (grupo '{grupo[0]}'): documentFileId={ganador_id} → '{nombre[:70]}'")
                return ganador_id, ganador_mk

        # FASE 4: sin match por nombre — buscar por tipo de documento directamente
        docs_por_tipo = [
            (fid, mk) for fid, mk in patrones
            if tipo_es_pliego(fid)
            and not any(x in nombres_map.get(fid, "") for x in KEYWORDS_EXCLUIR)
        ]
        if docs_por_tipo:
            file_id, mkey = docs_por_tipo[0]
            nombre = nombres_map.get(file_id, "sin nombre")
            tipo_log = tipos_map.get(file_id, "")
            print(f"✅ Pliego por TIPO (sin match de nombre): documentFileId={file_id} → '{nombre[:70]}' [tipo: {tipo_log[:50]}]")
            return file_id, mkey

        # FASE 5: último recurso — primer doc no excluido
        print("⚠️ Sin match por nombre ni tipo — usando primer doc no excluido")
        for file_id, mkey in patrones:
            nombre = nombres_map.get(file_id, "")
            if not nombre or not any(x in nombre for x in KEYWORDS_EXCLUIR):
                print(f"⚠️ Primer doc no excluido: documentFileId={file_id} → '{nombre[:70]}'")
                return file_id, mkey

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
            # Descarga con streaming + retry para evitar IncompleteRead en PDFs grandes
            MAX_INTENTOS_DL = 3
            for _intento_dl in range(MAX_INTENTOS_DL):
                try:
                    resp_pdf = session.get(url_real, headers=headers_retrieve, timeout=120, stream=True)
                    _chunks = []
                    for _chunk in resp_pdf.iter_content(chunk_size=1024 * 1024):
                        if _chunk:
                            _chunks.append(_chunk)
                    # Reemplazar .content con los bytes acumulados
                    resp_pdf._content = b"".join(_chunks)
                    print(f"📦 Respuesta RetrieveFile: {resp_pdf.status_code}, {len(resp_pdf.content):,} bytes, Content-Type: {resp_pdf.headers.get('Content-Type', '?')}")
                    break
                except Exception as _e_dl:
                    print(f"⚠️ Error descarga intento {_intento_dl+1}/{MAX_INTENTOS_DL}: {_e_dl}")
                    if _intento_dl == MAX_INTENTOS_DL - 1:
                        raise
        else:
            print(f"⚠️ Redirect JS no parseable: {repr(js_body[:300])}")

    if resp_pdf.status_code != 200:
        raise Exception(f"Error HTTP {resp_pdf.status_code} al descargar PDF")
    if len(resp_pdf.content) < 500:
        print(f"⚠️ Respuesta pequeña final: {repr(resp_pdf.content[:300])}")
        raise Exception(f"Respuesta demasiado pequeña ({len(resp_pdf.content)} bytes) tras seguir redirect")

    content_type_final = resp_pdf.headers.get("Content-Type", "").lower()
    # Si el portal devolvió un ZIP en vez de un PDF, no intentar parsear como PDF
    if "zip" in content_type_final or resp_pdf.content[:4] == b"PK\x03\x04":
        print(f"⚠️ El documento seleccionado es un ZIP, no un PDF — buscando alternativa PDF en el inventario...")
        # Buscar el primer documentFileId cuyo nombre termine en .pdf y no esté excluido
        import re as _re_zip
        pdf_alt = None
        for fid_alt, mk_alt in patrones:
            idx_alt = html_final.find(str(fid_alt))
            ctx_alt = html_final[max(0, idx_alt - 600):idx_alt + 100].lower()
            nombre_alt_m = _re_zip.search(r'["\>]([\w\s\-\._\(\)]+\.pdf)[<\"]', ctx_alt, _re_zip.IGNORECASE)
            if nombre_alt_m:
                nombre_alt = nombre_alt_m.group(1).lower()
                if not any(x in nombre_alt for x in KEYWORDS_EXCLUIR) and fid_alt != file_id_sel:
                    pdf_alt = (fid_alt, mk_alt, nombre_alt)
                    break
        if pdf_alt:
            fid_alt, mk_alt, nombre_alt = pdf_alt
            enlace_alt = f"{BASE_DOWNLOAD}?documentFileId={fid_alt}&mkey={mk_alt}"
            print(f"🔄 Reintentando con PDF alternativo: documentFileId={fid_alt} → '{nombre_alt}'")
            resp_pdf = session.get(enlace_alt, headers=headers_download, timeout=60)
            # Seguir redirect JS si aplica
            if len(resp_pdf.content) < 2000 and b"window.location.href" in resp_pdf.content:
                import re as _re_zip2
                js_body2 = resp_pdf.content.decode("utf-8", errors="ignore")
                m2 = _re_zip2.search(r"window\.location\.href\s*=\s*['\"]([^'\"]+)", js_body2)
                if m2:
                    url_alt2 = m2.group(1)
                    if not url_alt2.startswith("http"):
                        url_alt2 = "https://comunidad.comprasdominicana.gob.do" + url_alt2
                    resp_pdf = session.get(url_alt2, headers={**hdrs_exitosos, "Referer": enlace_alt}, timeout=60)
            content_type_final = resp_pdf.headers.get("Content-Type", "").lower()
            if "zip" in content_type_final or resp_pdf.content[:4] == b"PK\x03\x04":
                raise Exception(f"El documento alternativo también es un ZIP — no hay PDF disponible para analizar.")
        else:
            raise Exception(f"El pliego seleccionado es un ZIP y no se encontró un PDF alternativo en el inventario.")

    print(f"📄 PDF: {len(resp_pdf.content):,} bytes. Extrayendo texto...")

    pdf_bytes = resp_pdf.content
    del resp_pdf  # liberar la respuesta HTTP de inmediato para ahorrar RAM

    pdf_file = io.BytesIO(pdf_bytes)
    lector_pdf = PdfReader(pdf_file)

    texto_completo = ""
    for i in range(min(len(lector_pdf.pages), 80)):
        texto = lector_pdf.pages[i].extract_text()
        if texto:
            texto_completo += texto + "\n"

    del pdf_file, lector_pdf  # liberar objetos PDF de la memoria
    print(f"📝 {len(texto_completo):,} caracteres extraídos del PDF")

    # Si el PDF está escaneado (imágenes), usar los bytes crudos para que
    # Gemini Vision haga el OCR directamente — mucho más preciso
    if len(texto_completo.strip()) < 500:
        print(f"⚠️ PDF escaneado detectado ({len(texto_completo)} chars) — enviando bytes a Gemini Vision")
        import base64 as _b64

        # Si el PDF es muy grande (> 15 MB), recortar a las primeras 30 páginas
        # para no exceder el límite de Gemini Vision y evitar el 503
        PDF_MAX_BYTES = 15 * 1024 * 1024  # 15 MB
        if len(pdf_bytes) > PDF_MAX_BYTES:
            print(f"⚠️ PDF escaneado grande ({len(pdf_bytes):,} bytes) — recortando a primeras 30 páginas")
            try:
                import io as _io
                from pypdf import PdfReader as _PdfReader, PdfWriter as _PdfWriter
                _reader = _PdfReader(_io.BytesIO(pdf_bytes))
                _writer = _PdfWriter()
                _max_pags = min(30, len(_reader.pages))
                for _p in range(_max_pags):
                    _writer.add_page(_reader.pages[_p])
                _buf = _io.BytesIO()
                _writer.write(_buf)
                pdf_bytes_vision = _buf.getvalue()
                print(f"✂️ PDF recortado: {len(pdf_bytes_vision):,} bytes ({_max_pags} páginas)")
            except Exception as _e_crop:
                # Cortar bytes crudos produce PDF corrupto que Gemini rechaza con "no pages"
                # Mejor lanzar excepción para que el proceso quede en error con mensaje claro
                print(f"⚠️ Error recortando PDF: {_e_crop} — PDF demasiado grande, no se puede procesar")
                raise Exception(f"PDF escaneado demasiado grande ({len(pdf_bytes):,} bytes) y pypdf no pudo recortarlo: {_e_crop}")
        else:
            pdf_bytes_vision = pdf_bytes

        pdf_b64 = _b64.b64encode(pdf_bytes_vision).decode("utf-8")
        # Devolvemos un dict especial que ejecutar_analisis_gemini usará
        return {"__pdf_bytes_b64": pdf_b64, "__texto": texto_completo}

    return texto_completo


def enviar_email_analisis(proceso_id: str, analisis: dict):
    """
    Envía el resumen del análisis de pliego por email al usuario que siguió el proceso.
    Usa Resend como proveedor. Requiere RESEND_API_KEY en Railway.
    """
    RESEND_API_KEY = os.getenv("RESEND_API_KEY")
    SUPABASE_SERVICE_KEY = os.getenv("SUPABASE_SERVICE_KEY", os.getenv("SUPABASE_KEY"))
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
                # Intentar con la API admin de Supabase directamente via HTTP
                # (el método get_user varía según versión del SDK)
                resp_auth = requests.get(
                    f"{SUPABASE_URL}/auth/v1/admin/users/{uid}",
                    headers={
                        "apikey": SUPABASE_SERVICE_KEY,
                        "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}",
                    },
                    timeout=10,
                )
                if resp_auth.status_code == 200:
                    user_data = resp_auth.json()
                    email = user_data.get("email")
                    meta  = user_data.get("user_metadata") or {}
                    nombre = meta.get("nombre") or meta.get("full_name") or \
                             meta.get("name") or (email.split("@")[0] if email else "Usuario")
                    if email:
                        emails_destino.append(email)
                        nombres_destino.append(nombre)
                else:
                    print(f"⚠️ Auth API {resp_auth.status_code} para user {uid}: {resp_auth.text[:100]}")
            except Exception as e:
                print(f"⚠️ No se pudo obtener email de user {uid}: {e}")

        # MODO PRUEBA: si hay RESEND_TEST_EMAIL, redirigir todos los emails ahí
        # (Resend free tier solo permite enviar al email registrado sin dominio verificado)
        TEST_EMAIL = os.getenv("RESEND_TEST_EMAIL")
        if TEST_EMAIL:
            print(f"🧪 Modo prueba — redirigiendo email a {TEST_EMAIL}")
            # Conservar el nombre real del usuario, solo redirigir el destino
            nombre_real = nombres_destino[0] if nombres_destino else "Cliente"
            emails_destino  = [TEST_EMAIL]
            nombres_destino = [nombre_real]

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
            # Retry hasta 3 veces por el error de ConnectionTerminated en HTTP/2
            enviado = False
            for intento in range(3):
                try:
                    resp = requests.post(
                        "https://api.resend.com/emails",
                        headers={
                            "Authorization": f"Bearer {RESEND_API_KEY}",
                            "Content-Type": "application/json",
                        },
                        json=payload,
                        timeout=20,
                    )
                    if resp.status_code in (200, 201):
                        print(f"📧 Email enviado a {email}")
                        enviado = True
                        break
                    else:
                        print(f"⚠️ Intento {intento+1} fallido ({resp.status_code}): {resp.text[:100]}")
                except Exception as e_req:
                    print(f"⚠️ Intento {intento+1} error conexión: {e_req}")
                    time.sleep(2)
            if not enviado:
                print(f"❌ No se pudo enviar email a {email} tras 3 intentos")

    except Exception as e:
        # El email no debe romper el flujo principal
        print(f"⚠️ Error en envío de email para {proceso_id}: {e}")


def construir_html_email(proceso_id: str, proceso: dict, analisis: dict) -> str:
    """Genera el HTML del email con el análisis COMPLETO — sin botón de 'ver más'."""
    WS_NUMBER  = os.getenv("WHATSAPP_NUMBER", "18095551234")  # ej: 18091234567
    WS_MSG     = f"Hola, me interesa participar en el proceso {proceso_id} y quisiera asesoría."
    import urllib.parse
    ws_url     = f"https://wa.me/{WS_NUMBER}?text={urllib.parse.quote(WS_MSG)}"

    titulo       = proceso.get("titulo", proceso_id)[:100]
    entidad      = proceso.get("unidad_compra", "")
    objeto       = proceso.get("objeto_proceso", "")
    monto        = proceso.get("monto_estimado")
    fecha_cierre = proceso.get("fecha_fin_recepcion_ofertas", "")
    monto_str    = f"RD$ {monto:,.2f}" if monto else "No especificado"

    if fecha_cierre:
        try:
            fecha_cierre = datetime.fromisoformat(str(fecha_cierre).replace("Z","")).strftime("%d/%m/%Y")
        except Exception:
            pass

    # ── Resumen ejecutivo ──────────────────────────────────
    resumen      = analisis.get("resumen_ejecutivo", "")
    tipo_proceso = analisis.get("tipo_proceso", objeto)

    # ── Evaluación de competitividad ──────────────────────
    eval_comp    = analisis.get("evaluacion_competitividad") or {}
    dificultad   = eval_comp.get("nivel_dificultad", "")
    recomendacion = eval_comp.get("recomendacion", "")
    razon_dif    = eval_comp.get("razon", "")
    color_dif    = {"Alta": "#dc2626", "Media": "#d97706", "Baja": "#16a34a"}.get(dificultad, "#6b7280")

    # ── Plazos clave ──────────────────────────────────────
    plazos       = analisis.get("plazos_clave") or {}
    plazos_html  = ""
    labels_plazos = {
        "visita_sitio": "🏗️ Visita al sitio",
        "consultas": "❓ Cierre de consultas",
        "presentacion_ofertas": "📝 Presentación de ofertas",
        "apertura_ofertas": "📂 Apertura de sobres",
        "vigencia_oferta_dias": "⏱️ Vigencia de oferta",
    }
    for campo, label in labels_plazos.items():
        val = plazos.get(campo, "")
        if val and val != "N/A":
            plazos_html += f"<tr><td style='padding:5px 12px;font-size:12px;color:#6b7280;width:50%;'>{label}</td><td style='padding:5px 12px;font-size:13px;color:#374151;font-weight:bold;'>{val}</td></tr>"

    # ── Alertas de fraude ─────────────────────────────────
    alertas      = analisis.get("alertas_fraude") or []
    alertas_html = ""
    for a in (alertas if isinstance(alertas, list) else []):
        riesgo   = a.get("riesgo", "")
        hallazgo = a.get("hallazgo", "")
        cat      = a.get("categoria", "")
        art      = a.get("articulo_ley", "")
        color    = "#dc2626" if "alto" in riesgo.lower() else ("#d97706" if "medio" in riesgo.lower() else "#6b7280")
        art_badge = f"<span style='font-size:10px;color:#6b7280;margin-left:6px;'>{art}</span>" if art else ""
        alertas_html += f"""
        <tr>
          <td style="padding:10px 12px;border-bottom:1px solid #fee2e2;vertical-align:top;width:30%;">
            <span style="background:{color};color:white;padding:2px 8px;border-radius:12px;font-size:11px;font-weight:bold;display:block;text-align:center;margin-bottom:4px;">{riesgo}</span>
            <span style="font-size:10px;color:#9ca3af;display:block;text-align:center;">{cat}</span>
          </td>
          <td style="padding:10px 12px;border-bottom:1px solid #fee2e2;font-size:13px;color:#374151;">{hallazgo}{art_badge}</td>
        </tr>"""

    # ── Restricciones de participación ───────────────────
    restricciones     = analisis.get("restricciones_participacion") or []
    restricciones_html = ""
    for r in (restricciones if isinstance(restricciones, list) else []):
        tipo_r = r.get("tipo", "")
        desc_r = r.get("descripcion", "")
        restricciones_html += f"<li style='margin:6px 0;font-size:13px;color:#374151;'><strong style='color:#7c3aed;'>{tipo_r}:</strong> {desc_r}</li>"

    # ── Checklist por categorías ──────────────────────────
    checklist_docs = analisis.get("checklist_documentos") or analisis.get("checklist_legal") or {}

    def render_checklist_categoria(items, titulo_cat, color_cat):
        if not items:
            return ""
        html = f"<tr><td colspan='2' style='padding:8px 12px;background:{color_cat};font-size:11px;font-weight:bold;color:white;text-transform:uppercase;'>{titulo_cat}</td></tr>"
        for item in (items if isinstance(items, list) else []):
            doc       = item.get("documento", "")
            subsanable = item.get("es_subsanable", True)
            nota      = item.get("nota", "")
            bg        = "#fef2f2" if not subsanable else "white"
            badge     = "<span style='color:#dc2626;font-size:11px;font-weight:bold;'>❌ NO subsanable</span>" if not subsanable else "<span style='color:#16a34a;font-size:11px;'>✅ Subsanable</span>"
            nota_html = f"<br><span style='font-size:11px;color:#9ca3af;'>{nota}</span>" if nota else ""
            html += f"<tr style='background:{bg};'><td style='padding:6px 12px;font-size:13px;color:#374151;border-bottom:1px solid #f3f4f6;'>📄 {doc}{nota_html}</td><td style='padding:6px 12px;border-bottom:1px solid #f3f4f6;text-align:right;'>{badge}</td></tr>"
        return html

    if isinstance(checklist_docs, dict):
        checklist_html  = render_checklist_categoria(checklist_docs.get("legal", []), "⚖️ Legal", "#1e40af")
        checklist_html += render_checklist_categoria(checklist_docs.get("tecnica", []), "🔧 Técnica", "#0891b2")
        checklist_html += render_checklist_categoria(checklist_docs.get("financiera", []), "💰 Financiera", "#0f766e")
    else:
        # Fallback: lista plana (formato antiguo)
        checklist_html = render_checklist_categoria(checklist_docs, "📑 Documentos", "#4b5563")

    # ── Garantías ─────────────────────────────────────────
    garantias      = analisis.get("garantias_exigidas") or []
    garantias_html = ""
    for g in (garantias if isinstance(garantias, list) else []):
        subsanable = g.get("es_subsanable", False)
        badge      = "" if subsanable else " <span style='color:#dc2626;font-size:11px;'>⚠️ No subsanable</span>"
        garantias_html += f"<li style='margin:6px 0;font-size:13px;'><strong>{g.get('tipo','')}</strong>: {g.get('monto_o_porcentaje','')}{badge}</li>"

    # ── Requisitos experiencia y financieros ──────────────
    req_exp = analisis.get("requisitos_experiencia") or {}
    req_fin = analisis.get("requisitos_financieros") or {}

    exp_rows = ""
    labels_exp = {
        "anos_experiencia_empresa": "Años de experiencia empresa",
        "contratos_similares":      "Contratos similares requeridos",
        "sector_especialidad":      "Sector / especialidad",
        "periodo_referencia":       "Período de referencia",
        "ambito":                   "Ámbito (estatal/privado)",
        "montos_facturados":        "Facturación mínima",
        "otros":                    "Otros requisitos",
        # compatibilidad con estructura anterior
        "obras_similares":          "Obras similares",
    }
    for k, label in labels_exp.items():
        v = req_exp.get(k, "")
        if v and v not in ("N/A", "n/a", ""):
            exp_rows += f"<tr style='border-bottom:1px solid #f3f4f6;'><td style='padding:7px 12px;font-size:12px;color:#6b7280;width:38%;vertical-align:top;'>{label}</td><td style='padding:7px 12px;font-size:13px;color:#374151;'>{v}</td></tr>"

    fin_rows = ""
    labels_fin = {"indice_liquidez": "Índice de liquidez", "indice_endeudamiento": "Índice de endeudamiento", "patrimonio_minimo": "Patrimonio mínimo", "otros": "Otros"}
    for k, label in labels_fin.items():
        v = req_fin.get(k, "")
        if v and v not in ("N/A", "n/a", ""):
            fin_rows += f"<tr style='border-bottom:1px solid #f3f4f6;'><td style='padding:7px 12px;font-size:12px;color:#6b7280;width:38%;'>{label}</td><td style='padding:7px 12px;font-size:13px;color:#374151;'>{v}</td></tr>"

    # ── Personal clave ────────────────────────────────────
    personal_raw = (analisis.get("personal_y_equipos") or {}).get("personal_clave") or []
    equipos_raw  = (analisis.get("personal_y_equipos") or {}).get("equipos_minimos") or []
    personal_html = ""
    for p in (personal_raw if isinstance(personal_raw, list) else []):
        subsanable_p = p.get("es_subsanable", True)
        badge_p  = "<span style='background:#fef2f2;color:#dc2626;font-size:10px;padding:1px 5px;border-radius:4px;margin-left:5px;'>❌ NO subsanable</span>" if not subsanable_p else "<span style='background:#f0fdf4;color:#16a34a;font-size:10px;padding:1px 5px;border-radius:4px;margin-left:5px;'>✅ Subsanable</span>"
        posicion = p.get("posicion","")
        titulo   = p.get("titulo","")
        # Soporte nuevo formato con campos separados
        exp_gen  = p.get("anos_experiencia_general") or p.get("anos_experiencia","")
        exp_esp  = p.get("anos_experiencia_especifica","")
        registro = p.get("registro_profesional","")
        dedicacion = p.get("dedicacion","")
        nota_p   = p.get("nota","")
        detalles = []
        if titulo:   detalles.append(titulo)
        if exp_gen:  detalles.append(f"{exp_gen} años exp. general")
        if exp_esp:  detalles.append(f"{exp_esp} años exp. específica")
        if registro and registro not in ("N/A","n/a","No especificado",""):  detalles.append(f"Registro: {registro}")
        if dedicacion and dedicacion not in ("No especificado",""):          detalles.append(f"Dedicación: {dedicacion}")
        if nota_p:   detalles.append(nota_p)
        detalles_str = " · ".join(d for d in detalles if d)
        personal_html += f"""<tr style='border-bottom:1px solid #f3f4f6;'>
          <td style='padding:8px 12px;font-size:13px;vertical-align:top;'>
            <strong style='color:#1e293b;'>{posicion}</strong>{badge_p}
          </td>
          <td style='padding:8px 12px;font-size:12px;color:#6b7280;'>{detalles_str}</td>
        </tr>"""

    # ── Equipos ───────────────────────────────────────────
    equipos_html = ""
    for e in (equipos_raw if isinstance(equipos_raw, list) else []):
        if isinstance(e, dict):
            nombre_eq   = e.get("equipo","")
            cantidad_eq = e.get("cantidad","")
            specs_eq    = e.get("especificaciones","")
            antig_eq    = e.get("antiguedad_maxima","")
            modal_eq    = e.get("modalidad","")
            detalle_eq  = []
            if cantidad_eq: detalle_eq.append(f"Cant: {cantidad_eq}")
            if specs_eq and specs_eq not in ("N/A",""):    detalle_eq.append(specs_eq)
            if antig_eq and antig_eq not in ("N/A",""):    detalle_eq.append(f"Máx {antig_eq} años antigüedad")
            if modal_eq and modal_eq not in ("Cualquiera","No especificado",""):  detalle_eq.append(modal_eq)
            detalle_str = " · ".join(d for d in detalle_eq if d)
            sub_eq = f"<span style='font-size:11px;color:#6b7280;'> — {detalle_str}</span>" if detalle_str else ""
            equipos_html += f"<li style='font-size:13px;margin:4px 0;color:#374151;'><strong>{nombre_eq}</strong>{sub_eq}</li>"
        elif isinstance(e, str) and e:
            equipos_html += f"<li style='font-size:13px;margin:4px 0;color:#374151;'>{e}</li>"

    return f"""<!DOCTYPE html>
<html>
<head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"></head>
<body style="margin:0;padding:0;background:#f3f4f6;font-family:'Helvetica Neue',Arial,sans-serif;">
<table width="100%" cellpadding="0" cellspacing="0" style="background:#f3f4f6;padding:24px 0;">
<tr><td align="center">
<table width="620" cellpadding="0" cellspacing="0" style="background:white;border-radius:12px;overflow:hidden;box-shadow:0 2px 12px rgba(0,0,0,0.1);max-width:100%;">

  <tr><td style="background:linear-gradient(135deg,#1e40af,#2563eb);padding:24px 28px;">
    <h1 style="margin:0;color:white;font-size:20px;font-weight:800;">📋 LicitacionLab</h1>
    <p style="margin:4px 0 0;color:#bfdbfe;font-size:12px;">Análisis de proceso · República Dominicana</p>
  </td></tr>

  <tr><td style="padding:20px 28px 8px;">
    <p style="margin:0;font-size:15px;color:#1e293b;">Hola <strong>{{nombre}}</strong>,</p>
    <p style="margin:6px 0 0;font-size:13px;color:#64748b;">El análisis del siguiente proceso está listo. Aquí tienes el resumen completo:</p>
  </td></tr>

  <tr><td style="padding:8px 28px;">
    <table width="100%" style="background:#f8fafc;border-radius:8px;border:1px solid #e2e8f0;" cellpadding="0" cellspacing="0">
      <tr><td style="padding:14px 16px;">
        <p style="margin:0;font-size:15px;font-weight:bold;color:#1e293b;line-height:1.4;">{titulo}</p>
        <p style="margin:4px 0 0;font-size:12px;color:#64748b;">{entidad} · <span style="background:#dbeafe;color:#1d4ed8;padding:1px 6px;border-radius:4px;font-size:11px;">{tipo_proceso}</span></p>
        <table style="margin-top:10px;width:100%;" cellpadding="0" cellspacing="0"><tr>
          <td style="padding-right:16px;"><span style="font-size:10px;color:#94a3b8;text-transform:uppercase;display:block;">Código</span><strong style="font-size:12px;color:#1e293b;">{proceso_id}</strong></td>
          <td style="padding-right:16px;"><span style="font-size:10px;color:#94a3b8;text-transform:uppercase;display:block;">Monto estimado</span><strong style="font-size:12px;color:#1e293b;">{monto_str}</strong></td>
          <td><span style="font-size:10px;color:#94a3b8;text-transform:uppercase;display:block;">Cierre de ofertas</span><strong style="font-size:13px;color:#dc2626;">{fecha_cierre}</strong></td>
        </tr></table>
      </td></tr>
    </table>
  </td></tr>

  {'<tr><td style="padding:8px 28px;"><div style="background:#eff6ff;border-left:4px solid #3b82f6;padding:12px 14px;border-radius:0 8px 8px 0;"><p style="margin:0;font-size:13px;color:#1e40af;font-weight:bold;">💡 Resumen ejecutivo</p><p style="margin:6px 0 0;font-size:13px;color:#1e3a8a;line-height:1.5;">' + resumen + '</p></div></td></tr>' if resumen else ''}

  {'<tr><td style="padding:8px 28px;"><div style="background:#f8fafc;border:1px solid #e2e8f0;border-radius:8px;padding:12px 14px;"><table width="100%" cellpadding="0" cellspacing="0"><tr><td><p style="margin:0;font-size:13px;font-weight:bold;color:#1e293b;">📊 Evaluación de competitividad</p><p style="margin:4px 0 0;font-size:12px;color:#64748b;">' + razon_dif + '</p></td><td style="text-align:right;vertical-align:top;"><span style="background:' + color_dif + ';color:white;padding:4px 10px;border-radius:20px;font-size:12px;font-weight:bold;">Dificultad: ' + dificultad + '</span><br><span style="font-size:11px;color:#64748b;display:block;margin-top:4px;">' + recomendacion + '</span></td></tr></table></div></td></tr>' if dificultad else ''}

  {'<tr><td style="padding:8px 28px;"><p style="margin:0 0 6px;font-size:13px;font-weight:bold;color:#dc2626;">🚨 Alertas de posible irregularidad</p><p style="margin:0 0 6px;font-size:11px;color:#9ca3af;">Indicadores identificados según Ley 47-25. No implican irregularidad confirmada.</p><table width="100%" style="border:1px solid #fee2e2;border-radius:8px;overflow:hidden;" cellpadding="0" cellspacing="0">' + alertas_html + '</table></td></tr>' if alertas_html else '<tr><td style="padding:8px 28px;"><div style="background:#f0fdf4;border:1px solid #bbf7d0;padding:10px 14px;border-radius:8px;font-size:13px;color:#166534;">✅ No se detectaron alertas de irregularidad significativas.</div></td></tr>'}

  {'<tr><td style="padding:8px 28px;"><p style="margin:0 0 6px;font-size:13px;font-weight:bold;color:#7c3aed;">🚫 Restricciones de participación</p><ul style="margin:0;padding-left:18px;background:#faf5ff;border:1px solid #e9d5ff;border-radius:8px;padding:10px 10px 10px 26px;">' + restricciones_html + '</ul></td></tr>' if restricciones_html else ''}

  {'<tr><td style="padding:8px 28px;"><p style="margin:0 0 6px;font-size:13px;font-weight:bold;color:#1e293b;">📅 Plazos clave</p><table width="100%" style="border:1px solid #e5e7eb;border-radius:8px;overflow:hidden;" cellpadding="0" cellspacing="0">' + plazos_html + '</table></td></tr>' if plazos_html else ''}

  {'<tr><td style="padding:8px 28px;"><p style="margin:0 0 6px;font-size:13px;font-weight:bold;color:#1e293b;">📑 Documentos requeridos</p><table width="100%" style="border:1px solid #e5e7eb;border-radius:8px;overflow:hidden;" cellpadding="0" cellspacing="0">' + checklist_html + '</table><p style="margin:5px 0 0;font-size:11px;color:#9ca3af;">⚠️ Los documentos NO subsanables son críticos — su ausencia puede descalificarte directamente.</p></td></tr>' if checklist_html else ''}

  {'<tr><td style="padding:8px 28px;"><p style="margin:0 0 6px;font-size:13px;font-weight:bold;color:#1e293b;">🔒 Garantías exigidas</p><ul style="margin:0;background:#f8fafc;border:1px solid #e5e7eb;border-radius:8px;padding:10px 10px 10px 26px;">' + garantias_html + '</ul></td></tr>' if garantias_html else ''}

  {'<tr><td style="padding:8px 28px;"><p style="margin:0 0 6px;font-size:13px;font-weight:bold;color:#1e293b;">🏗️ Requisitos de experiencia</p><table width="100%" style="border:1px solid #e5e7eb;border-radius:8px;overflow:hidden;" cellpadding="0" cellspacing="0">' + exp_rows + '</table></td></tr>' if exp_rows else ''}

  {'<tr><td style="padding:8px 28px;"><p style="margin:0 0 6px;font-size:13px;font-weight:bold;color:#1e293b;">💰 Requisitos financieros</p><table width="100%" style="border:1px solid #e5e7eb;border-radius:8px;overflow:hidden;" cellpadding="0" cellspacing="0">' + fin_rows + '</table></td></tr>' if fin_rows else ''}

  {'<tr><td style="padding:8px 28px;"><p style="margin:0 0 6px;font-size:13px;font-weight:bold;color:#1e293b;">👷 Personal clave requerido</p><p style="margin:0 0 8px;font-size:11px;color:#9ca3af;">Cada posición con su subsanabilidad — los NO subsanables son críticos para la habilitación.</p><table width="100%" style="border:1px solid #e5e7eb;border-radius:8px;overflow:hidden;" cellpadding="0" cellspacing="0"><tr style="background:#f8fafc;"><th style="padding:6px 12px;font-size:11px;color:#6b7280;text-align:left;">Posición</th><th style="padding:6px 12px;font-size:11px;color:#6b7280;text-align:left;">Perfil requerido</th></tr>' + personal_html + '</table></td></tr>' if personal_html else ''}
  {'<tr><td style="padding:4px 28px 8px;"><p style="margin:0 0 6px;font-size:13px;font-weight:bold;color:#1e293b;">🚜 Equipos y maquinaria mínimos</p><ul style="margin:0;padding-left:18px;background:#f8fafc;border:1px solid #e5e7eb;border-radius:8px;padding:10px 10px 10px 26px;">' + equipos_html + '</ul></td></tr>' if equipos_html else ''}

  <tr><td style="padding:16px 28px 24px;" align="center">
    <p style="margin:0 0 12px;font-size:13px;color:#64748b;">¿Te interesa participar en este proceso?</p>
    <a href="{ws_url}" style="display:inline-block;background:#25d366;color:white;text-decoration:none;padding:14px 32px;border-radius:8px;font-size:15px;font-weight:bold;letter-spacing:0.3px;">
      💬 Contactar un experto por WhatsApp
    </a>
    <p style="margin:10px 0 0;font-size:11px;color:#94a3b8;">Respuesta en menos de 24 horas · LicitacionLab</p>
  </td></tr>

  <tr><td style="background:#f8fafc;padding:14px 28px;border-top:1px solid #e5e7eb;">
    <p style="margin:0;font-size:11px;color:#9ca3af;text-align:center;line-height:1.6;">
      LicitacionLab · Monitoreo de licitaciones públicas · República Dominicana<br>
      Este análisis está basado en la Ley 47-25 y debe ser verificado por un profesional.<br>
      <a href="{ws_url}" style="color:#9ca3af;">Recibiste este email porque sigues el proceso {proceso_id}</a>
    </p>
  </td></tr>

</table>
</td></tr>
</table>
</body>
</html>"""


def ejecutar_analisis_gemini(proceso_id: str, enviar_email: bool = True):
    """Descarga el pliego, lo lee y guarda el JSON en Supabase en segundo plano.

    Args:
        enviar_email: Si True (default), envía el email al completar.
                      Pasar False en reproceso automático para evitar spam.
    """
    try:
        print(f"🚀 Iniciando IA para: {proceso_id}")

        # ── CACHE: si ya está completado ──────
        existente = supabase_admin.table("analisis_pliego")\
            .select("estado")\
            .eq("proceso_id", proceso_id)\
            .eq("estado", "completado")\
            .execute()
        if existente.data:
            if enviar_email:
                print(f"⚡ Cache hit — {proceso_id} ya analizado, enviando email.")
                full = supabase_admin.table("analisis_pliego")\
                    .select("estado, checklist_categorizado, resumen_ejecutivo, evaluacion_competitividad, alertas_fraude, plazos_clave, restricciones_participacion, requisitos_experiencia, requisitos_financieros, garantias_exigidas, personal_y_equipos, puntaje_go_nogo, tipo_proceso")\
                    .eq("proceso_id", proceso_id)\
                    .execute()
                analisis_cacheado = full.data[0] if full.data else {}
                if analisis_cacheado.get("checklist_categorizado") and not analisis_cacheado.get("checklist_documentos"):
                    analisis_cacheado["checklist_documentos"] = analisis_cacheado["checklist_categorizado"]
                enviar_email_analisis(proceso_id, analisis_cacheado)
            else:
                print(f"⚡ Cache hit — {proceso_id} ya completado, saltando (reproceso automático).")
            return
        
        # 1. ACTUALIZAR ESTADO EN SUPABASE
        supabase_admin.table("analisis_pliego").upsert({
            "proceso_id": proceso_id, 
            "estado": "procesando"
        }).execute()

        # 2. SCRAPING Y EXTRACCIÓN DEL PDF
        # VÍA 1: URL con noticeUID guardada por el scraper en tiempo real
        proc_data = supabase.table("procesos").select("url").eq("codigo_proceso", proceso_id).execute()
        url_portal = proc_data.data[0].get("url", "") if proc_data.data else ""

        # VÍA 2 (fallback): si no hay noticeUID en la URL, buscar en /procesos/documentos de la API
        if not url_portal or "noticeUID" not in url_portal:
            print(f"⚠️ Sin noticeUID para {proceso_id} — buscando en API de documentos (fallback)...")
            try:
                import requests as _req
                resp = _req.get(
                    "https://datosabiertos.dgcp.gob.do/api-dgcp/v1/procesos/documentos",
                    params={"proceso": proceso_id},
                    timeout=15
                )
                docs_payload = resp.json().get("payload", {})
                docs_list = docs_payload.get("content", []) if isinstance(docs_payload, dict) else (docs_payload if isinstance(docs_payload, list) else [])

                # Buscar el pliego o documento principal
                KEYWORDS_PLIEGO = ["pliego", "bases", "condiciones", "especificaciones", "términos", "terminos"]
                doc_elegido = None
                for doc in docs_list:
                    nombre = (doc.get("nombre_documento") or "").lower()
                    tipo = (doc.get("tipo_documento") or "").lower()
                    if any(k in nombre or k in tipo for k in KEYWORDS_PLIEGO):
                        doc_elegido = doc
                        break
                if not doc_elegido and docs_list:
                    doc_elegido = docs_list[0]  # fallback al primer documento

                if doc_elegido:
                    url_doc = doc_elegido.get("url_documento", "")
                    if url_doc:
                        # Si tiene noticeUID, guardarla en Supabase para futuros usos
                        if "noticeUID" in url_doc:
                            import re as _re
                            m = _re.search(r"noticeUID=(DO1\.NTC\.[\w\.]+)", url_doc)
                            if m:
                                url_portal = (
                                    f"https://comunidad.comprasdominicana.gob.do"
                                    f"/Public/Tendering/OpportunityDetail/Index"
                                    f"?noticeUID={m.group(1)}"
                                )
                                supabase.table("procesos").update({"url": url_portal}).eq("codigo_proceso", proceso_id).execute()
                                print(f"✅ URL reconciliada desde API documentos: noticeUID={m.group(1)}")
                        else:
                            # URL directa al archivo PDF — intentar descarga directa
                            url_portal = url_doc
                        print(f"📄 Documento fallback: {doc_elegido.get('nombre_documento', '')}")
            except Exception as e_fb:
                print(f"⚠️ Fallback API documentos también falló: {e_fb}")

        if not url_portal:
            raise Exception(f"No se encontró URL del portal para {proceso_id} ni en Supabase ni en API de documentos")

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
                elif "503" in msg or "UNAVAILABLE" in msg:
                    # Gemini sobrecargado — backoff exponencial: 10s, 30s, 60s
                    esperas = [10, 30, 60]
                    espera = esperas[intento] if intento < len(esperas) else 60
                    print(f"⏳ Gemini 503 (alta demanda) — esperando {espera}s ({intento+1}/{MAX_REINTENTOS})...")
                    time.sleep(espera)
                    if intento == MAX_REINTENTOS - 1:
                        # Último intento fallido → marcar como pendiente para reprocesar
                        supabase_admin.table("analisis_pliego").upsert({
                            "proceso_id": proceso_id,
                            "estado": "pendiente_analisis"
                        }).execute()
                        print(f"⚠️ Gemini 503 persistente — {proceso_id} marcado como pendiente_analisis")
                        return
                else:
                    raise  # error no recuperable

        if datos_json is None:
            raise Exception(f"Gemini sin respuesta tras {MAX_REINTENTOS} reintentos.")
        
        # 4. GUARDAR RESULTADOS EN LA BÓVEDA
        # Mapeo explícito: columnas reales de Supabase vs campos del JSON de Gemini.
        # "checklist_documentos" (Gemini) -> "checklist_categorizado" (tabla)
        fila_supabase = {
            "proceso_id":                    proceso_id,
            "estado":                        "completado",
            "tipo_proceso":                  datos_json.get("tipo_proceso"),
            "resumen_ejecutivo":             datos_json.get("resumen_ejecutivo"),
            "alertas_fraude":                datos_json.get("alertas_fraude"),
            "restricciones_participacion":   datos_json.get("restricciones_participacion"),
            "requisitos_experiencia":        datos_json.get("requisitos_experiencia"),
            "requisitos_financieros":        datos_json.get("requisitos_financieros"),
            "garantias_exigidas":            datos_json.get("garantias_exigidas"),
            "personal_y_equipos":            datos_json.get("personal_y_equipos"),
            "checklist_categorizado":        datos_json.get("checklist_documentos"),
            "checklist_legal":               (datos_json.get("checklist_documentos") or {}).get("legal")
                                             if isinstance(datos_json.get("checklist_documentos"), dict) else None,
            "plazos_clave":                  datos_json.get("plazos_clave"),
            "evaluacion_competitividad":     datos_json.get("evaluacion_competitividad"),
        }
        # Eliminar None para no sobreescribir datos existentes con NULL
        fila_supabase = {k: v for k, v in fila_supabase.items() if v is not None}
        supabase_admin.table("analisis_pliego").upsert(fila_supabase).execute()
        
        print(f"✅ Análisis de {proceso_id} completado y guardado.")

        # 5. NOTIFICACIÓN PUSH — avisar que el análisis está listo
        try:
            APP_URL = os.getenv("APP_URL", "https://web-production-7b940.up.railway.app")
            seg = supabase_admin.table("seguimiento_procesos") \
                .select("user_id").eq("proceso_codigo", proceso_id).execute()
            user_ids_push = list(set(s["user_id"] for s in (seg.data or [])))
            for uid in user_ids_push:
                subs = supabase_admin.table("user_subscriptions") \
                    .select("endpoint,auth,p256dh").eq("user_id", uid).eq("active", True).execute().data or []
                for sub in subs:
                    enviar_push_y_limpiar(
                        sub,
                        titulo=f"📋 Análisis listo: {proceso_id}",
                        cuerpo=f"El análisis del pliego está completo. Abre la app para ver resultados o descarga el PDF.",
                        url=f"{APP_URL}?proceso={proceso_id}"
                    )
        except Exception as e_push:
            print(f"⚠️ Error enviando push de análisis: {e_push}")

        # 6. ENVIAR EMAIL AL USUARIO
        enviar_email_analisis(proceso_id, datos_json)

    except Exception as e:
        print(f"❌ Error de IA en {proceso_id}: {str(e)}")
        supabase_admin.table("analisis_pliego").upsert({
            "proceso_id": proceso_id,
            "estado": "error"
        }).execute()

@app.get("/api/admin/test-pliego")
def test_pliego(codigo: str = Query(..., description="Ej: DO1.NTC.1234567"), _: None = Depends(verificar_admin)):
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

            # Dump zona tabla de documentos — para diagnosticar estructura TR
            idx_tabla = html.find("grdGridDocumentList")
            if idx_tabla >= 0:
                intento["html_tabla_docs"] = html[max(0, idx_tabla - 200):idx_tabla + 3000]
            else:
                # Buscar por primer fileId encontrado
                primeros = []
                for regex in regexes.values():
                    m = _re.findall(regex, html, _re.IGNORECASE)
                    if m:
                        primeros = m
                        break
                if primeros:
                    first_id = primeros[0][0]
                    idx_fid = html.find(first_id)
                    intento["html_tabla_docs"] = html[max(0, idx_fid - 1500):idx_fid + 500]

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




@app.post("/api/admin/forzar-analisis")
async def forzar_analisis(background_tasks: BackgroundTasks, codigo: str, _: None = Depends(verificar_admin)):
    """Re-corre el análisis Gemini ignorando el cache. Limpia el estado primero."""
    try:
        supabase_admin.table("analisis_pliego").update({
            "estado": "pendiente",
        }).eq("proceso_id", codigo).execute()

        async def _forzar_con_semaforo():
            async with _semaforo_analisis:
                loop = asyncio.get_event_loop()
                await asyncio.wait_for(loop.run_in_executor(_executor_gemini, ejecutar_analisis_gemini, codigo), timeout=600)

        background_tasks.add_task(_forzar_con_semaforo)
        return {"status": "iniciado", "codigo": codigo, "mensaje": "Análisis re-iniciado. Revisa logs de Railway."}
    except Exception as e:
        import traceback
        return {"error": str(e), "traceback": traceback.format_exc()}


@app.get("/api/admin/debug-precio-articulo")
def debug_precio_articulo(notice_uid: str = "DO1.NTC.1679301", _: None = Depends(verificar_admin)):
    """Ver HTML exacto de las celdas de precio — usa mismos headers que scraper_articulos_portal"""
    import requests as _req
    from bs4 import BeautifulSoup as _BS

    PORTAL_BASE = "https://comunidad.comprasdominicana.gob.do"
    HEADERS = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "es-ES,es;q=0.9",
        "Referer": f"{PORTAL_BASE}/Public/Tendering/ContractNoticeManagement/Index",
    }

    session = _req.Session()
    session.verify = False
    # Obtener cookie de sesión igual que el scraper
    try:
        session.get(f"{PORTAL_BASE}/Public/Tendering/ContractNoticeManagement/Index",
                    headers=HEADERS, timeout=15)
    except Exception:
        pass

    url = f"{PORTAL_BASE}/Public/Tendering/OpportunityDetail/Index?noticeUID={notice_uid}&isModal=true&asPopupView=true"
    resp = session.get(url, headers=HEADERS, timeout=25)
    soup = _BS(resp.text, "html.parser")

    todas_trs = soup.find_all("tr")
    
    # Intentar 4 métodos distintos de selección
    filas_lambda  = soup.find_all("tr", class_=lambda x: x and "PriceListLine" in " ".join(x) and "Item" in " ".join(x))
    filas_css     = soup.select("tr.PriceListLine.Item")
    filas_flt_css = soup.select("tr.FltTr.PriceListLine.Item")
    filas_manual  = [tr for tr in todas_trs if tr.get("class") and "PriceListLine" in tr["class"] and "Item" in tr["class"]]

    filas = filas_manual or filas_css or filas_flt_css or filas_lambda

    if not filas:
        clases_tr = list(set([" ".join(tr.get("class",[])) for tr in todas_trs if tr.get("class")]))[:30]
        idx = resp.text.find("PriceListLine Item")
        zona = resp.text[max(0,idx-100):idx+800] if idx >= 0 else "NO ENCONTRADO EN HTML"
        return {
            "error": "Ningún método encontró filas",
            "html_size": len(resp.text),
            "template": soup.find("meta", {"name":"TemplateName"}) and soup.find("meta", {"name":"TemplateName"}).get("content"),
            "metodos": {
                "lambda": len(filas_lambda),
                "css": len(filas_css),
                "flt_css": len(filas_flt_css),
                "manual": len(filas_manual),
            },
            "clases_tr": clases_tr,
            "zona_html": zona,
        }

    fila = filas[0]
    line_row = fila.find("tr", class_=lambda x: x and "PriceListLineRow" in x)
    tds = line_row.find_all("td") if line_row else fila.find_all("td")

    return {
        "filas_encontradas": len(filas),
        "line_row_encontrado": line_row is not None,
        "tds_count": len(tds),
        "tds_clases": [" ".join(td.get("class", [])) for td in tds],
        "tds_texto": [td.get_text(strip=True)[:80] for td in tds],
        "fila_html": str(fila)[:4000],
    }



@app.post("/api/admin/rellenar-precios-articulos")
async def rellenar_precios_articulos(background_tasks: BackgroundTasks, _: None = Depends(verificar_admin)):
    """
    Re-scrapea artículos existentes que tienen precio_total_estimado = NULL.
    Corre en background para no bloquear el servidor.
    """
    def _run():
        from scraper_portal import scraper_articulos_portal
        import requests as _req

        # Buscar artículos sin precio
        sin_precio = supabase_admin.table("articulos_proceso") \
            .select("codigo_proceso") \
            .is_("precio_total_estimado", "null") \
            .execute().data or []

        # Agrupar por proceso
        codigos = list(set(a["codigo_proceso"] for a in sin_precio))
        print(f"🔄 Re-scrape precios: {len(codigos)} procesos con artículos sin precio")

        actualizados = 0
        for codigo in codigos[:100]:  # máx 100 por llamada
            try:
                # Obtener URL del proceso
                proc = supabase_admin.table("procesos") \
                    .select("url") \
                    .eq("codigo_proceso", codigo) \
                    .execute().data
                url = proc[0]["url"] if proc else None
                if not url or "noticeUID" not in url:
                    continue

                # Re-scrapear artículos con precios
                arts = scraper_articulos_portal(codigo, url_portal=url)
                if not arts:
                    continue

                # Actualizar cada artículo con el precio
                for a in arts:
                    precio = a.get("precio_total_estimado")
                    precio_u = a.get("precio_unitario_estimado")
                    desc = a.get("descripcion_articulo") or a.get("descripcion_usuario")
                    if precio is None and precio_u is None:
                        continue
                    supabase_admin.table("articulos_proceso") \
                        .update({
                            "precio_total_estimado": precio,
                            "precio_unitario_estimado": precio_u,
                            "unidad_medida": a.get("unidad_medida"),
                            "cantidad": a.get("cantidad"),
                        }) \
                        .eq("codigo_proceso", codigo) \
                        .eq("descripcion_articulo", desc) \
                        .execute()
                    actualizados += 1

                import time
                time.sleep(0.5)
            except Exception as e:
                print(f"⚠️ Error re-scraping {codigo}: {e}")

        print(f"✅ Re-scrape completado: {actualizados} artículos actualizados")

    background_tasks.add_task(_run)
    return {"status": "iniciado", "mensaje": "Re-scrape de precios corriendo en background. Revisa los logs."}


@app.get("/api/admin/html-articulos")
def debug_html_articulos(codigo: str = None, notice_uid: str = None, _: None = Depends(verificar_admin)):
    """
    Endpoint de diagnóstico para analizar estructura HTML de artículos del portal DGCP.
    Uso: GET /api/admin/html-articulos?notice_uid=DO1.NTC.1681956
      o: GET /api/admin/html-articulos?codigo=DIE-CCC-SO-2026-0004
    """
    import re as _re
    import requests as _req

    PORTAL_BASE = "https://comunidad.comprasdominicana.gob.do"
    HEADERS = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "es-ES,es;q=0.9",
        "Referer": f"{PORTAL_BASE}/Public/Tendering/ContractNoticeManagement/Index",
    }

    resultado = {"pasos": []}
    session = _req.Session()
    session.verify = False

    # Obtener notice_uid — directo o desde Supabase
    if not notice_uid and codigo:
        try:
            proc = supabase.table("procesos").select("url").eq("codigo_proceso", codigo).execute()
            url_db = proc.data[0].get("url", "") if proc.data else ""
            m = _re.search(r"noticeUID=(DO1\.NTC\.[\w\.]+)", url_db)
            if m:
                notice_uid = m.group(1)
                resultado["pasos"].append(f"✅ noticeUID desde Supabase: {notice_uid}")
            else:
                resultado["url_en_supabase"] = url_db
                resultado["error"] = "No hay noticeUID en la URL guardada en Supabase"
                return resultado
        except Exception as e:
            return {"error": str(e)}

    if not notice_uid:
        return {"error": "Pasa ?notice_uid=DO1.NTC.XXXXX o ?codigo=CODIGO"}

    resultado["notice_uid"] = notice_uid

    # Establecer sesión
    try:
        session.get(f"{PORTAL_BASE}/Public/Tendering/ContractNoticeManagement/Index",
                    headers=HEADERS, timeout=15)
        resultado["cookies"] = [c.name for c in session.cookies]
    except Exception as e:
        resultado["warn_sesion"] = str(e)

    # Intentar cargar detalle con variantes de URL
    url_base = f"{PORTAL_BASE}/Public/Tendering/OpportunityDetail/Index?noticeUID={notice_uid}"
    urls = [
        url_base + "&isModal=true&asPopupView=true",
        url_base + "&asPopupView=true",
        url_base,
    ]

    for url_intento in urls:
        try:
            resp = session.get(url_intento, headers=HEADERS, timeout=20)
            html = resp.text
            resultado["pasos"].append(f"✅ HTTP {resp.status_code} — {len(html)} chars — {url_intento[-50:]}")

            # Keywords de artículos
            kws = ["UNSPSC", "unspsc", "Cuestionario", "Lote", "Cantidad",
                   "Precio", "grdGrid", "GridView", "tblContainer", "Articulo"]
            resultado["keywords"] = {k: (k in html) for k in kws}

            # Zona de artículos — buscar la tabla
            for kw in ["UNSPSC", "Cuestionario", "grdGrid", "tblContainer"]:
                idx = html.find(kw)
                if idx >= 0:
                    resultado[f"zona_{kw}"] = html[max(0, idx-200):idx+4000]
                    resultado["pasos"].append(f"✅ '{kw}' encontrado en pos {idx}")
                    break

            resultado["html_inicio"] = html[:4000]
            resultado["html_final"] = html[-1000:]
            resultado["url_exitosa"] = url_intento
            break
        except Exception as e:
            resultado["pasos"].append(f"❌ {url_intento[-40:]}: {e}")

    return resultado


@app.get("/api/admin/test-articulos-portal")
def test_articulos_portal(notice_uid: str = None, codigo: str = None, _: None = Depends(verificar_admin)):
    """
    Prueba el scraper de artículos directamente del portal DGCP.
    Uso: GET /api/admin/test-articulos-portal?notice_uid=DO1.NTC.1681956
      o: GET /api/admin/test-articulos-portal?codigo=PROCESO-CODIGO
    """
    try:
        from scraper_portal import scraper_articulos_portal
        url = None
        if notice_uid:
            url = (f"https://comunidad.comprasdominicana.gob.do/Public/Tendering/"
                   f"OpportunityDetail/Index?noticeUID={notice_uid}")
            cod = codigo or "TEST"
        elif codigo:
            cod = codigo
        else:
            return {"error": "Pasa ?notice_uid=DO1.NTC.XXXXX o ?codigo=CODIGO"}

        articulos = scraper_articulos_portal(cod, url_portal=url)
        return {
            "total": len(articulos),
            "articulos": articulos[:10],
            "familias_unspsc": list(set(
                a.get("familia_unspsc") for a in articulos if a.get("familia_unspsc")
            )),
        }
    except Exception as e:
        import traceback
        return {"error": str(e), "traceback": traceback.format_exc()}




@app.get("/api/admin/html-lista-raw")
def debug_html_lista_raw(_: None = Depends(verificar_admin)):
    """Devuelve zonas clave del HTML de la lista para encontrar noticeUID"""
    import re as _re
    import requests as _req

    PORTAL_URL = (
        "https://comunidad.comprasdominicana.gob.do/Public/Tendering/"
        "ContractNoticeManagement/Index?currentLanguage=es&Country=DO&Theme=DGCP"
    )
    HEADERS = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    }
    resp = _req.get(PORTAL_URL, headers=HEADERS, timeout=20)
    html = resp.text

    # Buscar DO1 en cualquier forma
    do1_matches = [(m.start(), html[max(0,m.start()-50):m.start()+200]) 
                   for m in _re.finditer(r"DO1", html)]
    
    # Buscar data- attributes en filas de resultado
    data_attrs = _re.findall(r'data-[\w-]+="[^"]{5,50}"', html)
    
    # Buscar onclick o javascript con referencias a procesos
    onclicks = _re.findall(r'onclick="[^"]{20,200}"', html)
    
    # Buscar var o json con noticeUID en scripts
    scripts_zona = []
    for m in _re.finditer(r"noticeUID|NTC\.|OpportunityDetail", html):
        scripts_zona.append(html[max(0,m.start()-100):m.start()+300])

    # Ver 500 chars alrededor del primer CERTV (proceso que vimos)
    idx = html.find("CERTV-DAF-CD-2026-0016")
    zona_certv = html[max(0,idx-500):idx+1000] if idx >= 0 else "no encontrado"

    return {
        "do1_apariciones": len(do1_matches),
        "do1_muestras": [x[1] for x in do1_matches[:3]],
        "data_attrs_unicos": list(set(data_attrs))[:20],
        "onclicks": onclicks[:10],
        "scripts_con_noticeUID": scripts_zona[:3],
        "zona_proceso_certv": zona_certv,
    }


@app.get("/api/admin/html-lista-portal")
def debug_html_lista_portal(_: None = Depends(verificar_admin)):
    """
    Descarga el HTML raw de la lista del portal y busca noticeUID.
    Uso: GET /api/admin/html-lista-portal
    """
    import re as _re
    import requests as _req

    PORTAL_URL = (
        "https://comunidad.comprasdominicana.gob.do/Public/Tendering/"
        "ContractNoticeManagement/Index?currentLanguage=es&Country=DO&Theme=DGCP"
    )
    HEADERS = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    }
    try:
        resp = _req.get(PORTAL_URL, headers=HEADERS, timeout=20)
        html = resp.text

        # Buscar todos los noticeUID en el HTML
        uids = _re.findall(r"noticeUID=(DO1\.NTC\.[\w\.]+)", html)
        
        # Buscar todos los href con OpportunityDetail
        hrefs = _re.findall(r'href="([^"]*OpportunityDetail[^"]*)"', html)

        # Buscar todos los href con NoticeReference  
        refs = _re.findall(r'href="([^"]*NoticeReference=[^"]*)"', html)

        # Zona donde aparece el primer proceso (tabla)
        idx_do = html.find(">DO<")
        zona_tabla = html[max(0,idx_do-100):idx_do+2000] if idx_do >= 0 else "NO SE ENCONTRO >DO<"

        return {
            "html_size": len(html),
            "notice_uids_encontrados": list(set(uids))[:10],
            "hrefs_opportunity_detail": hrefs[:5],
            "hrefs_notice_reference": refs[:5],
            "zona_primera_fila_DO": zona_tabla,
            "html_inicio": html[:2000],
        }
    except Exception as e:
        import traceback
        return {"error": str(e), "traceback": traceback.format_exc()}


@app.post("/api/webhook/analizar-pliego")
async def webhook_analisis_pliego(request: Request, background_tasks: BackgroundTasks):
    """Endpoint llamado por Supabase (Webhook) al insertar un nuevo seguimiento"""
    payload = await request.json()
    
    registro = payload.get("record", {})
    proceso_id = registro.get("proceso_codigo")
    
    if proceso_id:
        async def _analizar_con_semaforo():
            # Esperar turno — máximo 5 análisis simultáneos
            async with _semaforo_analisis:
                # Ejecutar en threadpool para no bloquear el event loop
                # (ejecutar_analisis_gemini es síncrona y hace I/O pesado)
                loop = asyncio.get_event_loop()
                await asyncio.wait_for(loop.run_in_executor(_executor_gemini, ejecutar_analisis_gemini, proceso_id), timeout=600)

        background_tasks.add_task(_analizar_con_semaforo)
        return {"status": "success", "message": f"IA encolada para {proceso_id}"}
    
    return {"status": "error", "message": "Payload inválido o sin proceso_codigo"}


# ══════════════════════════════════════════════════
# MERCADO DE PROVEEDORES
# ══════════════════════════════════════════════════

@app.get("/api/mercado/proveedores")
def listar_proveedores(
    tipo:      str = Query(None),
    zona:      str = Query(None),
    busqueda:  str = Query(None),
    page:      int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=50),
):
    """Listar proveedores activos con filtros. Contacto de mano_obra/servicios/subcontratista
    se devuelve siempre — el control de acceso lo hace el frontend según suscripción."""
    try:
        q = supabase_admin.table("proveedores").select(
            "id, nombre_empresa, tipo, descripcion, zona_geografica, "
            "telefono, whatsapp, email_contacto, verificado, created_at, "
            "proveedor_categorias(categoria_nombre, unspsc_codigo)"
        ).eq("activo", True)

        if tipo:
            q = q.eq("tipo", tipo)
        if zona:
            q = q.contains("zona_geografica", [zona])
        if busqueda:
            q = q.ilike("nombre_empresa", f"%{busqueda}%")

        offset = (page - 1) * page_size
        q = q.order("created_at", desc=True).range(offset, offset + page_size - 1)

        res = q.execute()
        return {"proveedores": res.data, "page": page, "page_size": page_size}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/mercado/proveedores/{proveedor_id}")
def detalle_proveedor(proveedor_id: str):
    """Perfil completo de un proveedor."""
    try:
        res = supabase_admin.table("proveedores").select(
            "*, proveedor_categorias(*)"
        ).eq("id", proveedor_id).eq("activo", True).single().execute()
        if not res.data:
            raise HTTPException(status_code=404, detail="Proveedor no encontrado")
        return res.data
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/mercado/proveedores")
async def registrar_proveedor(request: Request):
    """Registrar un nuevo proveedor. Público — no requiere autenticación."""
    try:
        data = await request.json()

        # Validaciones mínimas
        required = ["nombre_empresa", "tipo", "telefono"]
        for field in required:
            if not data.get(field):
                raise HTTPException(status_code=400, detail=f"Campo requerido: {field}")

        tipos_validos = ["materiales", "equipos", "mano_obra", "servicios", "subcontratista"]
        if data["tipo"] not in tipos_validos:
            raise HTTPException(status_code=400, detail="Tipo de proveedor inválido")

        categorias = data.pop("categorias", [])

        proveedor_data = {
            "nombre_empresa":  data.get("nombre_empresa"),
            "rnc":             data.get("rnc"),
            "tipo":            data.get("tipo"),
            "descripcion":     data.get("descripcion"),
            "zona_geografica": data.get("zona_geografica", []),
            "telefono":        data.get("telefono"),
            "whatsapp":        data.get("whatsapp") or data.get("telefono"),
            "email_contacto":  data.get("email_contacto"),
        }

        res = supabase_admin.table("proveedores").insert(proveedor_data).execute()
        proveedor_id = res.data[0]["id"]

        if categorias:
            cats = [{"proveedor_id": proveedor_id, "categoria_nombre": c} for c in categorias if c]
            supabase_admin.table("proveedor_categorias").insert(cats).execute()

        return {"ok": True, "id": proveedor_id}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/mercado/contactos")
async def registrar_contacto(request: Request):
    """Registrar que un usuario contactó a un proveedor (analytics)."""
    try:
        data = await request.json()
        proveedor_id  = data.get("proveedor_id")
        tipo_contacto = data.get("tipo_contacto", "whatsapp")
        proceso_codigo = data.get("proceso_codigo")

        # Obtener user_id del header Authorization si existe
        auth_header = request.headers.get("Authorization", "")
        user_id = None
        if auth_header.startswith("Bearer "):
            token = auth_header.split(" ")[1]
            try:
                user_res = supabase.auth.get_user(token)
                user_id = user_res.user.id if user_res.user else None
            except:
                pass

        supabase_admin.table("proveedor_contactos").insert({
            "proveedor_id":     proveedor_id,
            "buscador_user_id": user_id,
            "proceso_codigo":   proceso_codigo,
            "tipo_contacto":    tipo_contacto,
        }).execute()

        return {"ok": True}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/mercado/stats")
def stats_mercado():
    """Conteos por tipo para los chips del Mercado."""
    try:
        tipos = ["materiales", "equipos", "mano_obra", "servicios", "subcontratista"]
        resultado = {}
        for t in tipos:
            r = supabase_admin.table("proveedores").select("id", count="exact").eq("activo", True).eq("tipo", t).execute()
            resultado[t] = r.count or 0
        resultado["total"] = sum(resultado.values())
        return resultado
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# --- NUEVA FUNCIONALIDAD PARA N8N ---

class ComparacionSchema(BaseModel):
    requeridos: List[str]
    encontrados: List[str]

@app.post("/api/v1/generar-oferta")
async def comparar_documentos(datos: ComparacionSchema):
    # Comparamos lo que pide la licitación vs lo que hay en Drive
    faltantes = [doc for doc in datos.requeridos if doc not in datos.encontrados]
    
    return {
        "status": "success",
        "faltantes": faltantes,
        "conteo": {
            "requeridos": len(datos.requeridos),
            "encontrados": len(datos.encontrados),
            "faltantes": len(faltantes)
        }
    }


# ══════════════════════════════════════════════════
# PDF — ANÁLISIS DE PLIEGO (Gemini)
# ══════════════════════════════════════════════════

# Paleta PDF — definida como strings, convertida dentro de las funciones


def _pdf_styles():
    from reportlab.lib.colors import HexColor, white, black
    from reportlab.lib.units import inch
    from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
    from reportlab.lib.enums import TA_CENTER, TA_JUSTIFY
    from reportlab.platypus import Paragraph, Table, TableStyle
    base = getSampleStyleSheet()
    return {
        "tit_header": ParagraphStyle("tit_header", fontSize=20, textColor=white,
            alignment=TA_CENTER, fontName="Helvetica-Bold", spaceAfter=2, leading=24),
        "sub_header": ParagraphStyle("sub_header", fontSize=10, textColor=HexColor("#C8E6C9"),
            alignment=TA_CENTER, fontName="Helvetica", leading=14),
        "seccion": ParagraphStyle("seccion", fontSize=11, textColor=white,
            fontName="Helvetica-Bold", leading=16),
        "body": ParagraphStyle("body", fontSize=9.5, textColor=HexColor("#212121"),
            fontName="Helvetica", leading=14, alignment=TA_JUSTIFY, spaceAfter=3),
        "label": ParagraphStyle("label", fontSize=8, textColor=HexColor("#546E7A"),
            fontName="Helvetica-Bold", leading=11),
        "valor": ParagraphStyle("valor", fontSize=9.5, textColor=HexColor("#212121"),
            fontName="Helvetica", leading=13),
        "item_alerta": ParagraphStyle("item_alerta", fontSize=9, textColor=HexColor("#C62828"),
            fontName="Helvetica-Bold", leading=13, leftIndent=4),
        "item_warn": ParagraphStyle("item_warn", fontSize=9, textColor=HexColor("#E65100"),
            fontName="Helvetica-Bold", leading=13, leftIndent=4),
        "item_ok": ParagraphStyle("item_ok", fontSize=9, textColor=HexColor("#1B5E20"),
            fontName="Helvetica", leading=13, leftIndent=4),
        "item_normal": ParagraphStyle("item_normal", fontSize=9, textColor=HexColor("#212121"),
            fontName="Helvetica", leading=13, leftIndent=4),
        "pie": ParagraphStyle("pie", fontSize=7.5, textColor=HexColor("#90A4AE"),
            fontName="Helvetica", alignment=TA_CENTER, leading=10),
    }


def _pdf_seccion_bar(texto, story, styles, color=None):
    from reportlab.lib.colors import HexColor, white, black
    from reportlab.lib.units import inch
    from reportlab.lib.styles import ParagraphStyle
    from reportlab.platypus import Paragraph, Spacer, Table, TableStyle
    color = color or HexColor("#2E7D32")
    t = Table([[Paragraph(texto, styles["seccion"])]], colWidths=[6.5 * inch])
    t.setStyle(TableStyle([
        ("BACKGROUND",    (0,0),(-1,-1), color),
        ("TOPPADDING",    (0,0),(-1,-1), 7),
        ("BOTTOMPADDING", (0,0),(-1,-1), 7),
        ("LEFTPADDING",   (0,0),(-1,-1), 12),
        ("RIGHTPADDING",  (0,0),(-1,-1), 12),
    ]))
    story.append(t)
    story.append(Spacer(1, 6))


def _pdf_tarjeta(rows, story, styles, fondo=None):
    from reportlab.lib.colors import HexColor, white, black
    from reportlab.lib.units import inch
    from reportlab.lib.styles import ParagraphStyle
    from reportlab.platypus import Paragraph, Spacer, Table, TableStyle
    fondo = fondo or HexColor("#ECEFF1")
    data = [[Paragraph(lbl, styles["label"]), Paragraph(str(val or "—"), styles["valor"])]
            for lbl, val in rows]
    t = Table(data, colWidths=[1.6*inch, 4.9*inch])
    t.setStyle(TableStyle([
        ("BACKGROUND",    (0,0),(-1,-1), fondo),
        ("ROWBACKGROUNDS",(0,0),(-1,-1), [fondo, white]),
        ("TOPPADDING",    (0,0),(-1,-1), 5),
        ("BOTTOMPADDING", (0,0),(-1,-1), 5),
        ("LEFTPADDING",   (0,0),(-1,-1), 10),
        ("RIGHTPADDING",  (0,0),(-1,-1), 10),
        ("LINEBELOW",     (0,0),(-1,-2), 0.3, HexColor("#90A4AE")),
        ("VALIGN",        (0,0),(-1,-1), "TOP"),
    ]))
    story.append(t)
    story.append(Spacer(1, 8))


def _pdf_lista(items, story, styles, tipo="normal"):
    from reportlab.lib.colors import HexColor, white, black
    from reportlab.lib.units import inch
    from reportlab.lib.styles import ParagraphStyle
    from reportlab.platypus import Paragraph, Spacer, Table, TableStyle
    iconos = {
        "alerta": ("🔴 ", styles["item_alerta"]),
        "warn":   ("⚠️ ", styles["item_warn"]),
        "ok":     ("✅ ", styles["item_ok"]),
        "normal": ("• ",  styles["item_normal"]),
    }
    icono_txt, estilo = iconos.get(tipo, iconos["normal"])
    data = []
    for item in items:
        if isinstance(item, dict):
            txt   = item.get("descripcion") or item.get("documento") or item.get("texto") or str(item)
            nota  = item.get("nota") or item.get("detalle") or ""
            sub   = item.get("es_subsanable")
            s_tag = (" [Subsanable]" if sub is True else " [No subsanable]" if sub is False else "")
            linea = f"{icono_txt}{txt}{s_tag}"
            if nota:
                linea += f"\n   {nota}"
        else:
            linea = f"{icono_txt}{item}"
        data.append([Paragraph(linea, estilo)])
    if not data:
        return
    t = Table(data, colWidths=[6.5*inch])
    t.setStyle(TableStyle([
        ("TOPPADDING",    (0,0),(-1,-1), 3),
        ("BOTTOMPADDING", (0,0),(-1,-1), 3),
        ("LEFTPADDING",   (0,0),(-1,-1), 8),
        ("RIGHTPADDING",  (0,0),(-1,-1), 8),
        ("ROWBACKGROUNDS",(0,0),(-1,-1), [white, HexColor("#F9FBE7")]),
    ]))
    story.append(t)
    story.append(Spacer(1, 6))


def _pdf_fmt_monto(m):
    try:
        return f"RD$ {float(m):,.2f}" if m else "—"
    except:
        return str(m)


def _pdf_fmt_fecha(f):
    if not f:
        return "—"
    try:
        return datetime.fromisoformat(str(f).replace("Z","")).strftime("%d/%m/%Y %H:%M")
    except:
        return str(f)[:10]


def _generar_pdf_analisis_bytes(proceso: dict, analisis: dict) -> bytes:
    # Imports lazy — reportlab solo se carga al generar un PDF
    from reportlab.lib.pagesizes import letter
    from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
    from reportlab.lib.colors import HexColor, white, black
    from reportlab.lib.units import inch
    from reportlab.lib.enums import TA_CENTER, TA_JUSTIFY
    from reportlab.platypus import (
        SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, HRFlowable
    )

    buf    = io.BytesIO()
    styles = _pdf_styles()
    doc    = SimpleDocTemplate(buf, pagesize=letter,
                leftMargin=0.65*inch, rightMargin=0.65*inch,
                topMargin=0.6*inch,  bottomMargin=0.7*inch,
                title=f"Análisis — {proceso.get('codigo_proceso','')}", author="LicitacionLab")
    story  = []

    # Encabezado
    header_t = Table([
        [Paragraph("ANÁLISIS DE PLIEGO", styles["tit_header"])],
        [Paragraph(f"{proceso.get('codigo_proceso','')} · {proceso.get('unidad_compra','')}", styles["sub_header"])],
        [Paragraph(f"Generado el {datetime.now().strftime('%d/%m/%Y %H:%M')} · LicitacionLab", styles["sub_header"])],
    ], colWidths=[6.5*inch])
    header_t.setStyle(TableStyle([
        ("BACKGROUND",    (0,0),(-1,-1), HexColor("#1A5C2A")),
        ("TOPPADDING",    (0,0),(0,0),   16),
        ("BOTTOMPADDING", (0,2),(0,2),   14),
        ("TOPPADDING",    (0,1),(0,2),   3),
        ("LEFTPADDING",   (0,0),(-1,-1), 16),
        ("RIGHTPADDING",  (0,0),(-1,-1), 16),
    ]))
    story.append(header_t)
    story.append(Spacer(1, 12))

    # Datos del proceso
    _pdf_seccion_bar("📋  INFORMACIÓN DEL PROCESO", story, styles)
    _pdf_tarjeta([
        ("TÍTULO",         proceso.get("titulo")),
        ("INSTITUCIÓN",    proceso.get("unidad_compra")),
        ("CÓDIGO",         proceso.get("codigo_proceso")),
        ("MODALIDAD",      proceso.get("modalidad")),
        ("TIPO",           proceso.get("objeto_proceso")),
        ("MONTO ESTIMADO", _pdf_fmt_monto(proceso.get("monto_estimado"))),
        ("PUBLICACIÓN",    _pdf_fmt_fecha(proceso.get("fecha_publicacion"))),
        ("CIERRE OFERTAS", _pdf_fmt_fecha(proceso.get("fecha_fin_recepcion_ofertas"))),
        ("APERTURA",       _pdf_fmt_fecha(proceso.get("fecha_apertura_ofertas"))),
        ("DURACIÓN",       proceso.get("duracion_contrato")),
    ], story, styles, fondo=HexColor("#E8F5E9"))

    # Resumen ejecutivo
    if analisis.get("resumen_ejecutivo"):
        _pdf_seccion_bar("📄  RESUMEN EJECUTIVO", story, styles)
        story.append(Paragraph(analisis["resumen_ejecutivo"], styles["body"]))
        story.append(Spacer(1, 8))

    # Alertas de riesgo
    if analisis.get("alertas_fraude"):
        _pdf_seccion_bar("🚨  ALERTAS DE RIESGO", story, styles, color=HexColor("#C62828"))
        _pdf_lista(analisis["alertas_fraude"], story, styles, tipo="alerta")

    # Evaluación de competitividad
    comp = analisis.get("evaluacion_competitividad") or {}
    if comp:
        _pdf_seccion_bar("📊  EVALUACIÓN DE COMPETITIVIDAD", story, styles, color=HexColor("#1565C0"))
        rows = []
        if comp.get("nivel"):        rows.append(("NIVEL",         comp["nivel"]))
        if comp.get("explicacion"):  rows.append(("ANÁLISIS",      comp["explicacion"]))
        if comp.get("recomendacion"):rows.append(("RECOMENDACIÓN", comp["recomendacion"]))
        if rows:
            _pdf_tarjeta(rows, story, styles, fondo=HexColor("#E3F2FD"))

    # Restricciones
    if analisis.get("restricciones_participacion"):
        _pdf_seccion_bar("🚫  RESTRICCIONES DE PARTICIPACIÓN", story, styles, color=HexColor("#6A1B9A"))
        _pdf_lista(analisis["restricciones_participacion"], story, styles, tipo="warn")

    # Plazos clave
    if analisis.get("plazos_clave"):
        _pdf_seccion_bar("📅  PLAZOS CLAVE", story, styles, color=HexColor("#00695C"))
        _pdf_lista(analisis["plazos_clave"], story, styles, tipo="normal")

    # Requisitos de experiencia
    if analisis.get("requisitos_experiencia"):
        _pdf_seccion_bar("🏆  REQUISITOS DE EXPERIENCIA", story, styles)
        _pdf_lista(analisis["requisitos_experiencia"], story, styles, tipo="normal")

    # Requisitos financieros
    if analisis.get("requisitos_financieros"):
        _pdf_seccion_bar("💰  REQUISITOS FINANCIEROS", story, styles)
        _pdf_lista(analisis["requisitos_financieros"], story, styles, tipo="normal")

    # Garantías exigidas
    if analisis.get("garantias_exigidas"):
        _pdf_seccion_bar("🔒  GARANTÍAS EXIGIDAS", story, styles)
        _pdf_lista(analisis["garantias_exigidas"], story, styles, tipo="normal")

    # Personal y equipos
    if analisis.get("personal_y_equipos"):
        _pdf_seccion_bar("👷  PERSONAL Y EQUIPOS REQUERIDOS", story, styles)
        _pdf_lista(analisis["personal_y_equipos"], story, styles, tipo="normal")

    # Checklist de documentos
    checklist = analisis.get("checklist_categorizado") or {}
    secciones_cl = [
        ("Legal",      checklist.get("legal",      [])),
        ("Técnica",    checklist.get("tecnica",     [])),
        ("Financiera", checklist.get("financiera",  [])),
    ]
    if any(items for _, items in secciones_cl):
        _pdf_seccion_bar("📋  CHECKLIST DE DOCUMENTOS", story, styles)
        for nombre_sec, items in secciones_cl:
            if not items:
                continue
            sub_t = Table([[Paragraph(f"  {nombre_sec}", ParagraphStyle(
                "sub_cl", fontSize=9.5, textColor=HexColor("#1A5C2A"), fontName="Helvetica-Bold"
            ))]], colWidths=[6.5*inch])
            sub_t.setStyle(TableStyle([
                ("BACKGROUND",    (0,0),(-1,-1), HexColor("#E8F5E9")),
                ("TOPPADDING",    (0,0),(-1,-1), 4),
                ("BOTTOMPADDING", (0,0),(-1,-1), 4),
                ("LEFTPADDING",   (0,0),(-1,-1), 8),
            ]))
            story.append(sub_t)
            cl_data = []
            for item in items:
                doc_n = item.get("documento", str(item)) if isinstance(item, dict) else str(item)
                nota  = item.get("nota","")          if isinstance(item, dict) else ""
                sub   = item.get("es_subsanable")    if isinstance(item, dict) else None
                s_tag = (" [Subsanable]" if sub is True else " [No subsanable]" if sub is False else "")
                linea = f"☐  {doc_n}{s_tag}"
                if nota:
                    linea += f"\n     {nota}"
                cl_data.append([Paragraph(linea, styles["item_normal"])])
            cl_t = Table(cl_data, colWidths=[6.5*inch])
            cl_t.setStyle(TableStyle([
                ("TOPPADDING",    (0,0),(-1,-1), 4),
                ("BOTTOMPADDING", (0,0),(-1,-1), 4),
                ("LEFTPADDING",   (0,0),(-1,-1), 12),
                ("ROWBACKGROUNDS",(0,0),(-1,-1), [white, HexColor("#ECEFF1")]),
                ("LINEBELOW",     (0,0),(-1,-2), 0.3, HexColor("#90A4AE")),
            ]))
            story.append(cl_t)
            story.append(Spacer(1, 6))

    # Pie
    story.append(Spacer(1, 16))
    story.append(HRFlowable(width="100%", thickness=0.5, color=HexColor("#90A4AE")))
    story.append(Spacer(1, 4))
    story.append(Paragraph(
        f"Generado por LicitacionLab · app.licitacionlab.com · "
        f"Análisis con Inteligencia Artificial · {datetime.now().strftime('%d/%m/%Y')}",
        styles["pie"]
    ))
    story.append(Paragraph(
        "Este análisis es orientativo. Verifica siempre el pliego oficial en el portal del DGCP.",
        styles["pie"]
    ))

    doc.build(story)
    buf.seek(0)
    return buf.read()


@app.get("/analisis/{proceso_id}/pdf")
async def descargar_pdf_analisis(proceso_id: str):
    """Descarga el PDF del análisis de pliego generado por Gemini/IA."""
    # Buscar análisis completado
    res_a = supabase_admin.table("analisis_pliego") \
        .select("*") \
        .eq("proceso_id", proceso_id) \
        .eq("estado", "completado") \
        .order("creado_en", desc=True) \
        .limit(1) \
        .execute()

    if not res_a.data:
        raise HTTPException(status_code=404,
            detail="Análisis no encontrado o aún no completado para este proceso")

    analisis = res_a.data[0]

    # Datos del proceso
    res_p = supabase_admin.table("procesos") \
        .select("codigo_proceso, titulo, unidad_compra, modalidad, objeto_proceso, "
                "monto_estimado, fecha_publicacion, fecha_fin_recepcion_ofertas, "
                "fecha_apertura_ofertas, duracion_contrato") \
        .eq("codigo_proceso", proceso_id) \
        .limit(1) \
        .execute()

    proceso = res_p.data[0] if res_p.data else {"codigo_proceso": proceso_id}

    try:
        pdf_bytes = _generar_pdf_analisis_bytes(proceso, analisis)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error generando PDF: {str(e)}")

    filename = f"Analisis_{proceso_id.replace('/', '-').replace(' ', '_')}.pdf"
    return StreamingResponse(
        io.BytesIO(pdf_bytes),
        media_type="application/pdf",
        headers={
            "Content-Disposition": f'attachment; filename="{filename}"',
            "Content-Length": str(len(pdf_bytes)),
        }
    )
