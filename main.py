"""
API de Licitaciones RD — Backend FastAPI
=========================================
- Sirve datos de procesos y artículos a la PWA
- Ejecuta el monitor automáticamente cada 10 minutos
"""

import os
import threading
import time
from datetime import datetime
from contextlib import asynccontextmanager
from fastapi.staticfiles import StaticFiles

from fastapi import FastAPI, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv
from supabase import create_client

from monitor import ejecutar_monitor
from notifications import enviar_notificacion

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

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
from fastapi.responses import FileResponse

import os
from fastapi.responses import FileResponse

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

@app.get("/api/sync-status")
def sync_status():
    """
    Estado de sincronización con la API DGCP.
    La DGCP opera sobre un Data Warehouse que se refresca cada ~8 horas,
    por lo que los procesos pueden demorar hasta 8h en aparecer desde su publicación.
    """
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


# NOTA: El endpoint principal /api/procesos está definido más abajo (get_procesos)
# Este bloque fue consolidado para evitar duplicación


@app.get("/api/procesos/{codigo_proceso}")
def detalle_proceso(codigo_proceso: str):
    """Obtiene el detalle de un proceso con sus artículos."""
    try:
        # Obtener proceso
        proc = supabase.table("procesos") \
            .select("*") \
            .eq("codigo_proceso", codigo_proceso) \
            .execute()
        
        if not proc.data:
            raise HTTPException(status_code=404, detail="Proceso no encontrado")
        
        # Obtener artículos del proceso
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
    """Lista los segmentos principales del catálogo UNSPSC (nivel más alto)."""
    try:
        result = supabase.rpc("get_segmentos", {}).execute()
        
        # Si el RPC no existe, hacemos la consulta directa
        if not result.data:
            result = supabase.table("catalogo_unspsc") \
                .select("segmento, descripcion_segmento") \
                .execute()
            
            # Agrupar por segmento (eliminar duplicados)
            segmentos = {}
            for item in result.data:
                seg = item["segmento"]
                if seg not in segmentos:
                    segmentos[seg] = item["descripcion_segmento"]
            
            return {
                "segmentos": [
                    {"codigo": k, "descripcion": v}
                    for k, v in sorted(segmentos.items())
                ]
            }
        
        return {"segmentos": result.data}
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/catalogo/familias/{segmento}")
def listar_familias(segmento: str):
    """Lista las familias dentro de un segmento."""
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
        
        return {
            "familias": [
                {"codigo": k, "descripcion": v}
                for k, v in sorted(familias.items())
            ]
        }
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/catalogo/clases/{familia}")
def listar_clases(familia: str):
    """Lista las clases dentro de una familia."""
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
        
        return {
            "clases": [
                {"codigo": k, "descripcion": v}
                for k, v in sorted(clases.items())
            ]
        }
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/unspsc/buscar")
def buscar_unspsc(q: str = ""):
    if not q or len(q) < 2:
        return []

    q = q.strip()
    resultados = []

    # Si el query es numérico, buscar primero por código de familia exacto o prefijo
    if q.isdigit():
        result_codigo = supabase.table("catalogo_unspsc") \
            .select("familia, descripcion_familia") \
            .like("familia", f"{q}%") \
            .limit(50) \
            .execute()
        resultados.extend(result_codigo.data or [])

    # Siempre también buscar por descripción (texto)
    result_texto = supabase.table("catalogo_unspsc") \
        .select("familia, descripcion_familia") \
        .or_(
            f"descripcion_familia.ilike.%{q}%,"
            f"descripcion_subclase.ilike.%{q}%,"
            f"sinonimos_subclase.ilike.%{q}%"
        ) \
        .limit(50) \
        .execute()
    resultados.extend(result_texto.data or [])

    # Deduplicar por familia, preservando orden
    seen = set()
    unique = []
    for r in resultados:
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
    """
    Busca procesos activos que contengan artículos de los rubros indicados.
    Este es el endpoint clave para las notificaciones filtradas.
    """
    try:
        # Construir filtro de artículos
        art_query = supabase.table("articulos_proceso").select("codigo_proceso")
        
        if clases:
            lista_clases = [c.strip() for c in clases.split(",")]
            art_query = art_query.in_("clase_unspsc", lista_clases)
        elif familias:
            lista_familias = [f.strip() for f in familias.split(",")]
            art_query = art_query.in_("familia_unspsc", lista_familias)
        elif segmentos:
            # Para segmentos, necesitamos buscar familias que empiecen con ese segmento
            lista_segmentos = [s.strip() for s in segmentos.split(",")]
            # Las familias UNSPSC empiezan con el código del segmento
            condiciones = []
            for seg in lista_segmentos:
                condiciones.append(f"familia_unspsc.like.{seg[:2]}%")
            art_query = art_query.or_(",".join(condiciones))
        else:
            raise HTTPException(status_code=400, detail="Debe especificar al menos un filtro de rubro")
        
        art_result = art_query.execute()
        
        # Obtener códigos únicos de procesos
        codigos = list(set(a["codigo_proceso"] for a in art_result.data))
        
        if not codigos:
            return {"procesos": [], "total": 0, "page": page, "limit": limit, "pages": 0}
        
        # Obtener esos procesos (solo activos)
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
    """Estadísticas generales del sistema."""
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
    """El frontend necesita esta clave para suscribirse."""
    return {"publicKey": os.getenv("VAPID_PUBLIC_KEY")}


@app.post("/api/notificaciones/suscribirse")
async def suscribirse(payload: dict):
    """Recibe la suscripción del navegador y la guarda en Supabase."""
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
    """Desactiva una suscripción."""
    endpoint = payload.get("endpoint")
    supabase.table("user_subscriptions")\
        .update({"active": False})\
        .eq("endpoint", endpoint)\
        .execute()
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
    """Actualiza los rubros de interés de una suscripción."""
    endpoint = payload.get("endpoint")
    rubros = payload.get("rubros", [])
    supabase.table("user_subscriptions")\
        .update({"intereses_rubros": rubros})\
        .eq("endpoint", endpoint)\
        .execute()
    return {"ok": True}


@app.post("/api/notificaciones/enviar-prueba")
async def enviar_prueba(payload: dict):
    """Envía notificación de prueba a un endpoint específico."""
    endpoint = payload.get("endpoint")
    try:
        result = supabase.table("user_subscriptions")\
            .select("*")\
            .eq("endpoint", endpoint)\
            .single()\
            .execute()
        
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



@app.get("/api/stats")
def get_stats():
    ahora = datetime.now().isoformat()
    total   = supabase.table("procesos").select("*", count="exact").execute()
    activos = supabase.table("procesos").select("*", count="exact").gte("fecha_fin_recepcion_ofertas", ahora).execute()
    obras   = supabase.table("procesos").select("*", count="exact").gte("fecha_fin_recepcion_ofertas", ahora).eq("objeto_proceso", "Obras").execute()
    bienes  = supabase.table("procesos").select("*", count="exact").gte("fecha_fin_recepcion_ofertas", ahora).eq("objeto_proceso", "Bienes").execute()
    servicios = supabase.table("procesos").select("*", count="exact").gte("fecha_fin_recepcion_ofertas", ahora).eq("objeto_proceso", "Servicios").execute()
    arts    = supabase.table("articulos_proceso").select("*", count="exact").execute()
    return {
        "total_procesos":    total.count,
        "procesos_activos":  activos.count,
        "obras_activas":     obras.count,
        "bienes_activos":    bienes.count,
        "servicios_activos": servicios.count,
        "total_articulos":   arts.count
    }

@app.get("/api/unspsc/buscar")
def buscar_unspsc(q: str = ""):
    if not q or len(q) < 2:
        return []
    result = supabase.table("catalogo_unspsc")        .select("familia, descripcion_familia")        .ilike("descripcion_familia", f"%{q}%")        .limit(15)        .execute()
    # Deduplicar por familia
    seen = set()
    unique = []
    for r in (result.data or []):
        if r["familia"] not in seen:
            seen.add(r["familia"])
            unique.append(r)
    return unique

@app.get("/api/procesos")
def get_procesos(page: int = 1, limit: int = 15, busqueda: str = "", objeto: str = "",
                 solo_activos: bool = True, institucion: str = "",
                 monto_min: float = None, monto_max: float = None,
                 familia_unspsc: str = ""):
    """
    Lista procesos con filtros combinados.
    - busqueda: busca en título, descripción, código y unidad_compra
    - institucion: busca en unidad_compra (nombre de la institución)
    - familia_unspsc: filtra por código de familia UNSPSC (ej: '72' busca 72XXXX)
    - solo_activos: filtra por estado_proceso = 'Publicado'
    """
    try:
        # ── Si hay filtro UNSPSC, primero obtener los códigos de proceso ──
        codigos_unspsc = None
        if familia_unspsc.strip():
            # Soporte para código parcial: "72" debe encontrar "72101500", "72102000", etc.
            familia_limpia = familia_unspsc.strip()
            arts = supabase.table("articulos_proceso") \
                .select("codigo_proceso") \
                .like("familia_unspsc", f"{familia_limpia}%") \
                .execute()
            codigos_unspsc = list(set(a["codigo_proceso"] for a in (arts.data or [])))
            if not codigos_unspsc:
                return {"procesos": [], "total": 0, "page": page, "pages": 1}

        # ── Base query ──
        if codigos_unspsc is not None:
            query = supabase.table("procesos").select("*", count="exact") \
                .in_("codigo_proceso", codigos_unspsc[:500])
        else:
            query = supabase.table("procesos").select("*", count="exact")

        # ── Filtros ──
        if solo_activos:
            # Activo = fecha de cierre de ofertas no ha pasado todavía
            query = query.gte("fecha_fin_recepcion_ofertas", datetime.now().isoformat())

        if busqueda.strip():
            # Busca en título, descripción, código Y nombre de institución
            q = busqueda.strip()
            query = query.or_(
                f"titulo.ilike.%{q}%,"
                f"descripcion.ilike.%{q}%,"
                f"codigo_proceso.ilike.%{q}%,"
                f"unidad_compra.ilike.%{q}%"
            )

        if objeto.strip():
            query = query.eq("objeto_proceso", objeto.strip())

        if institucion.strip():
            query = query.ilike("unidad_compra", f"%{institucion.strip()}%")

        if monto_min is not None:
            query = query.gte("monto_estimado", monto_min)

        if monto_max is not None:
            query = query.lte("monto_estimado", monto_max)

        # ── Paginación ──
        offset = (page - 1) * limit
        result = query.order("fecha_publicacion", desc=True) \
            .range(offset, offset + limit - 1) \
            .execute()

        total = result.count or 0
        return {
            "procesos": result.data,
            "total": total,
            "page": page,
            "pages": max(1, -(-total // limit))
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/procesos/{codigo}")
def get_proceso_detalle(codigo: str):
    proceso = supabase.table("procesos").select("*").eq("codigo_proceso", codigo).single().execute()
    articulos = supabase.table("articulos_proceso").select("*").eq("codigo_proceso", codigo).execute()
    return {"proceso": proceso.data, "articulos": articulos.data}

@app.post("/api/admin/forzar-monitor")
async def forzar_monitor_admin():
    from monitor import obtener_todas_las_paginas, guardar_procesos_nuevos, procesar_articulos_de_nuevos, notificar_procesos_nuevos
    procesos = obtener_todas_las_paginas(fecha_desde="2026-02-20", fecha_hasta="2026-02-24")
    nuevos = guardar_procesos_nuevos(procesos)
    if nuevos:
        procesar_articulos_de_nuevos(nuevos)
        notificar_procesos_nuevos(nuevos)
    return {"nuevos": len(nuevos)}
