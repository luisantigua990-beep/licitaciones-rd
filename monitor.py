"""
Monitor de Procesos de Compras Públicas - República Dominicana
==============================================================
Módulo que consulta la API de la DGCP, detecta procesos nuevos,
y guarda procesos + artículos (con códigos UNSPSC) en Supabase.
"""

import os
import time
import requests
from datetime import datetime, timedelta
from dotenv import load_dotenv
from supabase import create_client
from notifications import enviar_notificacion
load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
API_BASE_URL = "https://datosabiertos.dgcp.gob.do/api-dgcp/v1"

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)


# ============================================
# FUNCIONES PARA PROCESOS
# ============================================

def obtener_procesos_api(fecha_desde=None, fecha_hasta=None, page=1, limit=1000):
    if fecha_desde is None:
        fecha_desde = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    if fecha_hasta is None:
        fecha_hasta = datetime.now().strftime("%Y-%m-%d")

    params = {"startdate": fecha_desde, "enddate": fecha_hasta, "page": page, "limit": limit}

    try:
        response = requests.get(f"{API_BASE_URL}/procesos", params=params, timeout=30)
        response.raise_for_status()
        data = response.json()

        if data.get("hasError"):
            return []

        payload = data.get("payload")
        if payload is None:
            return []

        if isinstance(payload, list):
            procesos = payload
        elif isinstance(payload, dict):
            procesos = payload.get("content", []) or []
        else:
            procesos = []

        print(f"📥 API: {len(procesos)} procesos (pág {page})")
        return procesos

    except requests.exceptions.RequestException as e:
        print(f"❌ Error API: {e}")
        return []


def obtener_todas_las_paginas(fecha_desde=None, fecha_hasta=None):
    todos = []
    page = 1
    while True:
        procesos = obtener_procesos_api(fecha_desde, fecha_hasta, page=page, limit=1000)
        if not procesos:
            break
        todos.extend(procesos)
        if len(procesos) < 1000:
            break
        page += 1
    return todos


def obtener_codigos_existentes(codigos):
    if not codigos:
        return set()
    try:
        existentes = set()
        for i in range(0, len(codigos), 50):
            bloque = codigos[i:i+50]
            result = supabase.table("procesos").select("codigo_proceso").in_("codigo_proceso", bloque).execute()
            existentes.update(r["codigo_proceso"] for r in result.data)
        return existentes
    except Exception as e:
        print(f"❌ Error Supabase: {e}")
        return set()


def guardar_procesos_nuevos(procesos):
    if not procesos:
        return []

    codigos = [p["codigo_proceso"] for p in procesos]
    existentes = obtener_codigos_existentes(codigos)
    nuevos = [p for p in procesos if p["codigo_proceso"] not in existentes]

    if not nuevos:
        print("ℹ️  Sin procesos nuevos")
        return []

    registros = []
    for p in nuevos:
        registros.append({
            "codigo_proceso": p.get("codigo_proceso"),
            "codigo_unidad_compra": p.get("codigo_unidad_compra"),
            "unidad_compra": p.get("unidad_compra"),
            "modalidad": p.get("modalidad"),
            "tipo_excepcion": p.get("tipo_excepcion"),
            "titulo": p.get("titulo", "").strip() if p.get("titulo") else None,
            "descripcion": p.get("descripcion", "").strip() if p.get("descripcion") else None,
            "estado_proceso": p.get("estado_proceso"),
            "divisa": p.get("divisa", "DOP"),
            "monto_estimado": p.get("monto_estimado"),
            "fecha_publicacion": p.get("fecha_publicacion"),
            "fecha_enmienda": p.get("fecha_enmienda"),
            "fecha_fin_recepcion_ofertas": p.get("fecha_fin_recepcion_ofertas"),
            "fecha_apertura_ofertas": p.get("fecha_apertura_ofertas"),
            "fecha_estimada_adjudicacion": p.get("fecha_estimada_adjudicacion"),
            "fecha_suscripcion": p.get("fecha_suscripcion"),
            "dirigido_mipymes": p.get("dirigido_mipymes"),
            "dirigido_mipymes_mujeres": p.get("dirigido_mipymes_mujeres"),
            "objeto_proceso": p.get("objeto_proceso"),
            "subobjeto_proceso": p.get("subobjeto_proceso"),
            "url": p.get("url"),
            "duracion_contrato": p.get("duracion_contrato"),
            "proceso_lotificado": p.get("proceso_lotificado"),
            "compra_verde": p.get("compra_verde"),
            "compra_conjunta": p.get("compra_conjunta"),
            "notificado": False
        })

    try:
        insertados = 0
        for i in range(0, len(registros), 100):
            bloque = registros[i:i+100]
            result = supabase.table("procesos").insert(bloque).execute()
            insertados += len(result.data)
        print(f"✅ {insertados} procesos nuevos guardados")
        return nuevos
    except Exception as e:
        print(f"❌ Error guardando procesos: {e}")
        return []


# ============================================
# FUNCIONES PARA ARTÍCULOS
# ============================================

def obtener_articulos_proceso(codigo_proceso):
    try:
        response = requests.get(
            f"{API_BASE_URL}/procesos/articulos",
            params={"proceso": codigo_proceso, "limit": 1000},
            timeout=30
        )
        response.raise_for_status()
        data = response.json()
        payload = data.get("payload")
        if payload is None:
            return []
        if isinstance(payload, list):
            return payload
        elif isinstance(payload, dict):
            return payload.get("content", []) or []
        return []
    except Exception:
        return []


def guardar_articulos(codigo_proceso, articulos):
    if not articulos:
        return 0
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
        insertados = 0
        for i in range(0, len(registros), 100):
            bloque = registros[i:i+100]
            result = supabase.table("articulos_proceso").insert(bloque).execute()
            insertados += len(result.data)
        return insertados
    except Exception:
        return 0


def procesar_articulos_de_nuevos(procesos_nuevos):
    if not procesos_nuevos:
        return
    total_articulos = 0
    for i, proceso in enumerate(procesos_nuevos):
        articulos = obtener_articulos_proceso(proceso["codigo_proceso"])
        if articulos:
            total_articulos += guardar_articulos(proceso["codigo_proceso"], articulos)
        time.sleep(0.2)
    print(f"✅ {total_articulos} artículos guardados")


# ============================================
# FUNCIÓN PRINCIPAL
# ============================================
def notificar_procesos_nuevos(procesos_nuevos):
    if not procesos_nuevos:
        return
    
    try:
        # Obtener todas las suscripciones activas
        result = supabase.table("user_subscriptions")\
            .select("*")\
            .eq("active", True)\
            .execute()
        
        suscripciones = result.data
        if not suscripciones:
            print("ℹ️  Sin suscriptores activos")
            return
        
        notificaciones_enviadas = 0
        
        for proceso in procesos_nuevos:
            titulo_proceso = proceso.get("titulo", "Nueva licitación")[:60]
            entidad = proceso.get("unidad_compra", "")
            monto = proceso.get("monto_estimado")
            monto_str = f" | RD${monto:,.0f}" if monto else ""
            codigo = proceso.get("codigo_proceso", "")
            
            for sub in suscripciones:
                subscription_info = {
                    "endpoint": sub["endpoint"],
                    "keys": {
                        "auth": sub["auth"],
                        "p256dh": sub["p256dh"]
                    }
                }
                
                ok = enviar_notificacion(
                    subscription_info,
                    titulo=f"🏗️ {entidad}{monto_str}",
                    cuerpo=titulo_proceso,
                    url=f"/proceso/{codigo}"
                )
                
                if ok:
                    notificaciones_enviadas += 1
        
        print(f"🔔 {notificaciones_enviadas} notificaciones enviadas")
    
    except Exception as e:
        print(f"❌ Error enviando notificaciones: {e}")

def ejecutar_monitor():
    print(f"\n{'='*50}")
    print(f"🔍 Monitor | {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*50}")

    procesos = obtener_todas_las_paginas()
    if not procesos:
        print("📭 Sin procesos en el período")
        return []

    print(f"📊 {len(procesos)} procesos encontrados")
    nuevos = guardar_procesos_nuevos(procesos)

   if nuevos:
        procesar_articulos_de_nuevos(nuevos)
        notificar_procesos_nuevos(nuevos)
        for p in nuevos[:5]:
            monto = f"RD${p.get('monto_estimado', 0):,.2f}" if p.get('monto_estimado') else "N/A"
            print(f"   🆕 {p['titulo'][:50]}... | {monto}")
        if len(nuevos) > 5:
            print(f"   ... y {len(nuevos) - 5} más")

    print(f"✅ Monitor completado\n")
    return nuevos 


if __name__ == "__main__":
    ejecutar_monitor()
