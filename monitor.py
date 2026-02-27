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

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
API_BASE_URL = "https://datosabiertos.dgcp.gob.do/api-dgcp/v1"

# Ciclo de refresco del Data Warehouse de la DGCP (cada 8 horas)
CICLO_HORAS_DGCP = 8

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# ============================================
# CRON LOG
# ============================================

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
        print(f"⚠️  No se pudo registrar cron_log: {e}")



# ============================================
# TRACKING DE SINCRONIZACIÓN
# ============================================

def registrar_sync_exitosa(procesos_encontrados: int, procesos_nuevos: int):
    """Guarda el timestamp de la última sincronización exitosa con la API DGCP."""
    ahora = datetime.utcnow().isoformat()
    try:
        supabase.table("system_config").upsert({
            "clave": "ultima_sync_dgcp",
            "valor": ahora,
            "meta": {
                "procesos_encontrados": procesos_encontrados,
                "procesos_nuevos": procesos_nuevos,
                "ciclo_horas": CICLO_HORAS_DGCP
            }
        }, on_conflict="clave").execute()
        print(f"📡 Sync registrada: {ahora}")
    except Exception as e:
        print(f"⚠️  No se pudo registrar sync (tabla system_config puede no existir): {e}")


def obtener_estado_sync():
    """
    Devuelve el estado de sincronización con la API DGCP.
    Retorna: ultima_sync, proxima_sync, minutos_desde_sync, ciclo_horas
    """
    try:
        result = supabase.table("system_config") \
            .select("valor, meta") \
            .eq("clave", "ultima_sync_dgcp") \
            .single() \
            .execute()

        if not result.data:
            return None

        ultima_sync_str = result.data["valor"]
        meta = result.data.get("meta", {})

        ultima_sync = datetime.fromisoformat(ultima_sync_str)
        proxima_sync = ultima_sync + timedelta(hours=CICLO_HORAS_DGCP)
        ahora = datetime.utcnow()
        minutos_desde_sync = int((ahora - ultima_sync).total_seconds() / 60)

        return {
            "ultima_sync": ultima_sync_str,
            "proxima_sync": proxima_sync.isoformat(),
            "minutos_desde_sync": minutos_desde_sync,
            "ciclo_horas": CICLO_HORAS_DGCP,
            "procesos_encontrados_ultimo_ciclo": meta.get("procesos_encontrados", 0),
            "procesos_nuevos_ultimo_ciclo": meta.get("procesos_nuevos", 0),
            "mensaje": f"Datos verificados hace {minutos_desde_sync} min. Próximo ciclo DGCP: {proxima_sync.strftime('%H:%M')} UTC"
        }

    except Exception as e:
        print(f"⚠️  Error obteniendo estado sync: {e}")
        return None


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
    """
    Para cada proceso de la API DGCP:
    - Si NO existe en Supabase → insertar como nuevo
    - Si YA existe (vino del scraper) → actualizar con los campos que faltan
    """
    if not procesos:
        return []

    codigos = [p["codigo_proceso"] for p in procesos]

    # Obtener cuáles ya existen y si están enriquecidos
    try:
        existentes_data = {}
        for i in range(0, len(codigos), 50):
            bloque = codigos[i:i+50]
            result = supabase.table("procesos") \
                .select("codigo_proceso, enriquecido_api") \
                .in_("codigo_proceso", bloque) \
                .execute()
            for r in result.data:
                existentes_data[r["codigo_proceso"]] = r.get("enriquecido_api", False)
    except Exception as e:
        print(f"❌ Error consultando existentes: {e}")
        return []

    nuevos = []
    para_insertar = []
    para_actualizar = []

    for p in procesos:
        codigo = p["codigo_proceso"]
        if codigo not in existentes_data:
            # Proceso nuevo — insertar completo
            para_insertar.append({
                "codigo_proceso":                p.get("codigo_proceso"),
                "codigo_unidad_compra":          p.get("codigo_unidad_compra"),
                "unidad_compra":                 p.get("unidad_compra"),
                "modalidad":                     p.get("modalidad"),
                "tipo_excepcion":                p.get("tipo_excepcion"),
                "titulo":                        p.get("titulo", "").strip() if p.get("titulo") else None,
                "descripcion":                   p.get("descripcion", "").strip() if p.get("descripcion") else None,
                "estado_proceso":                p.get("estado_proceso"),
                "divisa":                        p.get("divisa", "DOP"),
                "monto_estimado":                p.get("monto_estimado"),
                "fecha_publicacion":             p.get("fecha_publicacion"),
                "fecha_enmienda":                p.get("fecha_enmienda"),
                "fecha_fin_recepcion_ofertas":   p.get("fecha_fin_recepcion_ofertas"),
                "fecha_apertura_ofertas":        p.get("fecha_apertura_ofertas"),
                "fecha_estimada_adjudicacion":   p.get("fecha_estimada_adjudicacion"),
                "fecha_suscripcion":             p.get("fecha_suscripcion"),
                "dirigido_mipymes":              p.get("dirigido_mipymes"),
                "dirigido_mipymes_mujeres":      p.get("dirigido_mipymes_mujeres"),
                "objeto_proceso":                p.get("objeto_proceso"),
                "subobjeto_proceso":             p.get("subobjeto_proceso"),
                "url":                           p.get("url"),
                "duracion_contrato":             p.get("duracion_contrato"),
                "proceso_lotificado":            p.get("proceso_lotificado"),
                "compra_verde":                  p.get("compra_verde"),
                "compra_conjunta":               p.get("compra_conjunta"),
                "notificado":                    False,
                "enriquecido_api":               True,
            })
            nuevos.append(p)
        elif not existentes_data[codigo]:
            # Existe pero vino del scraper (enriquecido_api=False) → actualizar
            para_actualizar.append({
                "codigo_proceso":              codigo,
                "objeto_proceso":              p.get("objeto_proceso"),
                "subobjeto_proceso":           p.get("subobjeto_proceso"),
                "modalidad":                   p.get("modalidad"),
                "tipo_excepcion":              p.get("tipo_excepcion"),
                "estado_proceso":              p.get("estado_proceso"),
                "divisa":                      p.get("divisa", "DOP"),
                "fecha_enmienda":              p.get("fecha_enmienda"),
                "fecha_apertura_ofertas":      p.get("fecha_apertura_ofertas"),
                "fecha_estimada_adjudicacion": p.get("fecha_estimada_adjudicacion"),
                "fecha_suscripcion":           p.get("fecha_suscripcion"),
                "dirigido_mipymes":            p.get("dirigido_mipymes"),
                "dirigido_mipymes_mujeres":    p.get("dirigido_mipymes_mujeres"),
                "duracion_contrato":           p.get("duracion_contrato"),
                "proceso_lotificado":          p.get("proceso_lotificado"),
                "compra_verde":                p.get("compra_verde"),
                "compra_conjunta":             p.get("compra_conjunta"),
                "url":                         p.get("url"),
                "enriquecido_api":             True,
            })

    # Insertar nuevos
    if para_insertar:
        try:
            insertados = 0
            for i in range(0, len(para_insertar), 100):
                result = supabase.table("procesos").insert(para_insertar[i:i+100]).execute()
                insertados += len(result.data)
            print(f"✅ {insertados} procesos nuevos insertados")
        except Exception as e:
            print(f"❌ Error insertando procesos: {e}")

    # Actualizar los que vinieron del scraper
    if para_actualizar:
        try:
            actualizados = 0
            for reg in para_actualizar:
                codigo = reg.pop("codigo_proceso")
                supabase.table("procesos").update(reg).eq("codigo_proceso", codigo).execute()
                actualizados += 1
            print(f"🔄 {actualizados} procesos enriquecidos con datos de la API")
        except Exception as e:
            print(f"❌ Error actualizando procesos: {e}")

    if not nuevos:
        print("ℹ️  Sin procesos nuevos (todos ya existían o fueron enriquecidos)")

    return nuevos


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
    """Obtiene y guarda artículos UNSPSC para procesos nuevos."""
    if not procesos_nuevos:
        return
    total_articulos = 0
    for proceso in procesos_nuevos:
        articulos = obtener_articulos_proceso(proceso["codigo_proceso"])
        if articulos:
            total_articulos += guardar_articulos(proceso["codigo_proceso"], articulos)
        time.sleep(0.2)
    print(f"✅ {total_articulos} artículos guardados")


def enriquecer_articulos_faltantes():
    """
    Busca procesos que ya fueron enriquecidos por la API pero que aún no tienen artículos
    (caso: el scraper los guardó, la API los actualizó pero los artículos aún no se buscaron).
    """
    try:
        # Procesos enriquecidos por la API
        enriquecidos = supabase.table("procesos") \
            .select("codigo_proceso") \
            .eq("enriquecido_api", True) \
            .execute()

        if not enriquecidos.data:
            return

        codigos_enriquecidos = [r["codigo_proceso"] for r in enriquecidos.data]

        # De esos, cuáles NO tienen artículos
        con_articulos = supabase.table("articulos_proceso") \
            .select("codigo_proceso") \
            .in_("codigo_proceso", codigos_enriquecidos[:500]) \
            .execute()

        codigos_con_arts = set(r["codigo_proceso"] for r in con_articulos.data)
        sin_articulos = [c for c in codigos_enriquecidos if c not in codigos_con_arts]

        if not sin_articulos:
            return

        print(f"🔎 {len(sin_articulos)} procesos sin artículos — obteniendo...")
        total = 0
        for codigo in sin_articulos[:50]:  # máx 50 por ciclo para no saturar
            articulos = obtener_articulos_proceso(codigo)
            if articulos:
                total += guardar_articulos(codigo, articulos)
            time.sleep(0.2)

        if total:
            print(f"✅ {total} artículos nuevos obtenidos para procesos existentes")

    except Exception as e:
        print(f"⚠️  Error en enriquecer_articulos_faltantes: {e}")


# ============================================
# NOTIFICACIONES
# ============================================

def notificar_procesos_nuevos(procesos_nuevos):
    if not procesos_nuevos:
        return

    try:
        from notifications import enviar_notificacion

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

                # Usar URL directa del portal DGCP si existe, si no la PWA
                url_proceso = proceso.get("url") or f"https://comprasdominicanas.gob.do/proceso/{codigo}"

                ok = enviar_notificacion(
                    subscription_info,
                    titulo=f"🏗️ {entidad}{monto_str}",
                    cuerpo=titulo_proceso,
                    url=url_proceso
                )

                if ok:
                    notificaciones_enviadas += 1

        print(f"🔔 {notificaciones_enviadas} notificaciones enviadas")

    except Exception as e:
        print(f"❌ Error enviando notificaciones: {e}")


# ============================================
# FUNCIÓN PRINCIPAL
# ============================================

def ejecutar_monitor():
    import time as _time
    t0 = _time.time()
    print(f"\n{'='*50}")
    print(f"🔍 Monitor | {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*50}")

    try:
        procesos = obtener_todas_las_paginas()
        if not procesos:
            print("📭 Sin procesos en el período")
            registrar_sync_exitosa(procesos_encontrados=0, procesos_nuevos=0)
            registrar_cron_log("monitor_api", "ok",
                {"procesos_encontrados": 0, "procesos_nuevos": 0, "nota": "sin procesos en período"},
                int((_time.time()-t0)*1000))
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

        enriquecer_articulos_faltantes()

        registrar_sync_exitosa(
            procesos_encontrados=len(procesos),
            procesos_nuevos=len(nuevos)
        )
        registrar_cron_log("monitor_api", "ok", {
            "procesos_encontrados": len(procesos),
            "procesos_nuevos": len(nuevos),
        }, int((_time.time()-t0)*1000))

        print(f"✅ Monitor completado\n")
        return nuevos

    except Exception as e:
        registrar_cron_log("monitor_api", "error", {"error": str(e)}, int((_time.time()-t0)*1000))
        raise


if __name__ == "__main__":
    ejecutar_monitor()
