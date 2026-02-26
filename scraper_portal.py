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

        # Parsear fecha de publicación (formato: DD/MM/YYYY HH:MM (UTC -4 horas))
        fecha_str = textos[5].replace("(UTC -4 horas)", "").strip()
        fecha_publicacion = None
        try:
            fecha_publicacion = datetime.strptime(fecha_str, "%d/%m/%Y %H:%M").isoformat()
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
            "monto_estimado": monto,
            "estado_proceso": textos[8].strip(),
            # URL directa al proceso en el portal
            "url": (
                f"https://comunidad.comprasdominicana.gob.do/Public/Tendering/"
                f"ContractNoticeManagement/Index?currentLanguage=es&Country=DO"
                f"&Theme=DGCP&NoticeReference={referencia}"
            ),
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

def obtener_articulos_rapido(codigo_proceso):
    """
    Llama a la API DGCP para obtener los artículos UNSPSC de UN proceso específico.
    Se ejecuta inmediatamente cuando el scraper detecta un proceso nuevo.
    """
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
        print(f"⚠️  No se pudieron obtener artículos para {codigo_proceso}: {e}")
        return []


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

        entidad = proceso.get("unidad_compra", "")[:40]
        titulo = proceso.get("titulo", "Nueva licitación")[:70]
        monto = proceso.get("monto_estimado")
        monto_str = f" | RD${monto:,.0f}" if monto else ""
        url = proceso.get("url", "https://comprasdominicanas.gob.do")

        enviadas = 0
        omitidas = 0
        for sub in suscripciones:
            intereses = sub.get("intereses_rubros") or []

            if not proceso_coincide_con_intereses(proceso, articulos, intereses):
                omitidas += 1
                continue

            ok = enviar_notificacion(
                subscription_info={
                    "endpoint": sub["endpoint"],
                    "keys": {"auth": sub["auth"], "p256dh": sub["p256dh"]},
                },
                titulo=f"🏗️ {entidad}{monto_str}",
                cuerpo=titulo,
                url=url,
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
        return []

    print(f"🆕 {len(nuevos)} procesos nuevos — obteniendo artículos UNSPSC...")

    total_notificaciones = 0
    for proceso in nuevos:
        codigo = proceso["codigo_proceso"]

        # 1. Guardar proceso básico en Supabase
        guardado = guardar_proceso_basico(proceso)
        if not guardado:
            continue  # Ya existía (race condition), saltar

        # 2. Obtener artículos UNSPSC inmediatamente de la API
        articulos = obtener_articulos_rapido(codigo)
        if articulos:
            guardar_articulos_portal(codigo, articulos)
            print(f"   📦 [{codigo}] {len(articulos)} artículos UNSPSC obtenidos")
        else:
            print(f"   ⚠️  [{codigo}] Sin artículos UNSPSC aún (API puede tener delay)")

        # 3. Notificar solo a usuarios con intereses coincidentes
        n = notificar_proceso_inmediato(proceso, articulos)
        total_notificaciones += n

        monto = proceso.get("monto_estimado")
        monto_str = f"RD${monto:,.0f}" if monto else "N/A"
        print(f"   🔔 [{codigo}] {proceso['titulo'][:50]}... | {monto_str} → {n} notifs")

        # Pequeña pausa para no saturar la API DGCP
        time.sleep(0.5)

    print(f"✅ Scraper completado: {len(nuevos)} nuevos, {total_notificaciones} notificaciones\n")
    return nuevos


if __name__ == "__main__":
    ejecutar_scraper_portal()
