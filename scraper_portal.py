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

def obtener_url_proceso(codigo_proceso):
    """
    Consulta la API DGCP para obtener la URL oficial del proceso.
    El portal transaccional no tiene URLs directas por proceso (usa JavaScript),
    así que obtenemos la URL real del campo 'url' que devuelve la API.
    """
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
        if procesos and procesos[0].get("url"):
            return procesos[0]["url"]
    except Exception:
        pass
    # Fallback: búsqueda en datosabiertos con el código
    return f"https://datosabiertos.dgcp.gob.do/?q={codigo_proceso}"


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
        titulo_raw = proceso.get("titulo", "Nueva licitación")
        objeto = (proceso.get("objeto_proceso") or "").strip()
        monto = proceso.get("monto_estimado")
        url = proceso.get("url", "https://comprasdominicanas.gob.do")

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

        def generar_mensaje(objeto, monto_fmt, entidad, titulo_raw, dias_cierre):
            tema = extraer_tema(titulo_raw)
            desc_corta, desc_larga = descripcion_notificacion(titulo_raw, tema)

            # ── URGENCIA (cierra pronto) ──
            if dias_cierre is not None and dias_cierre <= 3:
                if dias_cierre <= 0:
                    titulos = [
                        f"🚨 ¡Cierra HOY! {desc_corta}",
                        f"⏰ Último día para entrar: {desc_corta}",
                    ]
                    cuerpos = [
                        f"¡Esta es la señal! {entidad} cierra hoy. No lo dejes escapar.",
                        f"{entidad} — Fecha límite: HOY. ¿Ya tienes todo listo?",
                    ]
                elif dias_cierre == 1:
                    titulos = [
                        f"⚡ ¡Mañana cierra! {desc_corta}",
                        f"🔔 Queda 1 día para: {desc_corta}",
                    ]
                    cuerpos = [
                        f"¿Ya tienes todo listo? {entidad} no espera.",
                        f"{entidad} — ¡No lo dejes para después!",
                    ]
                else:
                    titulos = [
                        f"⏳ {dias_cierre} días para: {desc_corta}",
                        f"🚀 No te confíes — {desc_corta}",
                    ]
                    cuerpos = [
                        f"La competencia ya está preparando su oferta. {entidad}.",
                        f"{entidad} — Quedan {dias_cierre} días. ¿Ya empezaste?",
                    ]
                return random.choice(titulos), random.choice(cuerpos)

            # ── OBRAS ──
            if objeto == "Obras":
                if monto and float(monto) >= 50_000_000:
                    titulos = [
                        f"🔥 Esta obra puede cambiar tu año: {desc_corta}",
                        f"🏆 {monto_fmt} en juego — {desc_corta}",
                        f"💰 ¡Proyecto grande! {desc_corta}",
                    ]
                    cuerpos = [
                        f"{entidad} publicó esto hace instantes. ¿Participamos?",
                        f"Una de las más grandes del día. {monto_fmt} — {entidad}.",
                        f"Vale la pena revisarla. {entidad} está esperando ofertas.",
                    ]
                else:
                    titulos = [
                        f"🔨 ¡Hay trabajo! {desc_corta}",
                        f"🏗️ Acaba de salir: {desc_corta}",
                        f"📋 Nueva obra disponible: {desc_corta}",
                    ]
                    cuerpos = [
                        f"{entidad} está buscando contratista. ¿Eres tú?",
                        f"Publicado ahora mismo — tienes ventaja si entras temprano.",
                        f"¿Tu empresa puede con esto? {entidad} está esperando.",
                    ]

            # ── BIENES ──
            elif objeto == "Bienes":
                titulos = [
                    f"📦 {entidad[:28]} necesita: {desc_corta}",
                    f"🛒 Acaba de salir: {desc_corta}",
                    f"📦 Oportunidad de suministro: {desc_corta}",
                ]
                cuerpos = [
                    f"¿Tienes lo que necesitan? Revísalo antes que la competencia.",
                    f"{entidad} está comprando — publicado hace instantes.",
                    f"Proceso nuevo. Primero en llegar, primero en ganar.",
                ]

            # ── SERVICIOS ──
            elif objeto == "Servicios":
                titulos = [
                    f"🛠️ {entidad[:28]} busca: {desc_corta}",
                    f"💼 Contrato disponible: {desc_corta}",
                    f"🤝 Acaba de publicarse: {desc_corta}",
                ]
                cuerpos = [
                    f"¿Tu empresa puede con esto? {entidad} está esperando.",
                    f"Publicado ahora mismo — sé el primero en entrar.",
                    f"{entidad} — Primero en llegar, primero en ganar.",
                ]

            # ── GENÉRICO ──
            else:
                titulos = [
                    f"⚡ Acaba de salir: {desc_corta}",
                    f"🏛️ {entidad[:28]} publicó: {desc_corta}",
                    f"📢 Nueva oportunidad: {desc_corta}",
                ]
                cuerpos = [
                    f"Sé el primero en revisarlo. {entidad}.",
                    f"{entidad} — Publicado ahora mismo.",
                    f"¿Es para ti? Entra y revisa.",
                ]

            return random.choice(titulos), random.choice(cuerpos)

        enviadas = 0
        omitidas = 0
        for sub in suscripciones:
            intereses = sub.get("intereses_rubros") or []

            if not proceso_coincide_con_intereses(proceso, articulos, intereses):
                omitidas += 1
                continue

            notif_titulo, notif_cuerpo = generar_mensaje(
                objeto, monto_fmt, entidad, titulo_raw, dias_cierre
            )

            ok = enviar_notificacion(
                subscription_info={
                    "endpoint": sub["endpoint"],
                    "keys": {"auth": sub["auth"], "p256dh": sub["p256dh"]},
                },
                titulo=notif_titulo,
                cuerpo=notif_cuerpo,
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

        # 2. Obtener URL oficial del proceso desde la API DGCP
        url_oficial = obtener_url_proceso(codigo)
        if url_oficial:
            proceso["url"] = url_oficial

        # 3. Obtener artículos UNSPSC inmediatamente de la API
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
