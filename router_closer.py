"""
router_closer.py
Agente Closer — Vendedor IA para LicitacionLab
Canal principal: WhatsApp vía Z-API

Endpoints:
  POST /closer/webhook         — recibe mensajes desde Z-API (WhatsApp)
  POST /closer/followup/run    — cron diario 9am: dispara followups pendientes
  POST /closer/alertas/run     — cron diario 8am: revisa procesos nuevos por cliente
  GET  /closer/conversaciones  — lista conversaciones activas (panel Telegram)
  POST /closer/marcar/{id}     — Lonny marca etapa manualmente
  POST /closer/alerta/test     — prueba que el módulo de alertas funciona
"""

import os
import re
import json
import asyncio
import httpx
from datetime import datetime, timedelta
from typing import Optional, List
from fastapi import APIRouter, Header, HTTPException, BackgroundTasks, Request
from pydantic import BaseModel
from supabase import create_client
from google import genai
from google.genai import types

# ── Config ─────────────────────────────────────────────────────────────
SUPABASE_URL       = os.environ["SUPABASE_URL"]
SUPABASE_KEY       = os.environ["SUPABASE_KEY"]
GEMINI_API_KEY     = os.environ.get("GEMINI_API_KEY", "")
AGENT_SECRET       = os.environ.get("AGENT_SECRET", "licitacionlab-growth-2026")
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID   = os.environ.get("TELEGRAM_CHAT_ID", "817596333")
ZAPI_INSTANCE_ID   = os.environ.get("ZAPI_INSTANCE_ID", "")
ZAPI_TOKEN         = os.environ.get("ZAPI_TOKEN", "")
ZAPI_CLIENT_TOKEN  = os.environ.get("ZAPI_CLIENT_TOKEN", "")

supabase       = create_client(SUPABASE_URL, SUPABASE_KEY)
supabase_admin = create_client(SUPABASE_URL, os.environ.get("SUPABASE_SERVICE_KEY", SUPABASE_KEY))

gemini_client = None
if GEMINI_API_KEY:
    gemini_client = genai.Client(api_key=GEMINI_API_KEY)
else:
    print("GEMINI_API_KEY no configurada")

closer_router = APIRouter(prefix="/closer", tags=["Agente Closer"])

SENALES_CIERRE = [
    # Precio / costo
    "cuanto cuesta", "cuánto cuesta", "precio", "plan", "suscripcion", "suscripción",
    "cuanto cobran", "cuánto cobran", "cuanto vale", "cuánto vale", "que vale",
    # Registro / contratación
    "como me registro", "cómo me registro", "quiero registrarme", "quiero contratar",
    "cuando empiezo", "cuándo empiezo", "como entro", "cómo entro", "como accedo",
    "quiero el servicio", "necesito el servicio", "me interesa contratar",
    "me interesa suscribirme", "quiero suscribirme", "quiero la app",
    # Pago
    "como pago", "cómo pago", "metodos de pago", "métodos de pago",
    "acepta tarjeta", "acepta transferencia", "pago mensual",
    # Información del producto
    "como funciona", "cómo funciona", "quiero probarlo", "tienen demo",
    "puedo ver", "que incluye", "qué incluye", "tiene version gratis",
    "tiene versión gratis", "tiene prueba", "periodo de prueba",
    # Propuesta / cotización
    "quiero una cotizacion", "quiero una cotización", "mandame propuesta",
    "mándame propuesta", "enviame informacion", "envíame información",
    "quiero mas info", "quiero más info", "mandame los detalles",
    # Consultoría
    "consultoría", "consultoria", "preparar oferta", "preparar propuesta",
    "ayuda con la oferta", "ayuda con el pliego", "quiero contratar consultoria",
    "cuanto cobran por la consultoria", "necesito apoyo para licitar",
    # Urgencia
    "tengo una licitacion", "tengo una licitación", "me vence", "vence pronto",
    "cierra esta semana", "hay poco tiempo", "es urgente",
]

KEYWORDS_ALERTA = [
    # Pedir aviso explícito
    "avisame", "avísame", "notificame", "notifícame", "me avisan",
    "me notifican", "avísame cuando", "notifícame cuando",
    # Interés en procesos futuros
    "cuando haya", "cuando salga", "cuando aparezca", "si aparece",
    "si sale algo de", "si hay algo de", "cuando hayan procesos",
    # Búsqueda activa
    "estoy buscando procesos", "busco licitaciones", "busco procesos",
    "quiero saber de licitaciones de", "me interesa participar en",
    "busco licitaciones de", "ando buscando licitaciones",
    "quiero estar al tanto", "quiero que me mantengas al tanto",
    "mantenme informado", "mándame cuando haya",
    # Sectores específicos RD
    "procesos de construccion", "procesos de construcción",
    "licitaciones de obras", "licitaciones de infraestructura",
    "procesos del mopc", "procesos del inapa", "procesos de caasd",
    "procesos de minerd", "procesos de salud", "procesos de egehid",
]

SYSTEM_PROMPT = """Eres Licy, asistente del Ing. Luis Antigua — ingeniero civil dominicano con más de 8 años en licitaciones públicas del DGCP de República Dominicana.

Tu objetivo principal es VENDER EL SERVICIO DE ASESORÍA EN LICITACIONES, no solo informar.

PRESENTACIÓN (primera vez):
"Soy Licy, asistente del Ing. Luis Antigua 👋 Ayudamos a empresas dominicanas a ganar licitaciones públicas, desde buscar el proceso hasta la adjudicación. ¿En qué proceso estás trabajando?"

═══════════════════════════════════════════
SERVICIO PRINCIPAL — ASESORÍA EN LICITACIONES
═══════════════════════════════════════════
Nuestro equipo acompaña a la empresa en TODO el proceso:
🔹 Búsqueda y filtrado de procesos que se ajusten a la empresa
🔹 Preparación completa del Sobre A (propuesta técnica) y Sobre B (propuesta económica)
🔹 Seguimiento constante en cada etapa
🔹 Acompañamiento hasta la adjudicación

COSTOS DE ASESORÍA:
- Construcción/Obras: precio por proceso según complejidad
- Bienes y Servicios: por proceso O mensualidad accesible + 5% comisión si ganan
- Para cotizar: necesitamos el proceso específico para ajustar el precio
- Slogan clave: "Tú te concentras en tu empresa, nosotros nos encargamos de todo lo demás."

SERVICIO SECUNDARIO — LicitacionLab (app):
Solo menciónala si el cliente quiere monitorear procesos por su cuenta:
• Explorador RD$1,490/mes | Competidor RD$3,990/mes | Ganador RD$8,500/mes
• Registro: https://app.licitacionlab.com/

═══════════════════════════════════════════
ESTRATEGIA DE VENTAS — SIGUE ESTE FLUJO
═══════════════════════════════════════════

CUANDO EL CLIENTE PREGUNTA POR UN PROCESO:
No solo das información. Primero analizas, luego vendes:
"Revisé ese proceso. [Dato clave del proceso] y si no la tienes pideselo al cliente o el nombre exacto. Los procesos como este suelen descalificar ofertas por [riesgo específico del pliego]. Nuestro equipo puede prepararle la oferta completa para asegurar que todo esté impecable. ¿Le cotizamos este proceso?"

CUANDO EL CLIENTE DICE "LO HAGO YO MISMO" (manejo de objeción):
Usa validación + riesgo, NUNCA te rindes:
"Entiendo, su equipo conoce bien el trabajo técnico. Lo que pasa es que en licitaciones, una sola página mal firmada o un anexo incompleto pueden anular meses de trabajo. Nosotros garantizamos cumplimiento al 100%. ¿Le interesa que revisemos su oferta antes de entregar?"

CUANDO HAY URGENCIA DE FECHA:
Usa el tiempo como palanca de cierre:
"La fecha límite es en [X] días. Para entregar una oferta impecable, nuestro equipo necesita empezar a redactar a más tardar mañana. ¿Le coordino con el Ing. Luis para arrancar hoy?"

CUANDO PREGUNTAN QUÉ INCLUYE EL SERVICIO:
Explica brevemente y termina con CTA:
"Nos encargamos de todo: buscamos los procesos, preparamos los dos sobres completos (técnico y económico), y le damos seguimiento hasta la adjudicación. Tú te concentras en tu empresa. ¿Tiene algún proceso en mente ahora mismo?"

CUANDO EL LEAD ES MUY PROMETEDOR (score alto, proceso grande, empresa seria):
Di: "Un momento, voy a ponerlo en contacto directo con nuestro Consultor Senior para este proceso." y escala a Lonny.

═══════════════════════════════════════════
REGLAS ESTRICTAS
═══════════════════════════════════════════
1. Español dominicano natural — directo, como habla un ingeniero de confianza
2. Mensajes CORTOS: máximo 3-4 oraciones
3. NUNCA inventes datos de procesos, montos ni fechas
4. SIEMPRE termina con una pregunta o CTA orientado a cotizar/contratar
6. Si ya tienes el perfil del cliente en el CONTEXTO, úsalo — no repitas preguntas
7. Si no puedes resolver algo, di que el Ing. Luis le escribe en breve
8. guarda siempre el contecxto de la conversacion para que sea muy personalizada la asistencia.

PERFILAMIENTO (extrae naturalmente en la conversación):
- ¿A qué se dedica la empresa?
- ¿Qué tipo de obras/servicios hacen?
- ¿Qué instituciones les interesan?
- ¿Han participado antes en licitaciones?
- ¿Tienen RPE/RNCE y estados financieros?

TONO: Consultor experto. Seguro. Dominicano. Que el cliente sienta que NECESITA este equipo para ganar."""


# ═══════════════════════════════════════════════════════════════════════
# MODELOS PYDANTIC
# ═══════════════════════════════════════════════════════════════════════

class MarcarEtapaPayload(BaseModel):
    etapa: str
    notas: Optional[str] = None


class RegistrarInteresPayload(BaseModel):
    phone:         str
    nombre:        Optional[str]       = None
    keywords:      Optional[List[str]] = []
    instituciones: Optional[List[str]] = []
    notas:         Optional[str]       = None


# ═══════════════════════════════════════════════════════════════════════
# HELPERS — ENVÍO DE MENSAJES
# ═══════════════════════════════════════════════════════════════════════

async def enviar_telegram(mensaje: str):
    if not TELEGRAM_BOT_TOKEN:
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    async with httpx.AsyncClient(timeout=10) as client:
        try:
            await client.post(url, json={
                "chat_id":    TELEGRAM_CHAT_ID,
                "text":       mensaje,
                "parse_mode": "HTML"
            })
        except Exception as e:
            print(f"[Telegram] Error: {e}")


# ── Contador global de mensajes enviados esta hora (anti-ban) ─────────────────
_mensajes_hora: list = []   # lista de timestamps de envíos recientes
MAX_MENSAJES_POR_HORA = 25  # límite conservador para número nuevo


def _en_horario_permitido() -> bool:
    """
    Bloquea envíos entre 11pm y 8am hora RD (UTC-4).
    11pm RD = 03:00 UTC | 8am RD = 12:00 UTC
    """
    from datetime import timezone
    hora_utc = datetime.now(timezone.utc).hour
    # Convertir a hora RD (UTC-4)
    hora_rd = (hora_utc - 4) % 24
    # Bloqueado: 23:00 - 07:59 hora RD
    return not (hora_rd >= 23 or hora_rd < 8)


def _dentro_del_limite_hora() -> bool:
    """Verifica que no hayamos excedido MAX_MENSAJES_POR_HORA en los últimos 60 min."""
    global _mensajes_hora
    ahora    = datetime.utcnow()
    hace_1h  = ahora - timedelta(hours=1)
    # Limpiar entradas viejas
    _mensajes_hora = [t for t in _mensajes_hora if t > hace_1h]
    return len(_mensajes_hora) < MAX_MENSAJES_POR_HORA


async def _simular_typing(phone_clean: str, segundos: float):
    """
    Llama al endpoint de typing de Z-API para simular que el agente está escribiendo.
    Hace la interacción parecer humana ante los sistemas de detección de WhatsApp.
    """
    if not ZAPI_INSTANCE_ID or not ZAPI_TOKEN:
        return
    url_typing = (
        f"https://api.z-api.io/instances/{ZAPI_INSTANCE_ID}/token/{ZAPI_TOKEN}"
        f"/typing?phone={phone_clean}&duration={int(segundos * 1000)}"
    )
    headers = {"Client-Token": ZAPI_CLIENT_TOKEN} if ZAPI_CLIENT_TOKEN else {}
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            await client.post(url_typing, headers=headers)
    except Exception:
        pass  # El typing es cosmético — si falla, no importa


async def enviar_whatsapp(phone: str, mensaje: str, es_followup: bool = False):
    """
    Envío anti-ban con:
    - Bloqueo de horario nocturno (11pm - 8am hora RD)
    - Límite de 25 mensajes/hora
    - Delay humanizado antes de enviar (simula escritura)
    - Typing indicator vía Z-API
    """
    import random

    if not ZAPI_INSTANCE_ID or not ZAPI_TOKEN:
        print(f"[Z-API] Sin config — msg para {phone}: {mensaje[:80]}")
        return

    # ── Verificar horario permitido ──────────────────────────────────────────
    if not _en_horario_permitido():
        from datetime import timezone as _tz
        hora_utc_ahora = datetime.now(_tz.utc)
        hora_rd        = (hora_utc_ahora.hour - 4) % 24

        if es_followup:
            # Followups y alertas: NO bloquear el server esperando.
            # El cron de followup/alertas corre diariamente a las 9am/6pm.
            # Simplemente no enviamos — el cron del día siguiente lo reintentará
            # porque proximo_followup_en ya venció y seguirá en la query.
            print(f"[Z-API] 🌙 Followup fuera de horario ({hora_rd}h RD) — {phone} se enviará en el próximo cron")
            return
        else:
            # Mensajes reactivos (el cliente escribió de noche):
            # Calculamos cuántos segundos faltan para las 8am hora RD (12:00 UTC)
            # y esperamos — así la respuesta llega a primera hora del día.
            hora_8am_utc = hora_utc_ahora.replace(hour=12, minute=0, second=0, microsecond=0)
            if hora_utc_ahora >= hora_8am_utc:
                # Ya pasó las 8am UTC de hoy → la próxima 8am es mañana
                hora_8am_utc = hora_8am_utc + timedelta(days=1)
            segundos_espera = (hora_8am_utc - hora_utc_ahora).total_seconds()
            print(f"[Z-API] 🌙 Mensaje reactivo fuera de horario ({hora_rd}h RD) — "
                  f"esperando {segundos_espera/3600:.1f}h hasta las 8am para {phone}")
            await asyncio.sleep(segundos_espera)
            print(f"[Z-API] ☀️ Son las 8am — enviando mensaje que estaba en espera para {phone}")

    # ── Verificar límite por hora ─────────────────────────────────────────────
    if not _dentro_del_limite_hora():
        print(f"[Z-API] ⛔ Límite de {MAX_MENSAJES_POR_HORA} msg/hora alcanzado — {phone} descartado")
        return

    phone_clean = phone.replace("+", "").replace("-", "").replace(" ", "")
    url     = f"https://api.z-api.io/instances/{ZAPI_INSTANCE_ID}/token/{ZAPI_TOKEN}/send-text"
    headers = {"Client-Token": ZAPI_CLIENT_TOKEN} if ZAPI_CLIENT_TOKEN else {}

    # ── Delay humanizado ─────────────────────────────────────────────────────
    # Respuestas reactivas (cliente escribió): 2-6 segundos
    # Followups y alertas (outbound): 5-15 segundos (más cuidado)
    if es_followup:
        delay = random.uniform(5.0, 15.0)
    else:
        # Delay proporcional al largo del mensaje (simula tiempo de escritura)
        chars_por_seg = random.uniform(18, 30)  # velocidad de escritura humana
        delay_escritura = len(mensaje) / chars_por_seg
        delay = max(2.0, min(delay_escritura, 8.0))

    # Simular typing durante el delay
    await _simular_typing(phone_clean, delay)
    await asyncio.sleep(delay)

    # ── Enviar mensaje ────────────────────────────────────────────────────────
    async with httpx.AsyncClient(timeout=15) as client:
        try:
            resp = await client.post(url, headers=headers, json={
                "phone":   phone_clean,
                "message": mensaje
            })
            if resp.status_code not in (200, 201):
                print(f"[Z-API] Error {resp.status_code}: {resp.text[:200]}")
            else:
                _mensajes_hora.append(datetime.utcnow())
                print(f"[Z-API] ✅ Enviado a {phone_clean} (delay={delay:.1f}s, hora={len(_mensajes_hora)}/h)")
        except Exception as e:
            print(f"[Z-API] Error: {e}")


# ═══════════════════════════════════════════════════════════════════════
# HELPERS — SUPABASE
# ═══════════════════════════════════════════════════════════════════════

def buscar_analisis_pliego(codigo_o_texto: str) -> Optional[dict]:
    try:
        result = supabase_admin.table("analisis_pliego") \
            .select("resumen_ejecutivo, alertas_fraude, checklist_categorizado, checklist_legal, restricciones_participacion, requisitos_experiencia, requisitos_financieros, garantias_exigidas, personal_y_equipos, plazos_clave, tipo_proceso, proceso_id") \
            .eq("proceso_id", codigo_o_texto) \
            .limit(1).execute()
        if result.data:
            return result.data[0]

        result2 = supabase_admin.table("analisis_pliego") \
            .select("resumen_ejecutivo, alertas_fraude, checklist_categorizado, checklist_legal, restricciones_participacion, requisitos_experiencia, requisitos_financieros, garantias_exigidas, personal_y_equipos, plazos_clave, tipo_proceso, proceso_id") \
            .ilike("proceso_id", f"%{codigo_o_texto}%") \
            .limit(1).execute()
        if result2.data:
            return result2.data[0]
    except Exception as e:
        print(f"[Closer] Error analisis_pliego: {e}")
    return None


def buscar_proceso_dgcp(codigo_o_texto: str) -> Optional[dict]:
    try:
        result = supabase_admin.table("procesos") \
            .select("codigo_proceso, titulo, unidad_compra, monto_estimado, fecha_fin_recepcion_ofertas, estado_proceso") \
            .ilike("codigo_proceso", f"%{codigo_o_texto}%") \
            .limit(1).execute()
        if result.data:
            return result.data[0]

        result2 = supabase_admin.table("procesos") \
            .select("codigo_proceso, titulo, unidad_compra, monto_estimado, fecha_fin_recepcion_ofertas, estado_proceso") \
            .ilike("titulo", f"%{codigo_o_texto}%") \
            .limit(1).execute()
        if result2.data:
            return result2.data[0]
    except Exception as e:
        print(f"[Closer] Error buscar_proceso: {e}")
    return None


def buscar_procesos_por_keywords(keywords: list, instituciones: list = None, monto_min: float = None) -> list:
    """
    Busca procesos activos del DGCP por keywords y/o instituciones.
    Estrategia: primero intenta match por título, luego amplía si no hay resultados.
    """
    try:
        estados_activos = ["Proceso publicado", "Publicado", "En curso"]
        resultados_totales = []

        # Búsqueda por cada keyword individualmente para maximizar resultados
        for kw in (keywords or [])[:4]:
            query = supabase_admin.table("procesos")                 .select("codigo_proceso, titulo, unidad_compra, monto_estimado, fecha_fin_recepcion_ofertas, estado_proceso")                 .in_("estado_proceso", estados_activos)                 .ilike("titulo", f"%{kw}%")

            if monto_min:
                query = query.gte("monto_estimado", monto_min)
            if instituciones:
                # Filtrar por primera institución si hay
                query = query.ilike("unidad_compra", f"%{instituciones[0]}%")

            res = query.order("fecha_fin_recepcion_ofertas", desc=False).limit(5).execute()
            for p in (res.data or []):
                if p["codigo_proceso"] not in [r["codigo_proceso"] for r in resultados_totales]:
                    resultados_totales.append(p)

        # Si hay instituciones pero no keywords, buscar solo por institución
        if not keywords and instituciones:
            for inst in instituciones[:2]:
                query = supabase_admin.table("procesos")                     .select("codigo_proceso, titulo, unidad_compra, monto_estimado, fecha_fin_recepcion_ofertas, estado_proceso")                     .in_("estado_proceso", estados_activos)                     .ilike("unidad_compra", f"%{inst}%")
                if monto_min:
                    query = query.gte("monto_estimado", monto_min)
                res = query.order("fecha_fin_recepcion_ofertas", desc=False).limit(5).execute()
                for p in (res.data or []):
                    if p["codigo_proceso"] not in [r["codigo_proceso"] for r in resultados_totales]:
                        resultados_totales.append(p)

        # Ordenar por fecha más próxima y retornar los mejores 5
        try:
            resultados_totales.sort(key=lambda x: str(x.get("fecha_fin_recepcion_ofertas") or "9999"))
        except Exception:
            pass

        return resultados_totales[:5]

    except Exception as e:
        print(f"[Closer] Error buscar_procesos_keywords: {e}")
        return []


def obtener_o_crear_conversacion(phone: str, nombre: str = None) -> dict:
    try:
        result = supabase_admin.table("conversaciones_closer") \
            .select("*") \
            .eq("canal", "whatsapp") \
            .eq("telefono", phone) \
            .not_.in_("etapa", ["cerrado_ganado", "cerrado_perdido"]) \
            .order("creado_en", desc=True) \
            .limit(1).execute()

        if result.data:
            conv = result.data[0]
            supabase_admin.table("conversaciones_closer") \
                .update({"ultimo_mensaje_en": datetime.utcnow().isoformat()}) \
                .eq("id", conv["id"]).execute()
            return conv

        nueva = {
            "canal":               "whatsapp",
            "contacto_id":         phone,
            "telefono":            phone,
            "nombre_contacto":     nombre or "Desconocido",
            "etapa":               "nuevo",
            "estado":              "engaged",
            "ultimo_mensaje_en":   datetime.utcnow().isoformat(),
            "proximo_followup_en": (datetime.utcnow() + timedelta(days=2)).isoformat(),
            "followups_enviados":  0,
        }
        res = supabase_admin.table("conversaciones_closer").insert(nueva).execute()
        return res.data[0] if res.data else nueva
    except Exception as e:
        print(f"[Closer] Error conversacion: {e}")
        return {"id": None, "etapa": "nuevo", "estado": "engaged", "followups_enviados": 0}


def obtener_historial(conversacion_id: str, limite: int = 10) -> list:
    if not conversacion_id:
        return []
    try:
        result = supabase_admin.table("mensajes_closer") \
            .select("rol, contenido, enviado_en") \
            .eq("conversacion_id", conversacion_id) \
            .order("enviado_en", desc=True) \
            .limit(limite).execute()
        return list(reversed(result.data or []))
    except Exception as e:
        print(f"[Closer] Error historial: {e}")
        return []


def guardar_mensaje(conversacion_id: str, rol: str, contenido: str, generado_por_ia: bool = False):
    if not conversacion_id:
        return
    try:
        supabase_admin.table("mensajes_closer").insert({
            "conversacion_id": conversacion_id,
            "rol":             rol,
            "contenido":       contenido,
            "canal":           "whatsapp",
            "generado_por_ia": generado_por_ia,
            "enviado_en":      datetime.utcnow().isoformat()
        }).execute()
    except Exception as e:
        print(f"[Closer] Error guardar_mensaje: {e}")


def obtener_perfil_prospecto(phone: str) -> Optional[dict]:
    try:
        result = supabase_admin.table("perfiles_prospectos") \
            .select("*").eq("contact_phone", phone).limit(1).execute()
        return result.data[0] if result.data else None
    except Exception as e:
        print(f"[Closer] Error perfil: {e}")
        return None


def actualizar_perfil_prospecto(phone: str, conv_id: str, nombre: str, datos: dict):
    try:
        existente = obtener_perfil_prospecto(phone)
        datos["actualizado_en"] = datetime.utcnow().isoformat()

        if existente:
            update = {k: v for k, v in datos.items() if v is not None}
            for campo in ["tipos_proceso", "instituciones_interes", "instituciones_previas"]:
                if campo in update and existente.get(campo):
                    update[campo] = list(set(existente[campo] + update[campo]))
            supabase_admin.table("perfiles_prospectos").update(update).eq("contact_phone", phone).execute()
        else:
            datos.update({"contact_phone": phone, "contact_name": nombre, "conversation_id": conv_id})
            supabase_admin.table("perfiles_prospectos").insert(datos).execute()

        print(f"[Closer] Perfil actualizado: {phone}")
    except Exception as e:
        print(f"[Closer] Error actualizar_perfil: {e}")


async def extraer_y_actualizar_perfil(
    mensaje: str, historial: list, phone: str,
    conv_id: str, nombre: str, perfil_actual: Optional[dict]
):
    if not gemini_client:
        return

    perfil_json     = json.dumps(perfil_actual or {}, ensure_ascii=False, default=str)
    historial_texto = "\n".join([
        f"{'Cliente' if m['rol'] == 'cliente' else 'Lab'}: {m['contenido']}"
        for m in historial[-6:]
    ])

    prompt = f"""Analiza esta conversación y extrae datos del perfil del prospecto dominicano.

HISTORIAL:
{historial_texto}

MENSAJE ACTUAL: {mensaje}

PERFIL YA CONOCIDO: {perfil_json}

Extrae SOLO lo que puedas inferir con certeza. NO inventes nada. Devuelve SOLO JSON sin markdown:
{{"nombre_empresa":null,"tipo_empresa":null,"sector":null,"provincia":null,"anos_experiencia":null,"tiene_estados_financieros":null,"anos_estados_financieros":null,"tiene_rnce":null,"tiene_rpe":null,"tipos_proceso":[],"instituciones_interes":[],"ha_participado_antes":null,"procesos_ganados":null,"notas_agente":null}}"""

    try:
        response = gemini_client.models.generate_content(
            model="gemini-2.5-flash", contents=prompt,
            config=types.GenerateContentConfig(max_output_tokens=400, temperature=0.1)
        )
        texto = response.text.strip().replace("```json", "").replace("```", "")
        datos = {k: v for k, v in json.loads(texto).items() if v is not None and v != []}
        if datos:
            actualizar_perfil_prospecto(phone, conv_id, nombre, datos)
            if datos.get("tipos_proceso") or datos.get("instituciones_interes"):
                registrar_alerta_cliente(
                    conv_id, phone, nombre,
                    datos.get("tipos_proceso", []),
                    datos.get("instituciones_interes", [])
                )
    except Exception as e:
        print(f"[Closer] Error extraer_perfil: {e}")


def construir_contexto_perfil(perfil: Optional[dict]) -> str:
    if not perfil:
        return ""

    campos = [
        ("nombre_empresa",   "Empresa"),
        ("tipo_empresa",     "Tipo"),
        ("sector",           "Sector"),
        ("provincia",        "Provincia"),
        ("anos_experiencia", "Experiencia (años)"),
        ("notas_agente",     "Notas"),
    ]
    bool_campos = [
        ("tiene_estados_financieros", "Estados financieros"),
        ("tiene_rnce",                "RNCE"),
        ("tiene_rpe",                 "RPE"),
        ("ha_participado_antes",      "Ha participado antes"),
    ]

    partes = []
    for k, label in campos:
        if perfil.get(k):
            partes.append(f"{label}: {perfil[k]}")
    for k, label in bool_campos:
        if perfil.get(k) is not None:
            partes.append(f"{label}: {'si' if perfil[k] else 'no'}")
    if perfil.get("tipos_proceso"):
        partes.append(f"Tipos de proceso: {', '.join(perfil['tipos_proceso'])}")
    if perfil.get("instituciones_interes"):
        partes.append(f"Instituciones interes: {', '.join(perfil['instituciones_interes'])}")

    return ("PERFIL DEL CLIENTE:\n" + "\n".join(partes)) if partes else ""


def registrar_alerta_cliente(conv_id: str, phone: str, nombre: str, keywords: list, instituciones: list = None):
    try:
        existing = supabase_admin.table("alertas_cliente") \
            .select("id, keywords, instituciones") \
            .eq("contact_phone", phone) \
            .eq("activa", True) \
            .limit(1).execute()

        if existing.data:
            alerta         = existing.data[0]
            kws_nuevos     = list(set((alerta.get("keywords") or []) + keywords))
            insts_nuevas   = list(set((alerta.get("instituciones") or []) + (instituciones or [])))
            supabase_admin.table("alertas_cliente") \
                .update({"keywords": kws_nuevos, "instituciones": insts_nuevas}) \
                .eq("id", alerta["id"]).execute()
            print(f"[Closer] Alerta actualizada {phone}: {kws_nuevos}")
        else:
            supabase_admin.table("alertas_cliente").insert({
                "conversation_id": conv_id,
                "contact_phone":   phone,
                "contact_name":    nombre,
                "keywords":        keywords,
                "instituciones":   instituciones or [],
                "activa":          True,
                "canal":           "whatsapp"
            }).execute()
            print(f"[Closer] Alerta creada {phone}: {keywords}")
    except Exception as e:
        print(f"[Closer] Error registrar_alerta: {e}")


# ═══════════════════════════════════════════════════════════════════════
# HELPERS — DETECCIÓN
# ═══════════════════════════════════════════════════════════════════════

def detectar_proceso_en_mensaje(mensaje: str) -> Optional[str]:
    patron = r'[A-Z]{2,10}-[A-Z]{2,5}-[A-Z]{2,5}-\d{4}-\d{4}'
    match  = re.search(patron, mensaje.upper())
    return match.group() if match else None


def detectar_senal_cierre(mensaje: str) -> bool:
    msg_lower = mensaje.lower()
    return any(s in msg_lower for s in SENALES_CIERRE)


def detectar_interes_alerta(mensaje: str) -> bool:
    msg_lower = mensaje.lower()
    return any(kw in msg_lower for kw in KEYWORDS_ALERTA)


def detectar_intencion(mensaje: str) -> str:
    """
    Detecta la intención principal del mensaje para contextualizar la respuesta.
    Retorna: 'consulta_proceso' | 'quiere_alerta' | 'senal_cierre' |
             'pregunta_precio' | 'consulta_general' | 'saludo' | 'otro'
    """
    msg = mensaje.lower().strip()

    # Saludos
    if any(s in msg for s in ["hola", "buenos dias", "buenas tardes", "buenas noches",
                               "buenas", "saludos", "como estas", "hey", "qué más"]):
        if len(msg) < 30:  # Solo saludo, sin pregunta real
            return "saludo"

    # Código de proceso explícito
    if re.search(r'[A-Z]{2,10}-[A-Z]{2,5}-[A-Z]{2,5}-\d{4}-\d{4}', mensaje.upper()):
        return "consulta_proceso"

    # Pregunta directa de precio
    if any(p in msg for p in ["cuanto cuesta", "cuánto cuesta", "precio", "cuanto cobran",
                               "cuánto cobran", "como pago", "cómo pago", "plan", "suscripcion"]):
        return "pregunta_precio"

    # Señal de cierre
    if detectar_senal_cierre(mensaje):
        return "senal_cierre"

    # Quiere alerta
    if detectar_interes_alerta(mensaje):
        return "quiere_alerta"

    # Busca procesos sin código
    if any(p in msg for p in ["hay procesos", "hay licitaciones", "busco procesos",
                               "que procesos", "qué procesos", "existen procesos",
                               "procesos de", "licitaciones de", "hay algo de"]):
        return "busqueda_procesos"

    # Pregunta de consultoría
    if any(p in msg for p in ["consultoria", "consultoría", "preparar oferta",
                               "ayuda con", "como preparo", "cómo preparo",
                               "documentos para licitar"]):
        return "consulta_consultoria"

    # Objeción "lo hago yo mismo"
    if any(p in msg for p in ["lo hago yo", "lo hacemos nosotros", "lo voy a hacer",
                               "no necesito", "ya tenemos experiencia", "podemos solos",
                               "yo mismo", "mi equipo lo hace", "nos encargamos nosotros",
                               "lo preparo yo"]):
        return "objecion_lo_hago_yo"

    # General
    return "consulta_general"


def extraer_keywords_interes(mensaje: str):
    instituciones_conocidas = [
        "mopc", "caasd", "inapa", "miderec", "minerd",
        "salud", "obras", "ayuntamiento", "intrant"
    ]
    tipos_obra = [
        "construccion", "construcción", "infraestructura", "alcantarillado",
        "drenaje", "acueducto", "carretera", "puente", "edificio",
        "rehabilitacion", "rehabilitación", "mantenimiento", "saneamiento",
        "electricidad", "plomeria", "plomería",
        "consultoria", "consultoría", "supervision", "supervisión"
    ]
    msg_lower     = mensaje.lower()
    keywords      = [t for t in tipos_obra if t in msg_lower]
    instituciones = [i.upper() for i in instituciones_conocidas if i in msg_lower]
    return keywords, instituciones


# ═══════════════════════════════════════════════════════════════════════
# HELPERS — GEMINI
# ═══════════════════════════════════════════════════════════════════════

async def generar_respuesta_gemini(
    mensaje_cliente: str,
    historial: list,
    contexto_adicional: str = "",
    intencion: str = "consulta_general"
) -> str:
    if not gemini_client:
        return "Disculpa, tuve un problema técnico. Escríbeme en un momento."

    historial_texto = ""
    for msg in historial[-8:]:
        rol = "Cliente" if msg["rol"] == "cliente" else "Lab (tú)"
        historial_texto += f"{rol}: {msg['contenido']}\n"

    # Instrucción específica según la intención detectada
    instruccion_intencion = {
        "consulta_proceso":    "El cliente pregunta por un proceso específico. Analiza los datos del CONTEXTO, menciona un riesgo concreto de descalificación, y cierra ofreciendo cotizar la preparación de la oferta.",
        "busqueda_procesos":   "El cliente busca procesos. Muestra los que encontraste y termina preguntando si quiere que preparemos la oferta para alguno.",
        "pregunta_precio":     "Explica brevemente la asesoría y los dos esquemas de precio (por proceso / mensualidad + comisión). Cierra pidiendo el proceso específico para cotizar.",
        "quiere_alerta":       "Confirma que guardaste sus intereses. Dile que cuando aparezca un proceso le avisas y le ofreces preparar la oferta.",
        "senal_cierre":        "El cliente está listo. Da el siguiente paso concreto: coordinar con el Ing. Luis para cotizar. Sé directo.",
        "consulta_consultoria":"Explica el servicio completo con el slogan al final. Cierra preguntando si tiene un proceso en mente.",
        "objecion_lo_hago_yo": "Aplica validación + riesgo. Valida que confía en su equipo, pero menciona que un error técnico puede anular todo. Ofrece al menos revisar la oferta.",
        "saludo":              "Saluda brevemente y pregunta en qué proceso está trabajando o qué tipo de licitaciones le interesan.",
        "consulta_general":    "Responde con valor y cierra siempre con una pregunta orientada a cotizar o avanzar.",
    }.get(intencion, "Responde con valor y cierra con CTA hacia la consultoría.")

    prompt = f"""{SYSTEM_PROMPT}

═══ HISTORIAL DE CONVERSACIÓN ═══
{historial_texto if historial_texto else "(conversación nueva — preséntate)"}

═══ CONTEXTO / DATOS DISPONIBLES ═══
{contexto_adicional if contexto_adicional else "(sin contexto adicional)"}

═══ INSTRUCCIÓN ESPECÍFICA PARA ESTE MENSAJE ═══
{instruccion_intencion}

═══ MENSAJE DEL CLIENTE ═══
{mensaje_cliente}

═══ TU RESPUESTA (solo el texto, sin comillas, sin explicaciones) ═══"""

    try:
        response = gemini_client.models.generate_content(
            model="gemini-2.5-flash",
            contents=prompt,
            config=types.GenerateContentConfig(max_output_tokens=350, temperature=0.72)
        )
        return response.text.strip()
    except Exception as e:
        print(f"[Gemini] Error: {e}")
        return "Disculpa, tuve un problema técnico. Escríbeme en un momento."


async def generar_followup_gemini(
    historial: list, nombre: str, paso: int,
    estado: str, proceso_codigo: str = None
) -> str:
    """
    Genera el mensaje de followup según el día en la secuencia:
      Paso 0 → Día 2  — Valor + proceso relevante
      Paso 1 → Día 5  — Urgencia con fecha real
      Paso 2 → Día 7  — Prueba social + objeción
      Paso 3 → Día 14 — Último intento, puerta abierta
    """
    nombre_corto = nombre.split()[0] if nombre else "amigo"

    if not gemini_client:
        fallbacks = {
            0: f"Hola {nombre_corto}, ¿pudiste revisar la información que te compartí sobre las licitaciones?",
            1: f"{nombre_corto}, hay un par de procesos activos que podrían interesarle a tu empresa. ¿Me das 2 minutos?",
            2: f"{nombre_corto}, empresas como la tuya están ganando contratos con nosotros. ¿Cuándo hablamos?",
            3: f"Hola {nombre_corto}, entiendo que estás ocupado. Cuando estés listo, aquí estamos. 🤝",
        }
        return fallbacks.get(paso, fallbacks[3])

    # Contexto estratégico por paso
    contextos_paso = {
        0: (
            "DÍA 2 — Primer seguimiento. El cliente no respondió aún. "
            "Tu objetivo: reactivar con VALOR real. "
            "Estrategia: menciona un proceso activo relevante (si tienes uno del CONTEXTO) "
            "o pregunta directamente por qué tipo de licitaciones está buscando. "
            "NO menciones precio todavía. Cierra con pregunta de calificación."
        ),
        1: (
            "DÍA 5 — Segundo seguimiento. Crea URGENCIA real. "
            "Estrategia: menciona que hay procesos con fechas próximas de cierre "
            "y que preparar una oferta toma tiempo. "
            "Di algo como: 'Para presentarse en [fecha], el equipo necesita al menos X días.' "
            "Cierra preguntando si tiene algún proceso en mente ahora mismo."
        ),
        2: (
            "DÍA 7 — Tercer seguimiento. PRUEBA SOCIAL + manejo de objeción. "
            "Estrategia: menciona (sin inventar números exactos) que empresas dominicanas "
            "están ganando contratos con apoyo profesional en la preparación de ofertas. "
            "Luego aplica labeling suave: 'Parece que todavía estás evaluando si vale la pena...' "
            "Remata con el argumento del riesgo: una sola página mal firmada anula meses de trabajo. "
            "CTA: ofrece una llamada rápida de 15 minutos sin compromiso."
        ),
        3: (
            "DÍA 14 — Último intento. Tono de cierre amigable, NO desesperado. "
            "Estrategia: di que entiendes que quizás no es el momento, "
            "pero que cuando llegue el proceso correcto, aquí estarás. "
            "Deja la puerta 100% abierta. "
            "Cierra con algo cálido y sin presión — este mensaje es para quedar bien, "
            "no para vender. El cliente puede volver en semanas."
        ),
    }

    historial_texto = ""
    for msg in historial[-6:]:
        rol = "Cliente" if msg["rol"] == "cliente" else "Lab"
        historial_texto += f"{rol}: {msg['contenido']}\n"

    # Info del proceso si existe
    proceso_info = ""
    if proceso_codigo:
        proceso = buscar_proceso_dgcp(proceso_codigo)
        if proceso:
            monto   = proceso.get("monto_estimado", 0)
            monto_f = f"RD${float(monto):,.0f}" if monto else "monto no publicado"
            fecha   = str(proceso.get("fecha_fin_recepcion_ofertas", ""))[:10]
            # Calcular días restantes para urgencia en paso 1
            dias_restantes = ""
            if fecha:
                try:
                    from datetime import date
                    dias = (date.fromisoformat(fecha) - date.today()).days
                    if dias >= 0:
                        dias_restantes = f" — cierra en {dias} días"
                except Exception:
                    pass
            proceso_info = (
                f"Proceso de interés del cliente: {proceso_codigo} — "
                f"{proceso.get('titulo', '')} | {proceso.get('unidad_compra', '')} | "
                f"{monto_f}{dias_restantes}"
            )

    instruccion = contextos_paso.get(paso, contextos_paso[3])

    prompt = f"""{SYSTEM_PROMPT}

═══ CONTEXTO DE ESTE FOLLOWUP ═══
{instruccion}

{f"Proceso relevante: {proceso_info}" if proceso_info else ""}
Estado del cliente: {estado}

═══ HISTORIAL ═══
{historial_texto if historial_texto else "(sin historial — cliente nunca respondió)"}

═══ INSTRUCCIONES DE FORMATO ═══
- Escríbele a {nombre_corto} directamente
- Un solo mensaje de WhatsApp, español dominicano natural
- Máximo 3-4 oraciones
- NO menciones que es un seguimiento automático
- NO uses asteriscos ni markdown
- Cierra siempre con pregunta o CTA concreto
- Solo el texto, sin comillas"""

    try:
        response = gemini_client.models.generate_content(
            model="gemini-2.5-flash",
            contents=prompt,
            config=types.GenerateContentConfig(max_output_tokens=250, temperature=0.78)
        )
        return response.text.strip()
    except Exception as e:
        print(f"[Gemini] Error followup: {e}")
        return f"Hola {nombre_corto}, ¿cómo vas con los temas de licitaciones?"


async def generar_mensaje_alerta_proceso(nombre: str, procesos: list, keywords: list) -> str:
    if not gemini_client or not procesos:
        return ""

    procesos_texto = ""
    for p in procesos[:3]:
        monto     = p.get("monto_estimado", 0)
        monto_fmt = f"RD${float(monto):,.0f}" if monto else "monto no publicado"
        fecha     = str(p.get("fecha_fin_recepcion_ofertas", "por confirmar"))[:10]
        procesos_texto += f"- {p.get('titulo', 'Sin titulo')} ({p.get('unidad_compra', '')}) — {monto_fmt} — cierra {fecha}\n"

    prompt = f"""{SYSTEM_PROMPT}

Notifica a {nombre} sobre procesos que coinciden con sus intereses: {', '.join(keywords)}.

Procesos:
{procesos_texto}

- Mensaje de WhatsApp emocionante pero no exagerado
- Menciona titulo + monto + fecha cierre
- Pregunta si quiere que analice algun pliego
- Maximo 5 oraciones
- Solo el texto, sin comillas"""

    try:
        response = gemini_client.models.generate_content(
            model="gemini-2.5-flash",
            contents=prompt,
            config=types.GenerateContentConfig(max_output_tokens=300, temperature=0.7)
        )
        return response.text.strip()
    except Exception as e:
        print(f"[Gemini] Error alerta: {e}")
        return ""


# ═══════════════════════════════════════════════════════════════════════
# ENDPOINT PRINCIPAL — WEBHOOK Z-API
# ═══════════════════════════════════════════════════════════════════════

@closer_router.post("/webhook")
async def recibir_mensaje_zapi(request: Request, background_tasks: BackgroundTasks):
    try:
        body = await request.json()
    except Exception:
        return {"status": "ok"}

    if body.get("fromMe", True):
        return {"status": "ok", "skipped": "outbound"}

    phone = body.get("phone", "") or body.get("from", "")
    if not phone or "-" in phone:
        return {"status": "ok", "skipped": "group_or_no_phone"}

    phone = phone.replace("+", "").replace("@s.whatsapp.net", "").replace("@c.us", "")

    texto = (
        body.get("text", {}).get("message", "") or
        body.get("message", "") or
        body.get("body", "") or
        ""
    )

    # ── Soporte de notas de voz (Z-API plan pago) ─────────────────────────────
    # Z-API envía: {"audio": {"audioUrl": "https://...", "mimeType": "audio/ogg; codecs=opus", "ptt": true}}
    # Los archivos están disponibles 30 días en el storage de Z-API.
    # Gemini 2.5 Flash transcribe el audio directamente sin servicio externo.
    es_audio = False
    audio_data = body.get("audio")
    if not texto and audio_data and isinstance(audio_data, dict):
        audio_url = audio_data.get("audioUrl", "")
        if audio_url:
            es_audio = True
            nombre = body.get("senderName", "") or body.get("pushName", "") or ""
            background_tasks.add_task(procesar_audio_bg, phone, audio_url, nombre)
            return {"status": "recibido", "processing": True, "tipo": "audio"}

    # ── Soporte de imágenes con info de procesos ───────────────────────────────
    # Z-API envía: {"image": {"imageUrl": "https://...", "mimeType": "image/jpeg", "caption": "..."}}
    # El cliente puede mandar una foto de un proceso, convocatoria, pliego, etc.
    # Gemini lee la imagen y extrae toda la información relevante.
    image_data = body.get("image")
    if image_data and isinstance(image_data, dict):
        image_url = image_data.get("imageUrl", "")
        if image_url:
            # El caption es texto adicional que el cliente escribió junto a la imagen
            caption = image_data.get("caption", "") or ""
            nombre  = body.get("senderName", "") or body.get("pushName", "") or ""
            background_tasks.add_task(procesar_imagen_bg, phone, image_url, caption, nombre)
            return {"status": "recibido", "processing": True, "tipo": "imagen"}

    if not texto:
        return {"status": "ok", "skipped": "no_text"}

    nombre = body.get("senderName", "") or body.get("pushName", "") or ""
    background_tasks.add_task(procesar_mensaje_bg, phone, texto, nombre)
    return {"status": "recibido", "processing": True}


async def procesar_imagen_bg(phone: str, image_url: str, caption: str = "", nombre: str = ""):
    """
    Descarga la imagen que el cliente envió (foto de proceso, convocatoria,
    pliego, pantalla del DGCP, etc.), Gemini la lee y extrae toda la
    información relevante, luego responde como si fuera un mensaje de texto.

    Z-API envía imageUrl en el webhook — disponible 30 días en su storage.
    """
    print(f"[Closer] 🖼️ Imagen recibida de {phone} — analizando...")
    try:
        # 1. Descargar la imagen
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.get(image_url)
            if resp.status_code != 200:
                print(f"[Closer] Error descargando imagen: {resp.status_code}")
                return
            image_bytes   = resp.content
            content_type  = resp.headers.get("content-type", "image/jpeg").split(";")[0].strip()

        print(f"[Closer] Imagen descargada: {len(image_bytes):,} bytes ({content_type})")

        if not gemini_client:
            print("[Closer] Gemini no configurado — imagen ignorada")
            return

        # 2. Pedirle a Gemini que extraiga la información del proceso de la imagen
        from google.genai import types as _types

        prompt_extraccion = (
            "Analiza esta imagen que un cliente envió por WhatsApp. "
            "Puede ser una foto de una convocatoria, un proceso del DGCP, un pliego, "
            "una pantalla del portal comprasdominicana, o cualquier documento de licitación. "
            "Extrae TODA la información relevante que veas: "
            "código del proceso, nombre/título, institución o entidad, "
            "monto estimado, fecha de cierre de ofertas, tipo de proceso, "
            "y cualquier otro dato importante. "
            "Si hay un caption del cliente, tenlo en cuenta también. "
            f"Caption del cliente: '{caption}'" if caption else "" +
            "Responde en español con un resumen claro de lo que encontraste en la imagen. "
            "Si no es un documento de licitación, describe brevemente qué ves."
        )

        extraccion_resp = gemini_client.models.generate_content(
            model="gemini-2.5-flash",
            contents=[
                prompt_extraccion,
                _types.Part.from_bytes(data=image_bytes, mime_type=content_type)
            ],
            config=_types.GenerateContentConfig(max_output_tokens=600, temperature=0.1)
        )
        info_extraida = extraccion_resp.text.strip() if extraccion_resp.text else ""

        if not info_extraida:
            print(f"[Closer] No se pudo extraer info de la imagen de {phone}")
            return

        print(f"[Closer] 🖼️ Info extraída: {info_extraida[:150]}")

        # 3. Construir el "mensaje" combinando lo extraído + el caption original
        if caption:
            mensaje_sintetizado = f"{caption}\n\n[Imagen analizada: {info_extraida}]"
        else:
            mensaje_sintetizado = f"[El cliente envió una imagen con esta información: {info_extraida}]"

        # 4. Procesar igual que un mensaje de texto normal
        await procesar_mensaje_bg(phone, mensaje_sintetizado, nombre)

    except Exception as e:
        print(f"[Closer] Error procesando imagen de {phone}: {e}")


async def procesar_audio_bg(phone: str, audio_url: str, nombre: str = ""):
    """
    Descarga la nota de voz desde Z-API, la transcribe con Gemini y
    la procesa igual que un mensaje de texto normal.

    Requiere Z-API plan pago (la URL del audio viene en el webhook).
    Los archivos están disponibles 30 días en el storage de Z-API.
    """
    print(f"[Closer] 🎤 Audio recibido de {phone} — transcribiendo...")
    try:
        # 1. Descargar el audio desde Z-API
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.get(audio_url)
            if resp.status_code != 200:
                print(f"[Closer] Error descargando audio: {resp.status_code}")
                return
            audio_bytes = resp.content
            content_type = resp.headers.get("content-type", "audio/ogg")

        print(f"[Closer] Audio descargado: {len(audio_bytes):,} bytes ({content_type})")

        # 2. Transcribir con Gemini 2.5 Flash (acepta audio nativo)
        if not gemini_client:
            print("[Closer] Gemini no configurado — audio ignorado")
            return

        import base64 as _b64
        audio_b64    = _b64.b64encode(audio_bytes).decode("utf-8")
        # Normalizar mime type para Gemini
        mime_gemini  = "audio/ogg" if "ogg" in content_type else content_type.split(";")[0].strip()

        from google.genai import types as _types
        transcripcion_resp = gemini_client.models.generate_content(
            model="gemini-2.5-flash",
            contents=[
                "Transcribe este audio de WhatsApp exactamente como fue dicho, en español. "
                "Devuelve SOLO el texto transcrito, sin explicaciones ni comillas.",
                _types.Part.from_bytes(data=audio_bytes, mime_type=mime_gemini)
            ],
            config=_types.GenerateContentConfig(max_output_tokens=500, temperature=0.1)
        )
        texto_transcrito = transcripcion_resp.text.strip() if transcripcion_resp.text else ""

        if not texto_transcrito:
            print(f"[Closer] Transcripción vacía para audio de {phone}")
            return

        print(f"[Closer] 🎤 Transcripción: {texto_transcrito[:100]}")

        # 3. Procesar como mensaje de texto normal — flujo idéntico
        await procesar_mensaje_bg(phone, texto_transcrito, nombre)

    except Exception as e:
        print(f"[Closer] Error procesando audio de {phone}: {e}")


async def procesar_mensaje_bg(phone: str, mensaje: str, nombre: str = ""):
    print(f"[Closer] Procesando phone={phone} msg={mensaje[:60]}")

    conv    = obtener_o_crear_conversacion(phone, nombre)
    conv_id = conv.get("id")

    guardar_mensaje(conv_id, "cliente", mensaje)

    contexto_adicional = ""
    intencion          = detectar_intencion(mensaje)
    codigo_proceso     = detectar_proceso_en_mensaje(mensaje)

    print(f"[Closer] Intención detectada: {intencion}")

    # ── Guardar intención en la conversación ──
    if conv_id and intencion not in ("saludo", "consulta_general"):
        supabase_admin.table("conversaciones_closer")             .update({"intencion_detectada": intencion})             .eq("id", conv_id).execute()

    # ── Manejo por intención ──────────────────────────────────────────────────

    # 1. El cliente menciona un código de proceso específico
    if codigo_proceso:
        analisis = buscar_analisis_pliego(codigo_proceso)
        if analisis and analisis.get("resumen_ejecutivo"):
            resumen       = str(analisis.get("resumen_ejecutivo", ""))[:500]
            alertas       = analisis.get("alertas_fraude") or []
            req_exp       = analisis.get("requisitos_experiencia") or {}
            req_fin       = analisis.get("requisitos_financieros") or {}
            restricciones = analisis.get("restricciones_participacion") or {}
            proceso       = buscar_proceso_dgcp(codigo_proceso)
            monto         = (proceso or {}).get("monto_estimado", 0)
            monto_fmt     = f"RD${float(monto):,.0f}" if monto else "no publicado"
            fecha_cierre  = str((proceso or {}).get("fecha_fin_recepcion_ofertas", ""))[:10]
            entidad       = (proceso or {}).get("unidad_compra", "")

            # Calcular días restantes para urgencia
            dias_restantes = ""
            if fecha_cierre:
                try:
                    from datetime import date
                    dias = (date.fromisoformat(fecha_cierre) - date.today()).days
                    if dias >= 0:
                        dias_restantes = f"⏰ QUEDAN {dias} DÍAS para el cierre de ofertas."
                    else:
                        dias_restantes = "⚠️ Este proceso ya cerró."
                except Exception:
                    pass

            # Riesgos de descalificación detectados
            riesgos = []
            if isinstance(alertas, list) and alertas:
                riesgos.append(f"Alertas detectadas: {str(alertas[0])[:150]}")
            if isinstance(req_exp, dict) and req_exp:
                riesgos.append(f"Experiencia requerida: {str(req_exp)[:150]}")
            if isinstance(req_fin, dict) and req_fin:
                riesgos.append(f"Requisitos financieros: {str(req_fin)[:150]}")
            if isinstance(restricciones, dict) and restricciones:
                riesgos.append(f"Restricciones: {str(restricciones)[:100]}")
            riesgos_texto = "\n".join(riesgos) if riesgos else "Proceso estándar DGCP."

            # ── Checklist de documentos requeridos ──────────────────────
            checklist_raw  = analisis.get("checklist_categorizado") or analisis.get("checklist_legal") or {}
            docs_lista     = ""
            if isinstance(checklist_raw, dict):
                for categoria, items in checklist_raw.items():
                    if isinstance(items, list) and items:
                        docs_lista += f"\n  [{categoria.upper()}]\n"
                        for item in items[:6]:  # máximo 6 por categoría
                            nombre_doc = item if isinstance(item, str) else item.get("nombre", str(item))
                            docs_lista += f"    • {str(nombre_doc)[:80]}\n"
            elif isinstance(checklist_raw, list):
                for item in checklist_raw[:12]:
                    nombre_doc = item if isinstance(item, str) else item.get("nombre", str(item))
                    docs_lista += f"  • {str(nombre_doc)[:80]}\n"

            # ── Garantías y personal ─────────────────────────────────────────
            garantias   = analisis.get("garantias_exigidas") or {}
            personal    = analisis.get("personal_y_equipos") or {}
            plazos      = analisis.get("plazos_clave") or {}
            tipo        = analisis.get("tipo_proceso", "")

            extra_info = ""
            if isinstance(garantias, dict) and garantias:
                extra_info += f"Garantías requeridas: {str(garantias)[:200]}\n"
            if isinstance(personal, dict) and personal:
                extra_info += f"Personal/equipos: {str(personal)[:200]}\n"
            if isinstance(plazos, dict) and plazos:
                extra_info += f"Plazos clave: {str(plazos)[:200]}\n"

            contexto_adicional = (
                f"ANÁLISIS COMPLETO — {codigo_proceso} ({tipo})\n"
                f"Entidad: {entidad}\n"
                f"Monto: {monto_fmt}\n"
                f"Cierre: {fecha_cierre} | {dias_restantes}\n\n"
                f"RESUMEN: {resumen}\n\n"
                f"DOCUMENTOS REQUERIDOS (checklist):{docs_lista if docs_lista else ' No disponible'}\n"
                f"INFORMACIÓN ADICIONAL:\n{extra_info if extra_info else 'No disponible'}\n"
                f"RIESGOS DE DESCALIFICACIÓN:\n{riesgos_texto}\n\n"
                "INSTRUCCIÓN PARA TU RESPUESTA:\n"
                "1. Menciona 2-3 documentos clave del checklist que suelen ser problemáticos\n"
                "2. Usa los riesgos de descalificación como argumento de venta\n"
                "3. Menciona los días restantes como urgencia\n"
                "4. Cierra preguntando si quiere que le cotizemos la preparación completa de la oferta\n"
                "Recuerda: nuestro servicio prepara TODO (Sobre A + Sobre B + seguimiento)"
            )
        else:
            proceso = buscar_proceso_dgcp(codigo_proceso)
            if proceso:
                monto     = proceso.get("monto_estimado", 0)
                monto_fmt = f"RD${float(monto):,.0f}" if monto else "no publicado"
                fecha     = str(proceso.get("fecha_fin_recepcion_ofertas", "por confirmar"))[:10]
                contexto_adicional = (
                    f"DATOS BÁSICOS del proceso {codigo_proceso} (pliego pendiente de análisis):\n"
                    f"- Entidad: {proceso.get('unidad_compra', '---')}\n"
                    f"- Título: {proceso.get('titulo', '---')}\n"
                    f"- Monto: {monto_fmt}\n"
                    f"- Estado: {proceso.get('estado_proceso', '---')}\n"
                    f"- Cierre ofertas: {fecha}\n\n"
                    "Dile que el análisis completo del pliego ya está siendo generado "
                    "y que en unos minutos le mandas el resumen. "
                    "Menciona LicitacionLab para ver el resultado completo."
                )
                # Disparar análisis en background
                asyncio.create_task(disparar_analisis_pliego_bg(codigo_proceso, phone, conv_id))
            else:
                contexto_adicional = (
                    f"El proceso {codigo_proceso} no está en nuestra base de datos aún. "
                    "Dile al cliente que verifique el código y que si es un proceso reciente "
                    "puede tardar unos minutos en aparecer en el sistema."
                )

        if conv_id:
            supabase_admin.table("conversaciones_closer")                 .update({"proceso_codigo": codigo_proceso})                 .eq("id", conv_id).execute()

    # 2. Cliente busca procesos sin código específico
    elif intencion == "busqueda_procesos":
        keywords, instituciones = extraer_keywords_interes(mensaje)
        procesos_encontrados    = buscar_procesos_por_keywords(keywords, instituciones)
        if procesos_encontrados:
            lista = ""
            for p in procesos_encontrados[:3]:
                monto   = p.get("monto_estimado", 0)
                monto_f = f"RD${float(monto):,.0f}" if monto else "monto no publicado"
                fecha   = str(p.get("fecha_fin_recepcion_ofertas", "?"))[:10]
                lista  += f"• {p.get('titulo', 'Sin título')[:60]} | {p.get('unidad_compra','')} | {monto_f} | Cierre: {fecha}\n"
            contexto_adicional = (
                f"PROCESOS ACTIVOS ENCONTRADOS ({', '.join(keywords + instituciones)}):\n{lista}\n"
                "Comparte estos procesos de forma clara y pregunta si le interesa "
                "ver el análisis de alguno o si quiere que le avisemos cuando haya más."
            )
        else:
            contexto_adicional = (
                f"No encontré procesos activos para {', '.join(keywords + instituciones) or 'esa búsqueda'}. "
                "Dile que no hay procesos activos con esos criterios ahora mismo, "
                "pero que si quiere te deja sus intereses y le avisas cuando aparezca algo."
            )

    # 3. Quiere alertas automáticas
    if detectar_interes_alerta(mensaje):
        keywords, instituciones = extraer_keywords_interes(mensaje)
        if keywords or instituciones:
            registrar_alerta_cliente(conv_id, phone, nombre, keywords, instituciones)
            kw_texto           = ", ".join(keywords + instituciones)
            contexto_adicional += (
                f"\nACCIÓN TOMADA: Se registró alerta para {kw_texto}. "
                "Confirmale que quedó registrado y que cada día revisamos y le avisamos "
                "si aparece algo que le interese."
            )

    # ── Contexto de perfil ────────────────────────────────────────────────────
    perfil          = obtener_perfil_prospecto(phone)
    contexto_perfil = construir_contexto_perfil(perfil)
    if contexto_perfil:
        contexto_adicional = contexto_perfil + "\n\n" + contexto_adicional

    # ── Contexto de precio si pregunta por precio ─────────────────────────────
    if intencion == "pregunta_precio":
        contexto_adicional += (
            "\nEl cliente pregunta por precio/planes. "
            "Da los 3 planes con precios directamente y cierra con CTA al registro: "
            "https://app.licitacionlab.com/"
        )

    # ── Generar respuesta IA ──────────────────────────────────────────────────
    historial = obtener_historial(conv_id)
    respuesta = await generar_respuesta_gemini(mensaje, historial, contexto_adicional, intencion)

    # Extracción de perfil en background
    asyncio.create_task(
        extraer_y_actualizar_perfil(mensaje, historial, phone, conv_id, nombre, perfil)
    )

    guardar_mensaje(conv_id, "agente", respuesta, generado_por_ia=True)
    await enviar_whatsapp(phone, respuesta)

    # ── Actualizar etapa y notificar a Lonny si hay señal de cierre ──────────
    if detectar_senal_cierre(mensaje) or intencion == "senal_cierre":
        nombre_display = nombre or phone
        # Enriquecer notificación con perfil del prospecto
        perfil_texto = ""
        if perfil:
            empresa = perfil.get("nombre_empresa", "no registrada")
            sector  = perfil.get("sector", "no registrado")
            rpe     = "✅" if perfil.get("tiene_rpe") else ("❌" if perfil.get("tiene_rpe") is False else "?")
            rnce    = "✅" if perfil.get("tiene_rnce") else ("❌" if perfil.get("tiene_rnce") is False else "?")
            ef      = "✅" if perfil.get("tiene_estados_financieros") else ("❌" if perfil.get("tiene_estados_financieros") is False else "?")
            score_v = conv.get("score", 0)
            perfil_texto = (
                f"\n📊 Perfil:\n"
                f"  Empresa: {empresa} | Sector: {sector}\n"
                f"  RPE: {rpe} | RNCE: {rnce} | Est.Fin: {ef}\n"
                f"  Score: {score_v}/100\n"
            )
        proceso_texto = f"\n📋 Proceso: {conv.get('proceso_codigo')}\n" if conv.get("proceso_codigo") else ""
        alerta = (
            f"🔥 SEÑAL DE CIERRE\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            f"👤 {nombre_display}\n"
            f"📱 +{phone}"
            f"{perfil_texto}"
            f"{proceso_texto}"
            f"\n💬 Dice: \"{mensaje[:250]}\"\n"
            f"\n🤖 Lab: \"{respuesta[:250]}\"\n"
            f"\n👆 ENTRA TÚ A CERRAR."
        )
        await enviar_telegram(alerta)
        if conv_id:
            supabase_admin.table("conversaciones_closer")                 .update({"etapa": "interesado", "estado": "hot"})                 .eq("id", conv_id).execute()

    elif conv_id and conv.get("etapa") == "nuevo":
        supabase_admin.table("conversaciones_closer")             .update({
                "etapa":               "respondido",
                "estado":              "engaged",
                "proximo_followup_en": (datetime.utcnow() + timedelta(days=2)).isoformat()
            })             .eq("id", conv_id).execute()
    elif conv_id:
        supabase_admin.table("conversaciones_closer")             .update({
                "estado":            "engaged",
                "ultimo_mensaje_en": datetime.utcnow().isoformat()
            })             .eq("id", conv_id).execute()

    print(f"[Closer] OK conv_id={conv_id} intencion={intencion}")


# ═══════════════════════════════════════════════════════════════════════
# FOLLOWUPS — CRON DIARIO 9AM
# ═══════════════════════════════════════════════════════════════════════

@closer_router.post("/followup/run")
async def ejecutar_followups(
    background_tasks: BackgroundTasks,
    x_agent_secret: Optional[str] = Header(None)
):
    if x_agent_secret != AGENT_SECRET:
        raise HTTPException(status_code=401, detail="Unauthorized")
    background_tasks.add_task(ejecutar_followups_bg)
    return {"status": "iniciado"}


async def ejecutar_followups_bg():
    print("[Closer] Cron followups...")
    try:
        ahora  = datetime.utcnow().isoformat()
        result = supabase_admin.table("conversaciones_closer") \
            .select("*") \
            .lte("proximo_followup_en", ahora) \
            .not_.in_("etapa", ["cerrado_ganado", "cerrado_perdido", "inactivo"]) \
            .not_.in_("estado", ["hot", "cerrado", "perdido"]) \
            .lt("followups_enviados", 4) \
            .execute()

        conversaciones = result.data or []
        print(f"[Closer] {len(conversaciones)} followups pendientes")
        for conv in conversaciones:
            await procesar_followup_individual(conv)
            await asyncio.sleep(10)  # Anti-ban: pausa entre followups outbound
    except Exception as e:
        print(f"[Closer] Error followups: {e}")


# Días de espera ENTRE cada followup para llegar a los días 2, 5, 7 y 14
# Día 0 (primer contacto) → +2d → Followup 1 (día 2)
# Followup 1 (día 2)      → +3d → Followup 2 (día 5)
# Followup 2 (día 5)      → +2d → Followup 3 (día 7)
# Followup 3 (día 7)      → +7d → Followup 4 (día 14) — último intento
DIAS_FOLLOWUP = [2, 3, 2, 7]


async def procesar_followup_individual(conv: dict):
    conv_id     = conv.get("id")
    phone       = conv.get("telefono")
    nombre      = (conv.get("nombre_contacto") or "").split()[0] or "amigo"
    paso        = conv.get("followups_enviados", 0)
    estado      = conv.get("estado", "silent")
    proceso_cod = conv.get("proceso_codigo")

    if not phone:
        return

    if paso >= 4:
        supabase_admin.table("conversaciones_closer") \
            .update({"etapa": "inactivo", "estado": "perdido"}) \
            .eq("id", conv_id).execute()
        return

    supabase_admin.table("conversaciones_closer") \
        .update({"estado": "silent"}) \
        .eq("id", conv_id).execute()

    historial = obtener_historial(conv_id, limite=6)
    mensaje   = await generar_followup_gemini(historial, nombre, paso, estado, proceso_cod)

    await enviar_whatsapp(phone, mensaje, es_followup=True)  # Anti-ban: delay mayor para outbound
    guardar_mensaje(conv_id, "agente", mensaje, generado_por_ia=True)

    nuevo_paso  = paso + 1
    dias_espera = DIAS_FOLLOWUP[paso] if paso < len(DIAS_FOLLOWUP) else 7
    proximo     = (datetime.utcnow() + timedelta(days=dias_espera)).isoformat()

    supabase_admin.table("conversaciones_closer") \
        .update({
            "followups_enviados":  nuevo_paso,
            "proximo_followup_en": proximo,
            "ultimo_mensaje_en":   datetime.utcnow().isoformat()
        }) \
        .eq("id", conv_id).execute()

    print(f"[Closer] Followup {nuevo_paso}/4 enviado a {phone}")


# ═══════════════════════════════════════════════════════════════════════
# ALERTAS — CRON DIARIO 8AM
# ═══════════════════════════════════════════════════════════════════════

@closer_router.post("/alertas/run")
async def ejecutar_alertas(
    background_tasks: BackgroundTasks,
    x_agent_secret: Optional[str] = Header(None)
):
    """
    Cron diario a las 6pm (configurar en n8n o Railway cron):
    POST /closer/alertas/run
    Header: X-Agent-Secret: <AGENT_SECRET>

    Configuración n8n: Cron trigger → todos los días → hora 18:00 (6pm hora RD = UTC-4 → 22:00 UTC)
    """
    if x_agent_secret != AGENT_SECRET:
        raise HTTPException(status_code=401, detail="Unauthorized")
    background_tasks.add_task(ejecutar_alertas_bg)
    return {"status": "iniciado", "mensaje": "Revisión de alertas iniciada"}


async def ejecutar_alertas_bg():
    print("[Closer] Cron alertas...")
    try:
        result  = supabase_admin.table("alertas_cliente").select("*").eq("activa", True).execute()
        alertas = result.data or []
        print(f"[Closer] {len(alertas)} alertas activas")
        for alerta in alertas:
            await procesar_alerta_individual(alerta)
            await asyncio.sleep(8)  # Anti-ban: pausa entre alertas outbound
    except Exception as e:
        print(f"[Closer] Error alertas: {e}")


async def procesar_alerta_individual(alerta: dict):
    """
    Revisa si hay procesos nuevos para el cliente y le manda un WhatsApp.
    Solo notifica procesos que NO le hayan sido notificados antes.
    """
    phone         = alerta.get("contact_phone")
    nombre        = alerta.get("contact_name", "")
    keywords      = alerta.get("keywords") or []
    instituciones = alerta.get("instituciones") or []

    if not phone:
        return
    if not keywords and not instituciones:
        return

    procesos = buscar_procesos_por_keywords(keywords, instituciones)
    if not procesos:
        print(f"[Closer] Sin procesos para alerta de {phone} — keywords: {keywords}")
        return

    ya_notificados = alerta.get("procesos_notificados") or []
    procesos_nuevos = [
        p for p in procesos
        if p.get("codigo_proceso") and p.get("codigo_proceso") not in ya_notificados
    ]

    if not procesos_nuevos:
        print(f"[Closer] Sin procesos NUEVOS para {phone} — ya notificados: {len(ya_notificados)}")
        return

    # Generar mensaje con IA
    mensaje = await generar_mensaje_alerta_proceso(nombre or phone, procesos_nuevos, keywords)
    if not mensaje:
        return

    await enviar_whatsapp(phone, mensaje, es_followup=True)  # Anti-ban: delay mayor para outbound

    # Marcar como notificados (máximo 50 en memoria para no crecer infinito)
    codigos_nuevos   = [p.get("codigo_proceso") for p in procesos_nuevos]
    actualizados     = list(set(ya_notificados + codigos_nuevos))[-50:]
    supabase_admin.table("alertas_cliente")         .update({
            "procesos_notificados": actualizados,
            "ultimo_check_at":      datetime.utcnow().isoformat()
        })         .eq("id", alerta["id"]).execute()

    # Guardar en historial de mensajes si hay conv_id
    conv_id = alerta.get("conversation_id")
    if conv_id:
        guardar_mensaje(str(conv_id), "agente", mensaje, generado_por_ia=True)

    print(f"[Closer] ✅ Alerta enviada a {phone}: {codigos_nuevos}")


# ═══════════════════════════════════════════════════════════════════════
# ENDPOINTS DE GESTIÓN
# ═══════════════════════════════════════════════════════════════════════

@closer_router.get("/conversaciones")
async def listar_conversaciones(
    x_agent_secret: Optional[str] = Header(None),
    estado: Optional[str]         = None,
    limit: int                    = 20
):
    if x_agent_secret != AGENT_SECRET:
        raise HTTPException(status_code=401, detail="Unauthorized")
    try:
        query = supabase_admin.table("conversaciones_closer") \
            .select("*") \
            .not_.in_("etapa", ["cerrado_ganado", "cerrado_perdido", "inactivo"]) \
            .order("ultimo_mensaje_en", desc=True) \
            .limit(limit)
        if estado:
            query = query.eq("estado", estado)
        result = query.execute()
        return {"conversaciones": result.data or [], "total": len(result.data or [])}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@closer_router.post("/marcar/{conv_id}")
async def marcar_conversacion(
    conv_id: str,
    payload: MarcarEtapaPayload,
    x_agent_secret: Optional[str] = Header(None)
):
    if x_agent_secret != AGENT_SECRET:
        raise HTTPException(status_code=401, detail="Unauthorized")

    etapas_validas = [
        "nuevo", "respondido", "interesado", "demo_agendada",
        "propuesta_enviada", "negociacion", "cerrado_ganado",
        "cerrado_perdido", "inactivo"
    ]
    if payload.etapa not in etapas_validas:
        raise HTTPException(status_code=400, detail=f"Etapa invalida. Opciones: {etapas_validas}")

    try:
        update = {"etapa": payload.etapa}
        if payload.notas:
            update["notas"] = payload.notas
        supabase_admin.table("conversaciones_closer").update(update).eq("id", conv_id).execute()
        return {"status": "ok", "etapa": payload.etapa, "conv_id": conv_id}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@closer_router.post("/interes/registrar")
async def registrar_interes_manual(
    payload: RegistrarInteresPayload,
    x_agent_secret: Optional[str] = Header(None)
):
    if x_agent_secret != AGENT_SECRET:
        raise HTTPException(status_code=401, detail="Unauthorized")

    conv    = obtener_o_crear_conversacion(payload.phone, payload.nombre)
    conv_id = conv.get("id")

    if payload.keywords or payload.instituciones:
        registrar_alerta_cliente(
            conv_id, payload.phone, payload.nombre or payload.phone,
            payload.keywords or [], payload.instituciones or []
        )
    if payload.notas:
        actualizar_perfil_prospecto(
            payload.phone, conv_id, payload.nombre or payload.phone,
            {"notas_agente": payload.notas}
        )
    return {"status": "ok", "conv_id": conv_id, "phone": payload.phone}


@closer_router.post("/alerta/test")
async def test_alerta(x_agent_secret: Optional[str] = Header(None)):
    if x_agent_secret != AGENT_SECRET:
        raise HTTPException(status_code=401, detail="Unauthorized")
    # Contar alertas activas y conversaciones
    try:
        alertas  = supabase_admin.table("alertas_cliente").select("id", count="exact").eq("activa", True).execute()
        convs    = supabase_admin.table("conversaciones_closer").select("id", count="exact").not_.in_("etapa", ["cerrado_ganado", "cerrado_perdido", "inactivo"]).execute()
        hot      = supabase_admin.table("conversaciones_closer").select("id", count="exact").eq("estado", "hot").execute()
        resumen  = (
            f"✅ Test Agente Closer OK\n\n"
            f"📊 Estado actual:\n"
            f"• Alertas activas: {alertas.count or 0}\n"
            f"• Conversaciones activas: {convs.count or 0}\n"
            f"• Hot leads: {hot.count or 0}\n\n"
            f"Sistema funcionando correctamente."
        )
        await enviar_telegram(resumen)
    except Exception as e:
        await enviar_telegram(f"Test Closer — Error: {e}")
    return {"status": "ok"}


@closer_router.get("/stats")
async def stats_closer(x_agent_secret: Optional[str] = Header(None)):
    """Estadísticas rápidas del agente para el panel."""
    if x_agent_secret != AGENT_SECRET:
        raise HTTPException(status_code=401, detail="Unauthorized")
    try:
        alertas  = supabase_admin.table("alertas_cliente").select("id", count="exact").eq("activa", True).execute()
        convs    = supabase_admin.table("conversaciones_closer").select("id, etapa, estado, nombre_contacto, score").not_.in_("etapa", ["cerrado_ganado", "cerrado_perdido", "inactivo"]).order("score", desc=True).limit(10).execute()
        hot      = [c for c in (convs.data or []) if c.get("estado") == "hot"]
        return {
            "alertas_activas":       alertas.count or 0,
            "conversaciones_activas": len(convs.data or []),
            "hot_leads":             len(hot),
            "top_leads":             convs.data or [],
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ═══════════════════════════════════════════════════════════════════════
# SCORING
# ═══════════════════════════════════════════════════════════════════════

def calcular_score_prospecto(perfil: dict, conv: dict) -> int:
    score = 0

    if perfil.get("nombre_empresa"):           score += 10
    if perfil.get("sector"):                   score += 5
    if perfil.get("tipo_empresa"):             score += 5
    if perfil.get("tiene_rpe"):                score += 10
    if perfil.get("tiene_rnce"):               score += 5
    if perfil.get("tiene_estados_financieros"): score += 5
    if perfil.get("ha_participado_antes"):      score += 10
    if int(perfil.get("procesos_ganados") or 0) > 0: score += 10
    if perfil.get("tipos_proceso"):            score += 10
    if perfil.get("instituciones_interes"):    score += 5

    etapa = conv.get("etapa", "")
    if etapa == "interesado":          score += 10
    elif etapa == "propuesta_enviada": score += 15
    elif etapa == "negociacion":       score += 20

    estado = conv.get("estado", "")
    if estado == "hot":     score += 10
    elif estado == "engaged": score += 5

    ultimo = conv.get("ultimo_mensaje_en")
    if ultimo:
        try:
            dt   = datetime.fromisoformat(ultimo.replace("Z", "+00:00"))
            dias = (datetime.now(dt.tzinfo) - dt).days
            if dias <= 1:   score += 10
            elif dias <= 3: score += 7
            elif dias <= 7: score += 3
        except Exception:
            pass

    return min(score, 100)


def emoji_score(score: int) -> str:
    if score >= 80: return "🔥"
    if score >= 60: return "⭐"
    if score >= 40: return "👀"
    return "🌱"


@closer_router.post("/scores/recalcular")
async def recalcular_scores(
    background_tasks: BackgroundTasks,
    x_agent_secret: Optional[str] = Header(None)
):
    if x_agent_secret != AGENT_SECRET:
        raise HTTPException(status_code=401, detail="Unauthorized")
    background_tasks.add_task(recalcular_scores_bg)
    return {"status": "iniciado"}


async def recalcular_scores_bg():
    print("[Closer] Recalculando scores...")
    try:
        convs_result = supabase_admin.table("conversaciones_closer") \
            .select("*") \
            .not_.in_("etapa", ["cerrado_ganado", "cerrado_perdido", "inactivo"]) \
            .execute()

        for conv in (convs_result.data or []):
            perfil = obtener_perfil_prospecto(conv.get("telefono")) or {}
            score  = calcular_score_prospecto(perfil, conv)
            supabase_admin.table("conversaciones_closer") \
                .update({"score": score}) \
                .eq("id", conv["id"]).execute()

        print(f"[Closer] Scores actualizados para {len(convs_result.data or [])} conversaciones")
    except Exception as e:
        print(f"[Closer] Error scores: {e}")


# ═══════════════════════════════════════════════════════════════════════
# RESUMEN DIARIO
# ═══════════════════════════════════════════════════════════════════════

@closer_router.post("/resumen/diario")
async def generar_resumen_diario(
    background_tasks: BackgroundTasks,
    x_agent_secret: Optional[str] = Header(None)
):
    if x_agent_secret != AGENT_SECRET:
        raise HTTPException(status_code=401, detail="Unauthorized")
    background_tasks.add_task(generar_resumen_diario_bg)
    return {"status": "iniciado"}


async def generar_resumen_diario_bg():
    try:
        hoy = datetime.utcnow().date().isoformat()

        msgs_hoy = supabase_admin.table("mensajes_closer") \
            .select("id", count="exact") \
            .gte("enviado_en", hoy) \
            .eq("rol", "cliente").execute()

        convs_activas = supabase_admin.table("conversaciones_closer") \
            .select("id, etapa, estado, nombre_contacto, telefono", count="exact") \
            .not_.in_("etapa", ["cerrado_ganado", "cerrado_perdido", "inactivo"]).execute()

        hot_leads  = [c for c in (convs_activas.data or []) if c.get("estado") == "hot"]

        nuevas_hoy = supabase_admin.table("conversaciones_closer") \
            .select("id", count="exact") \
            .gte("creado_en", hoy).execute()

        hot_lista = "\n".join([
            f"  - {c.get('nombre_contacto', '?')} ({c.get('telefono', '')})"
            for c in hot_leads[:5]
        ])

        resumen = (
            f"Resumen Agente Closer — {hoy}\n\n"
            f"Mensajes recibidos hoy: {msgs_hoy.count or 0}\n"
            f"Conversaciones nuevas: {nuevas_hoy.count or 0}\n"
            f"Conversaciones activas: {convs_activas.count or 0}\n"
            f"Hot leads: {len(hot_leads)}\n"
            + (f"\nLeads calientes:\n{hot_lista}" if hot_leads else "")
        )
        await enviar_telegram(resumen)
        print("[Closer] Resumen diario enviado")
    except Exception as e:
        print(f"[Closer] Error resumen: {e}")


# ═══════════════════════════════════════════════════════════════════════
# ANALISIS AUTOMATICO DE PLIEGOS
# ═══════════════════════════════════════════════════════════════════════

async def disparar_analisis_pliego_bg(codigo_proceso: str, phone: str, conv_id: str):
    """
    Dispara el análisis de un pliego que aún no está en BD.
    Usa el endpoint interno /api/procesos/{codigo}/analizar del main.py.
    Luego notifica al cliente por WhatsApp cuando esté listo.
    """
    try:
        print(f"[Closer] Disparando análisis automático de {codigo_proceso}")

        proceso = buscar_proceso_dgcp(codigo_proceso)
        if not proceso:
            await enviar_whatsapp(phone,
                f"No encontré el proceso {codigo_proceso} en nuestra base de datos. "
                f"¿Puedes confirmar el código exacto? 🔍"
            )
            return

        # URL interna del propio Railway service
        base_url = os.environ.get("APP_URL", "")
        if not base_url:
            railway_domain = os.environ.get("RAILWAY_PUBLIC_DOMAIN", "")
            base_url = f"https://{railway_domain}" if railway_domain else "http://localhost:8080"

        # Avisar al cliente que ya estamos en ello
        await enviar_whatsapp(phone,
            f"Perfecto, ya inicié el análisis del pliego {codigo_proceso} 🤖 "
            f"Dame unos minutos y te mando el resumen completo."
        )
        guardar_mensaje(conv_id, "agente",
            f"Análisis iniciado para {codigo_proceso} — esperando resultado...",
            generado_por_ia=False
        )

        # Llamar al endpoint de análisis del main.py
        # Necesitamos un user_id dummy para que el endpoint no falle
        # Usamos el endpoint admin que no requiere user_id
        async with httpx.AsyncClient(timeout=300) as client:
            resp = await client.post(
                f"{base_url}/api/procesos/{codigo_proceso}/analizar",
                params={"user_id": "closer-agent"},
                headers={"X-Admin-Key": os.environ.get("ADMIN_SECRET", "")}
            )
            print(f"[Closer] Análisis dispatch: {resp.status_code}")

        # Esperar hasta 5 minutos revisando cada 30s si el análisis completó
        for intento in range(10):
            await asyncio.sleep(30)
            analisis = buscar_analisis_pliego(codigo_proceso)
            if analisis and analisis.get("resumen_ejecutivo"):
                resumen = str(analisis.get("resumen_ejecutivo", ""))[:500]
                monto   = proceso.get("monto_estimado", 0)
                monto_fmt = f"RD${float(monto):,.0f}" if monto else "no publicado"
                fecha   = str(proceso.get("fecha_fin_recepcion_ofertas", "por confirmar"))[:10]

                mensaje = (
                    f"✅ Listo el análisis de *{codigo_proceso}*\n\n"
                    f"📋 *{proceso.get('titulo', '')}*\n"
                    f"🏛 {proceso.get('unidad_compra', '')}\n"
                    f"💰 {monto_fmt}\n"
                    f"📅 Cierre: {fecha}\n\n"
                    f"{resumen}\n\n"
                    f"Para ver el checklist completo, alertas de fraude y todos los requisitos, "
                    f"entra a: https://app.licitacionlab.com/ 🚀"
                )
                await enviar_whatsapp(phone, mensaje)
                guardar_mensaje(conv_id, "agente", mensaje, generado_por_ia=True)
                print(f"[Closer] Análisis entregado a {phone} — {codigo_proceso}")
                return

        # Si después de 5 min no completó
        await enviar_whatsapp(phone,
            f"El análisis de {codigo_proceso} está tardando más de lo normal. "
            f"Te lo envío en cuanto esté listo, ¡no te preocupes! 👌"
        )
    except Exception as e:
        print(f"[Closer] Error disparar_analisis_bg: {e}")
        await enviar_whatsapp(phone,
            f"Tuve un problema procesando el pliego {codigo_proceso}. "
            f"Avísale al Ing. Luis directamente para que te ayude."
        )
