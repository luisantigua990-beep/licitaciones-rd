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

SYSTEM_PROMPT = """Eres Licy, asistente del Ing. Luis Antigua — ingeniero civil dominicano con más de 8 años en licitaciones públicas del DGCP de República Dominicana. Licy es un hombre, así que habla en masculino siempre (ej: "estoy listo", "soy el asistente", nunca "lista" ni "la asistente").

Tu objetivo principal es VENDER EL SERVICIO DE ASESORÍA EN LICITACIONES, no solo informar.

PRESENTACIÓN (primera vez):
"Soy Licy, asistente del Ing. Luis Antigua 👋 Ayudamos a empresas dominicanas a ganar licitaciones públicas, desde buscar el proceso hasta la adjudicación. ¿En qué proceso estás trabajando o qué tipo de licitaciones te interesan?"

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
2. Longitud según lo que amerite: si el cliente pregunta por documentos, requisitos o información de un proceso, da la respuesta COMPLETA aunque sean 8-10 oraciones. Si es un saludo, 2 oraciones bastan. El objetivo es que el cliente entienda, no que el mensaje sea corto.
3. NUNCA inventes datos de procesos, montos ni fechas
4. SIEMPRE termina con una pregunta o CTA orientado a cotizar/contratar
6. Si ya tienes el perfil del cliente en el CONTEXTO, úsalo — no repitas preguntas
7. Si no puedes resolver algo, di que el Ing. Luis le escribe en breve
8. HONESTIDAD SOBRE MEMORIA: Si el HISTORIAL está vacío o no tienes contexto de conversaciones anteriores, NO digas "recuerdo perfectamente" ni inventes haber tenido una conversación previa. Di: "Disculpa, no tengo el contexto de nuestra conversación anterior. ¿Me recuerdas en qué proceso o rubro estabas interesado?"
9. SÉ CONCISO. Tus respuestas NUNCA deben exceder los 2 párrafos cortos. Si muestras una lista de procesos, no agregues texto innecesario abajo. Si hablas de más, el sistema cortará tu mensaje a la mitad.

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
    from datetime import timezone
    hora_utc = datetime.now(timezone.utc).hour
    hora_rd = (hora_utc - 4) % 24
    return not (hora_rd >= 23 or hora_rd < 8)


def _dentro_del_limite_hora() -> bool:
    global _mensajes_hora
    ahora    = datetime.utcnow()
    hace_1h  = ahora - timedelta(hours=1)
    _mensajes_hora = [t for t in _mensajes_hora if t > hace_1h]
    return len(_mensajes_hora) < MAX_MENSAJES_POR_HORA


async def _simular_typing(phone_clean: str, segundos: float):
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
        pass


async def enviar_whatsapp(phone: str, mensaje: str, es_followup: bool = False):
    import random

    if not ZAPI_INSTANCE_ID or not ZAPI_TOKEN:
        print(f"[Z-API] Sin config — msg para {phone}: {mensaje[:80]}")
        return

    if not _en_horario_permitido():
        from datetime import timezone as _tz
        hora_utc_ahora = datetime.now(_tz.utc)
        hora_rd        = (hora_utc_ahora.hour - 4) % 24

        if es_followup:
            print(f"[Z-API] 🌙 Followup fuera de horario ({hora_rd}h RD) — {phone} se enviará en el próximo cron")
            return
        else:
            hora_8am_utc = hora_utc_ahora.replace(hour=12, minute=0, second=0, microsecond=0)
            if hora_utc_ahora >= hora_8am_utc:
                hora_8am_utc = hora_8am_utc + timedelta(days=1)
            segundos_espera = (hora_8am_utc - hora_utc_ahora).total_seconds()
            print(f"[Z-API] 🌙 Mensaje reactivo fuera de horario ({hora_rd}h RD) — esperando {segundos_espera/3600:.1f}h hasta las 8am para {phone}")
            await asyncio.sleep(segundos_espera)

    if not _dentro_del_limite_hora():
        print(f"[Z-API] ⛔ Límite alcanzado — {phone} descartado")
        return

    phone_clean = phone.replace("+", "").replace("-", "").replace(" ", "")
    url     = f"https://api.z-api.io/instances/{ZAPI_INSTANCE_ID}/token/{ZAPI_TOKEN}/send-text"
    headers = {"Client-Token": ZAPI_CLIENT_TOKEN} if ZAPI_CLIENT_TOKEN else {}

    if es_followup:
        delay = random.uniform(5.0, 15.0)
    else:
        chars_por_seg = random.uniform(18, 30)
        delay_escritura = len(mensaje) / chars_por_seg
        delay = max(2.0, min(delay_escritura, 8.0))

    await _simular_typing(phone_clean, delay)
    await asyncio.sleep(delay)

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
                print(f"[Z-API] ✅ Enviado a {phone_clean}")
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
        if result.data: return result.data[0]

        result2 = supabase_admin.table("analisis_pliego") \
            .select("resumen_ejecutivo, alertas_fraude, checklist_categorizado, checklist_legal, restricciones_participacion, requisitos_experiencia, requisitos_financieros, garantias_exigidas, personal_y_equipos, plazos_clave, tipo_proceso, proceso_id") \
            .ilike("proceso_id", f"%{codigo_o_texto}%") \
            .limit(1).execute()
        if result2.data: return result2.data[0]
    except Exception as e:
        print(f"[Closer] Error analisis_pliego: {e}")
    return None


def buscar_proceso_dgcp(codigo_o_texto: str) -> Optional[dict]:
    """Busca un proceso específico por código o título. Prioriza procesos_activos (fecha futura)."""
    CAMPOS = "codigo_proceso, titulo, unidad_compra, monto_estimado, fecha_fin_recepcion_ofertas, estado_proceso"
    try:
        # Primero busca en la vista de activos (fecha_fin_recepcion_ofertas > ahora)
        result = supabase_admin.table("procesos_activos") \
            .select(CAMPOS) \
            .ilike("codigo_proceso", f"%{codigo_o_texto}%") \
            .gt("fecha_fin_recepcion_ofertas", datetime.utcnow().isoformat()) \
            .limit(1).execute()
        if result.data: return result.data[0]

        result2 = supabase_admin.table("procesos_activos") \
            .select(CAMPOS) \
            .ilike("titulo", f"%{codigo_o_texto}%") \
            .gt("fecha_fin_recepcion_ofertas", datetime.utcnow().isoformat()) \
            .limit(1).execute()
        if result2.data: return result2.data[0]

        # Fallback a tabla procesos completa (puede estar vencido)
        result3 = supabase_admin.table("procesos") \
            .select(CAMPOS) \
            .ilike("codigo_proceso", f"%{codigo_o_texto}%") \
            .limit(1).execute()
        if result3.data: return result3.data[0]
    except Exception as e:
        print(f"[Closer] Error buscar_proceso: {e}")
    return None


def buscar_procesos_por_keywords(familias: list = None, keywords: list = None, instituciones: list = None, monto_min: float = None) -> list:
    """
    Busca procesos activos usando la vista procesos_con_rubros.
    PRIORIDAD 1: familias UNSPSC (exacto, sin falsos positivos)
    PRIORIDAD 2: keywords en texto (fallback)
    Soporta filtro por institución combinado con cualquiera de los anteriores.
    """
    try:
        ahora = datetime.utcnow().isoformat()
        CAMPOS = "codigo_proceso, titulo, unidad_compra, monto_estimado, fecha_fin_recepcion_ofertas, estado_proceso"
        codigos_vistos: set = set()
        resultados_totales: list = []

        def _agregar(filas: list):
            for p in (filas or []):
                codigo = p.get("codigo_proceso")
                if codigo and codigo not in codigos_vistos:
                    codigos_vistos.add(codigo)
                    resultados_totales.append(p)

        fams  = [f for f in (familias or []) if f][:4]
        terms = [k for k in (keywords or []) if k][:4]
        insts = (instituciones or [])[:3]

        # ── BÚSQUEDA POR FAMILIA UNSPSC (precisa, sin falsos positivos) ──
        if fams:
            for fam in fams:
                q = (supabase_admin.table("procesos_con_rubros")
                     .select(CAMPOS)
                     .gt("fecha_fin_recepcion_ofertas", ahora)
                     .eq("familia_unspsc", fam))
                if monto_min:
                    q = q.gte("monto_estimado", monto_min)
                if insts:
                    q = q.ilike("unidad_compra", f"%{insts[0]}%")
                res = q.order("fecha_fin_recepcion_ofertas", desc=False).limit(8).execute()
                _agregar(res.data)
                if len(resultados_totales) >= 10:
                    break

        # ── BÚSQUEDA POR TEXTO (fallback cuando no hay familias) ──
        if not resultados_totales and terms:
            for kw in terms:
                or_filter = (
                    f"titulo.ilike.%{kw}%,"
                    f"descripcion_articulo.ilike.%{kw}%,"
                    f"objeto_proceso.ilike.%{kw}%"
                )
                q = (supabase_admin.table("procesos_con_rubros")
                     .select(CAMPOS)
                     .gt("fecha_fin_recepcion_ofertas", ahora)
                     .or_(or_filter))
                if monto_min:
                    q = q.gte("monto_estimado", monto_min)
                if insts:
                    q = q.ilike("unidad_compra", f"%{insts[0]}%")
                res = q.order("fecha_fin_recepcion_ofertas", desc=False).limit(8).execute()
                _agregar(res.data)

        # ── SOLO INSTITUCIÓN (sin rubro) ──
        if not resultados_totales and insts and not fams and not terms:
            for inst in insts:
                q = (supabase_admin.table("procesos_con_rubros")
                     .select(CAMPOS)
                     .gt("fecha_fin_recepcion_ofertas", ahora)
                     .ilike("unidad_compra", f"%{inst}%"))
                if monto_min:
                    q = q.gte("monto_estimado", monto_min)
                res = q.order("fecha_fin_recepcion_ofertas", desc=False).limit(8).execute()
                _agregar(res.data)

        try:
            resultados_totales.sort(key=lambda x: str(x.get("fecha_fin_recepcion_ofertas") or "9999"))
        except Exception:
            pass

        print(f"[Closer] buscar_procesos → {len(resultados_totales)} para fam={fams} kw={terms} inst={insts}")
        return resultados_totales[:5]

    except Exception as e:
        print(f"[Closer] Error buscar_procesos_keywords: {e}")
        return []


def obtener_o_crear_conversacion(phone: str, nombre: str = None) -> dict:
    try:
        result = supabase_admin.table("conversaciones_closer") \
            .select("*").eq("canal", "whatsapp").eq("telefono", phone) \
            .not_.in_("etapa", ["cerrado_ganado", "cerrado_perdido"]) \
            .order("creado_en", desc=True).limit(1).execute()

        if result.data:
            conv = result.data[0]
            supabase_admin.table("conversaciones_closer").update({"ultimo_mensaje_en": datetime.utcnow().isoformat()}).eq("id", conv["id"]).execute()
            return conv

        nueva = {
            "canal": "whatsapp", "contacto_id": phone, "telefono": phone, "nombre_contacto": nombre or "Desconocido",
            "etapa": "nuevo", "estado": "engaged", "ultimo_mensaje_en": datetime.utcnow().isoformat(),
            "proximo_followup_en": (datetime.utcnow() + timedelta(days=2)).isoformat(), "followups_enviados": 0,
        }
        res = supabase_admin.table("conversaciones_closer").insert(nueva).execute()
        if res.data:
            print(f"[Closer] ✅ Conversación creada id={res.data[0].get('id')} para {phone}")
            return res.data[0]
        else:
            print(f"[Closer] ⚠️ INSERT conversación no devolvió data para {phone} — historial no funcionará")
            return nueva
    except Exception as e:
        print(f"[Closer] ❌ Error obtener_o_crear_conversacion: {e}")
        return {"id": None, "etapa": "nuevo", "estado": "engaged", "followups_enviados": 0}


def obtener_historial(conversacion_id: str, limite: int = 10) -> list:
    if not conversacion_id: return []
    try:
        result = supabase_admin.table("mensajes_closer").select("rol, contenido, enviado_en").eq("conversacion_id", conversacion_id).order("enviado_en", desc=True).limit(limite).execute()
        return list(reversed(result.data or []))
    except Exception:
        return []


def guardar_mensaje(conversacion_id: str, rol: str, contenido: str, generado_por_ia: bool = False):
    if not conversacion_id: return
    try:
        supabase_admin.table("mensajes_closer").insert({
            "conversacion_id": conversacion_id, "rol": rol, "contenido": contenido, "canal": "whatsapp",
            "generado_por_ia": generado_por_ia, "enviado_en": datetime.utcnow().isoformat()
        }).execute()
    except Exception:
        pass


def obtener_perfil_prospecto(phone: str) -> Optional[dict]:
    try:
        result = supabase_admin.table("perfiles_prospectos").select("*").eq("contact_phone", phone).limit(1).execute()
        return result.data[0] if result.data else None
    except Exception:
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
    except Exception:
        pass


async def extraer_y_actualizar_perfil(mensaje: str, historial: list, phone: str, conv_id: str, nombre: str, perfil_actual: Optional[dict]):
    if not gemini_client: return
    perfil_json     = json.dumps(perfil_actual or {}, ensure_ascii=False, default=str)
    historial_texto = "\n".join([f"{'Cliente' if m['rol'] == 'cliente' else 'Lab'}: {m['contenido']}" for m in historial[-6:]])

    prompt = f"""Analiza esta conversación y extrae datos del perfil del prospecto dominicano.
    HISTORIAL: {historial_texto}
    MENSAJE ACTUAL: {mensaje}
    PERFIL YA CONOCIDO: {perfil_json}
    Extrae SOLO lo que puedas inferir con certeza. NO inventes nada. Devuelve SOLO JSON sin markdown:
    {{"nombre_empresa":null,"tipo_empresa":null,"sector":null,"provincia":null,"anos_experiencia":null,"tiene_estados_financieros":null,"anos_estados_financieros":null,"tiene_rnce":null,"tiene_rpe":null,"tipos_proceso":[],"instituciones_interes":[],"ha_participado_antes":null,"procesos_ganados":null,"notas_agente":null}}"""

    try:
        response = gemini_client.models.generate_content(
            model="gemini-2.5-flash", contents=prompt, config=types.GenerateContentConfig(max_output_tokens=400, temperature=0.1)
        )
        texto = response.text.strip().replace("```json", "").replace("```", "").strip()
        idx_ini, idx_fin = texto.find("{"), texto.rfind("}")
        if idx_ini == -1 or idx_fin == -1: return
        texto = texto[idx_ini:idx_fin + 1]
        try:
            datos = {k: v for k, v in json.loads(texto).items() if v is not None and v != []}
        except json.JSONDecodeError:
            import re as _re
            datos = {k: v for k, v in json.loads(_re.sub(r'[ -  ]', ' ', texto)).items() if v is not None and v != []}
        if datos:
            actualizar_perfil_prospecto(phone, conv_id, nombre, datos)
            if datos.get("tipos_proceso") or datos.get("instituciones_interes"):
                registrar_alerta_cliente(conv_id, phone, nombre, datos.get("tipos_proceso", []), datos.get("instituciones_interes", []))
    except Exception:
        pass


def construir_contexto_perfil(perfil: Optional[dict]) -> str:
    if not perfil: return ""
    campos = [("nombre_empresa", "Empresa"), ("tipo_empresa", "Tipo"), ("sector", "Sector"), ("provincia", "Provincia"), ("anos_experiencia", "Experiencia (años)"), ("notas_agente", "Notas")]
    bool_campos = [("tiene_estados_financieros", "Estados financieros"), ("tiene_rnce", "RNCE"), ("tiene_rpe", "RPE"), ("ha_participado_antes", "Ha participado antes")]
    partes = []
    for k, label in campos:
        if perfil.get(k): partes.append(f"{label}: {perfil[k]}")
    for k, label in bool_campos:
        if perfil.get(k) is not None: partes.append(f"{label}: {'si' if perfil[k] else 'no'}")
    if perfil.get("tipos_proceso"): partes.append(f"Tipos de proceso: {', '.join(perfil['tipos_proceso'])}")
    if perfil.get("instituciones_interes"): partes.append(f"Instituciones interes: {', '.join(perfil['instituciones_interes'])}")
    return ("PERFIL DEL CLIENTE:\n" + "\n".join(partes)) if partes else ""


def registrar_alerta_cliente(conv_id: str, phone: str, nombre: str, keywords: list, instituciones: list = None):
    try:
        existing = supabase_admin.table("alertas_cliente").select("id, keywords, instituciones").eq("contact_phone", phone).eq("activa", True).limit(1).execute()
        if existing.data:
            alerta         = existing.data[0]
            kws_nuevos     = list(set((alerta.get("keywords") or []) + keywords))
            insts_nuevas   = list(set((alerta.get("instituciones") or []) + (instituciones or [])))
            supabase_admin.table("alertas_cliente").update({"keywords": kws_nuevos, "instituciones": insts_nuevas}).eq("id", alerta["id"]).execute()
        else:
            supabase_admin.table("alertas_cliente").insert({
                "conversation_id": conv_id, "contact_phone": phone, "contact_name": nombre,
                "keywords": keywords, "instituciones": instituciones or [], "activa": True, "canal": "whatsapp"
            }).execute()
    except Exception:
        pass


# ═══════════════════════════════════════════════════════════════════════
# HELPERS — DETECCIÓN Y SEMÁNTICA
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
    msg = mensaje.lower().strip()

    # ── Mensajes completamente off-topic (no responder con ventas) ──────
    TEMAS_IRRELEVANTES = [
        "tengo hambre", "tengo sueño", "quiero dormir", "me voy a dormir",
        "tengo sed", "estoy cansado", "qué hora es", "que hora es",
        "cómo está el tiempo", "como está el tiempo", "chiste", "cuéntame un chiste",
        "cuéntame algo", "aburrid", "me aburro", "quiero hablar", "qué haces",
        "que haces", "cómo te llamas", "como te llamas", "eres un robot",
        "te quiero", "te amo", "eres linda", "eres bonita",
    ]
    if any(t in msg for t in TEMAS_IRRELEVANTES):
        return "fuera_de_tema"
    # Detecta mensajes muy cortos y claramente off-topic
    # CONSERVADOR: solo bloquea si no hay NINGUNA señal de negocio, rubro o institución
    palabras_negocio = [
        "proceso", "licitacion", "licitación", "oferta", "obra", "contrato",
        "precio", "costo", "servicio", "empresa", "dgcp", "pliego", "sobre",
        "presupuesto", "consultor", "licitar",
        "mivhed", "inapa", "mopc", "caasd", "minerd", "msp", "egehid", "fonper",
        "adn", "intrant", "coraasan", "mirex", "micm", "hacienda", "politur",
        "uasd", "intec", "unphu", "pucmm", "itse", "mescyt", "mhc",
        "lubricant", "aceite", "vehiculo", "medicament", "alimento", "vivere",
        "equipo", "material", "uniforme", "pintura", "combustible", "diesel",
        "computadora", "laptop", "mobiliario", "limpieza", "seguridad",
        "construccion", "remozamien", "rehabilitac", "mantenimient", "suministro",
        "herramienta", "ferreteria", "electrico", "plomeria",
        "tiro", "tiraron", "sacaron", "publicaron", "hay", "tienen", "busco",
        "andan", "salio", "lanzaron", "abrieron",
    ]
    if len(msg) < 25 and not any(p in msg for p in palabras_negocio) and \
       not any(s in msg for s in ["hola", "buenos", "buenas", "saludos"]):
        return "fuera_de_tema"

    if any(s in msg for s in ["hola", "buenos dias", "buenas tardes", "buenas noches", "buenas",
                               "saludos", "como estas", "cómo estás", "como estás", "cómo estas",
                               "hey", "qué más", "que mas", "buenas", "ey"]):
        if len(msg) < 40: return "saludo"
    if re.search(r'[A-Z]{2,10}-[A-Z]{2,5}-[A-Z]{2,5}-\d{4}-\d{4}', mensaje.upper()): return "consulta_proceso"
    if any(p in msg for p in ["cuanto cuesta", "cuánto cuesta", "precio", "cuanto cobran", "cuánto cobran", "como pago", "cómo pago", "plan", "suscripcion"]): return "pregunta_precio"
    if detectar_senal_cierre(mensaje): return "senal_cierre"
    if detectar_interes_alerta(mensaje): return "quiere_alerta"
    if any(p in msg for p in ["hay procesos", "hay proceso", "hay licitaciones", "hay licitacion",
                               "busco procesos", "busco proceso", "que procesos", "qué procesos",
                               "que proceso", "qué proceso", "existen procesos", "procesos de",
                               "licitaciones de", "hay algo de", "cual proceso", "cuál proceso",
                               "disponible", "activo"]):
        return "busqueda_procesos"
    if any(p in msg for p in ["consultoria", "consultoría", "preparar oferta", "ayuda con", "como preparo", "cómo preparo", "documentos para licitar"]): return "consulta_consultoria"
    if any(p in msg for p in ["lo hago yo", "lo hacemos nosotros", "lo voy a hacer", "no necesito", "ya tenemos experiencia", "podemos solos", "yo mismo", "mi equipo lo hace", "nos encargamos nosotros", "lo preparo yo"]): return "objecion_lo_hago_yo"
    return "consulta_general"


# ── Catálogo UNSPSC completo extraído de la BD (186 familias) ──────────────
# Mapea familia UNSPSC → etiqueta en español con sinónimos dominicanos reales
# Usado por el motor semántico para expandir búsquedas a todo el catálogo
CATALOGO_UNSPSC = {
    "10100000": "carnes productos carnicos pollo res cerdo embutidos salami longaniza",
    "10110000": "veterinaria medicamentos animales mascotas antiparasitario",
    "10120000": "alimento animal forraje maiz trigo avena peces ganado",
    "10150000": "viveres alimentos frescos verduras hortalizas frutas arroz ajo cebolla",
    "10160000": "flores plantas ornamentales arreglos florales",
    "10170000": "abonos fertilizantes agroquimicos nutrientes plantas",
    "10190000": "pesticidas herbicidas insecticidas baygon fumigacion",
    "11100000": "materiales abrasivos lija esmeril pulimento",
    "11110000": "arena agregados aridos materiales petreos gravilla",
    "11120000": "fibras naturales algodon telas materiales",
    "11150000": "hilos telas confeccion textiles costura",
    "11160000": "uniformes ropa laboral cortinas telas casimir banderas",
    "12130000": "senalizacion vial reflectivos marcas viales pintura carretera",
    "12140000": "gases industriales oxigeno acetileno agua destilada",
    "12160000": "aceites vegetales productos quimicos laboratorio acidos",
    "12170000": "pinturas barnices brochas rodillos pintura esmalte",
    "12180000": "adhesivos selladores pegamentos silicona impermeabilizante aceite lubricante",
    "12350000": "reactivos laboratorio productos quimicos acido muriatico cloro",
    "13100000": "plasticos resinas PVC polietileno tuberias plasticas",
    "13110000": "caucho goma mangueras tuberias flexibles",
    "14100000": "papel carton higienico servilletas",
    "14110000": "papel bond papel oficina papeleria resmas cuadernos libretas",
    "14120000": "carton empaques materiales impresos cajas embalaje",
    "15100000": "combustibles gasolina diesel GLP gas vales tickets combustible",
    "15110000": "gas propano butano cilindros GLP cocina",
    "15120000": "lubricantes aceite motor aceite hidraulico grasa lubricante fluido",
    "20110000": "equipos mineria perforacion extraccion",
    "21100000": "maquinaria industrial equipos manufactura procesamiento",
    "22100000": "maquinaria construccion equipos pesados excavadora retroexcavadora",
    "23100000": "herramientas manuales llaves destornilladores martillo",
    "23130000": "herramientas corte sierra taladro cortadora",
    "23150000": "herramientas industriales equipos taller",
    "23170000": "ferreteria piezas metalicas hardware tornillos",
    "24100000": "empaques envases cajas bolsas contenedores",
    "24110000": "contenedores almacenamiento depositos tanques",
    "24120000": "paletas estantes almacenamiento bodegas",
    "25100000": "vehiculos automoviles camiones camionetas pickup jeep bus",
    "25110000": "motocicletas bicicletas",
    "25170000": "repuestos autopartes vehiculos piezas accesorios carros",
    "25180000": "neumaticos gomas llantas",
    "26100000": "equipos electricos transformadores generadores plantas electricas",
    "26110000": "baterias acumuladores UPS energia electrica",
    "26120000": "materiales electricos cables alambres instalaciones electricas",
    "27110000": "herramientas ferreteria tuberias plomeria gasfiteria",
    "30100000": "estructuras metalicas hierro acero vigas columnas perfiles",
    "30110000": "tubos tuberias PVC hierro plomeria red hidraulica",
    "30120000": "escaleras andamios estructuras temporales plataformas",
    "30130000": "prefabricados bloques concreto adoquines losetas",
    "30150000": "maderas tablas madera construccion puertas madera",
    "30160000": "puertas ventanas carpinteria aluminio herrajes",
    "30170000": "ceramicas pisos azulejos revestimientos porcelanato",
    "30180000": "pinturas impermeabilizantes recubrimientos acabados",
    "30190000": "concreto cemento mezcla mortero",
    "30200000": "vidrios cristales espejos mamparas",
    "30220000": "impermeabilizacion techo laminas zinc cubierta",
    "31150000": "tornillos pernos fijaciones anclajes",
    "31160000": "valvulas accesorios tuberias plomeria conexiones",
    "31200000": "componentes mecanicos engranajes transmision",
    "31210000": "rodamientos cojinetes piezas mecanicas",
    "32100000": "componentes electronicos circuitos semiconductores",
    "32120000": "conectores cables electricos instalaciones",
    "39100000": "lamparas iluminacion focos LED luminarias",
    "39110000": "accesorios electricos tomacorrientes interruptores",
    "39120000": "equipos electricos UPS baterias estabilizadores reguladores",
    "40100000": "aires acondicionados refrigeracion split minisplit",
    "40140000": "equipos HVAC ventilacion climatizacion fan coil",
    "40150000": "tuberias plomeria sanitaria instalaciones hidraulicas",
    "40160000": "bombas hidraulicas equipos agua presion",
    "41100000": "equipos laboratorio instrumentos medicion analizadores",
    "41110000": "reactivos insumos laboratorio pruebas diagnostico",
    "41120000": "equipos cientificos medicion instrumentacion",
    "42120000": "camillas equipos hospitalarios mobiliario clinico",
    "42130000": "equipos diagnostico medico ecografo rayos X",
    "42140000": "material quirurgico instrumental medico bisturi",
    "42150000": "insumos medicos descartables guantes jeringas gasas",
    "42160000": "equipos radiologia imagenologia tomografia",
    "42180000": "material ortopedico protesis implantes",
    "42220000": "equipos odontologicos dental silla dental",
    "42270000": "equipos rehabilitacion fisioterapia",
    "42280000": "equipos optica oftalmologia lentes",
    "42290000": "equipos hospitalarios camas sillas ruedas camillas",
    "42310000": "mobiliario medico hospitalario muebles clinica",
    "43190000": "almacenamiento datos servidores storage",
    "43200000": "computadoras laptops equipos informaticos desktop",
    "43210000": "perifericos impresoras accesorios computadora escaner",
    "43220000": "redes telecomunicaciones switches routers firewall",
    "43230000": "software licencias sistemas informaticos ERP",
    "44100000": "mobiliario oficina escritorios sillas mesas",
    "44110000": "electrodomesticos equipos cocina nevera microondas",
    "44120000": "suministros oficina utiles papeleria toner cartucho",
    "45100000": "equipos fotografia camaras",
    "45110000": "equipos audio video proyector presentacion",
    "45120000": "televisores monitores pantallas",
    "46150000": "alarmas seguridad CCTV camaras vigilancia circuito cerrado",
    "46160000": "equipos bomberos emergencias extintores",
    "46170000": "equipos proteccion personal EPP seguridad cascos",
    "46180000": "armamento municiones seguridad publica policia",
    "46190000": "cascos chalecos proteccion policial equipo tactico",
    "47100000": "articulos limpieza generales",
    "47120000": "productos limpieza desinfectantes jabon detergente cloro",
    "47130000": "materiales aseo higiene papel higienico toalla",
    "48100000": "equipos cocina industrial gastronómicos",
    "48130000": "utensilios cocina cubiertos vajilla",
    "49100000": "equipos deportivos canchas implementos pelotas",
    "49120000": "juguetes juegos educativos recreacion",
    "49160000": "instrumentos musicales",
    "50100000": "frutas vegetales frescos alimentos mercado",
    "50110000": "cereales granos arroz maiz platano",
    "50120000": "proteinas carnes aves pescado mariscos",
    "50130000": "lacteos huevos queso leche yogur",
    "50150000": "aceites grasas comestibles manteca margarina",
    "50160000": "azucar dulces mermeladas conservas",
    "50170000": "condimentos especias sazon salsas",
    "50180000": "bebidas jugos refrescos agua botellones",
    "50190000": "enlatados conservas procesados latas",
    "50200000": "panaderia reposteria pan galletas harina",
    "50220000": "alimentos procesados preparados raciones",
    "51100000": "antibioticos antimicrobianos medicamentos farmacos",
    "51110000": "anestesicos sedantes medicamentos",
    "51120000": "cardiovasculares medicamentos corazon hipertension",
    "51130000": "vitaminas suplementos minerales multivitaminicos",
    "51140000": "analgesicos antiinflamatorios ibuprofeno acetaminofen",
    "51150000": "antiparasitarios antifungicos antimicóticos",
    "51160000": "respiratorios broncodilatadores asma",
    "51170000": "dermatologicos oftalmicos cremas gotas",
    "51180000": "vacunas biologicos inmunologicos",
    "51190000": "antidiabeticos hormonas insulina",
    "51200000": "oncologicos quimioterapia cancer",
    "51210000": "neurologicos psiquiatricos antidepresivos",
    "52120000": "articulos domesticos hogar enseres",
    "52130000": "electrodomesticos pequenos plancha secadora",
    "52140000": "muebles hogar sala comedor",
    "52150000": "ropa cama blancos textiles hogar sabanas",
    "53100000": "uniformes ropa laboral vestimenta institucional",
    "53110000": "ropa interior",
    "53120000": "calzado zapatos botas",
    "53130000": "accesorios ropa gorras mochilas bolsos",
    "55100000": "libros publicaciones textos educativos",
    "55120000": "materiales educativos didacticos",
    "56100000": "mobiliario muebles escritorios sillas oficina",
    "56110000": "mobiliario escolar educativo pupitres aulas",
    "60100000": "material arte diseno grafico",
    "60120000": "material educativo escolar",
    "70110000": "servicios agricolas ganaderia",
    "70140000": "servicios ambientales reforestacion medio ambiente",
    "70170000": "servicios veterinarios",
    "72100000": "construccion obras civiles remozamiento rehabilitacion edificacion infraestructura mejoramiento remodelacion",
    "72130000": "demolicion excavacion movimiento tierra",
    "73100000": "mantenimiento general",
    "73110000": "mantenimiento equipos maquinaria reparacion",
    "73120000": "mantenimiento instalaciones edificios",
    "73130000": "mantenimiento vehiculos mecanica taller",
    "73150000": "instalacion equipos sistemas",
    "73180000": "reparacion equipos electronicos",
    "76100000": "servicios limpieza aseo conserjeria",
    "76110000": "servicios jardineria",
    "76120000": "tratamiento agua saneamiento acueducto alcantarillado",
    "78100000": "flete transporte carga logistica",
    "78110000": "servicio transporte pasajeros bus ruta",
    "78140000": "mudanzas mensajeria courier paqueteria",
    "78180000": "servicio transporte logistica distribucion",
    "80100000": "consultoria asesoria gerencial gestion",
    "80110000": "recursos humanos nomina",
    "80120000": "contabilidad auditoria finanzas",
    "80130000": "marketing publicidad comunicacion",
    "80140000": "servicios profesionales consultoria tecnica",
    "81100000": "estudios ingenieria arquitectura diseno planos",
    "81110000": "servicios ingenieria supervision inspeccion supervision obras",
    "81140000": "topografia cartografia GIS",
    "82100000": "impresion publicidad material grafico",
    "82120000": "fotografia diseno grafico",
    "83110000": "servicios forestales plantacion",
    "84110000": "seguros polizas cobertura",
    "84130000": "servicios bancarios financieros prestamos",
    "85120000": "capacitacion formacion entrenamiento cursos",
    "85160000": "servicios educativos",
    "86100000": "servicios medicos salud clinica hospital",
    "90100000": "servicios comunidad sociales",
    "90110000": "servicios bienestar social",
    "91110000": "servicios defensa seguridad nacional",
    "92100000": "servicios electricidad energia",
    "92120000": "servicios agua alcantarillado",
    "93130000": "servicios fiscales tributarios",
}

# Texto comprimido del catálogo para inyectar en el prompt de Gemini
_CATALOGO_TEXTO = """10150000:viveres,arroz,alimentos,comida|10170000:abono,fertilizante|10190000:pesticida,insecticida,fumigacion|11110000:arena,agregado,gravilla|11160000:uniforme,ropa,tela,cortina|12170000:pintura,barniz,brocha|12350000:reactivo,quimico,cloro,acido|13100000:plastico,PVC,polietileno|13110000:caucho,manguera,goma|14110000:papel,papeleria,toner,resma,cartucho|15100000:combustible,gasolina,diesel,GLP,vale|15110000:gas,propano,cilindro|15120000:lubricante,aceite motor,grasa,hidraulico,15W40|20110000:mineria,perforacion|22100000:maquinaria,excavadora,equipo pesado|23100000:herramienta,llave,destornillador|23170000:ferreteria,tornillo,hardware|25100000:vehiculo,camion,camioneta,pickup,jeep,bus,automovil|25170000:repuesto,autoparte,pieza vehiculo|25180000:neumatico,goma,llanta|26100000:generador,transformador,planta electrica|26120000:cable,alambre,electrico|30100000:estructura metalica,hierro,acero,viga|30110000:tuberia,PVC,plomeria|30150000:madera,tabla|30160000:puerta,ventana,aluminio,herraje|30170000:ceramica,piso,azulejo,porcelanato|30180000:impermeabilizante,recubrimiento|30190000:concreto,cemento,mezcla|39100000:lampara,iluminacion,LED|39120000:UPS,bateria,estabilizador|40100000:aire acondicionado,split,refrigeracion|40160000:bomba hidraulica,agua|41110000:insumo laboratorio,reactivo|42150000:insumo medico,guante,jeringa,descartable|43200000:computadora,laptop,desktop|43210000:impresora,periferico,scanner|43220000:red,switch,router,telecomunicaciones|43230000:software,licencia,sistema|44100000:mobiliario,escritorio,silla,mueble|44120000:utiles,papeleria,toner,oficina,suministro|46150000:camara,CCTV,vigilancia,alarma|46170000:EPP,casco,chaleco,proteccion|47120000:limpieza,desinfectante,jabon,detergente|47130000:aseo,papel higienico,toalla|50100000:fruta,vegetal,alimento fresco|50110000:cereal,grano,arroz,maiz|50120000:carne,pollo,pescado,proteina|50130000:lacteo,leche,queso,huevo|51100000:antibiotico,medicamento,farmaco|51140000:analgesico,ibuprofeno,acetaminofen|51180000:vacuna,biologico|52150000:sabana,blancos,textil hogar|53100000:uniforme,ropa laboral,vestimenta|53120000:calzado,zapato,bota|55100000:libro,texto educativo|56100000:mobiliario,mueble,escritorio|56110000:mobiliario escolar,pupitre,aula|70140000:ambiental,reforestacion|72100000:construccion,obra,remozamiento,rehabilitacion,edificacion,infraestructura,mejoramiento,remodelacion|72130000:demolicion,excavacion,movimiento tierra|73110000:mantenimiento equipo,reparacion|73120000:mantenimiento edificio,instalacion|73130000:mantenimiento vehiculo,mecanica|73150000:instalacion sistema|76100000:limpieza,aseo,conserjeria|76110000:jardineria|76120000:agua,saneamiento,acueducto,alcantarillado|78100000:transporte,flete,carga|78110000:transporte pasajero,bus|78180000:logistica,distribucion|80100000:consultoria,asesoria|80140000:servicio profesional,tecnico|81100000:ingenieria,arquitectura,diseno,plano|81110000:supervision,inspeccion obra|82100000:impresion,publicidad|84110000:seguro,poliza|85120000:capacitacion,formacion,curso|86100000:servicio medico,salud"""


def _buscar_rubros_en_bd(termino: str, limite: int = 3) -> list:
    """Busca descripciones reales de artículos en la BD para enriquecer el contexto."""
    try:
        res = supabase_admin.table("articulos_proceso") \
            .select("descripcion_articulo") \
            .ilike("descripcion_articulo", f"%{termino}%") \
            .limit(limite).execute()
        return [r["descripcion_articulo"] for r in (res.data or [])]
    except Exception:
        return []


async def extraer_keywords_interes(mensaje: str):
    """
    Motor semántico robusto con catálogo UNSPSC completo (186 familias).
    Gemini recibe el catálogo real y genera sinónimos alineados con el vocabulario
    exacto que usa el DGCP en sus procesos y artículos.
    Funciona para CUALQUIER rubro del catálogo dominicano.
    """
    if not gemini_client:
        return [], []

    msg_lower = mensaje.lower().strip()
    # Señales mínimas para activar el motor — si no hay ninguna, no gastar tokens
    SEÑALES = [
        "proceso", "licitacion", "licitación", "obra", "construc", "servicio",
        "suministro", "contrat", "hay", "tienen", "busco", "tiro", "sacaron",
        "publicaron", "necesito", "busca", "procesos", "licitaciones", "andan",
        "inapa", "mopc", "mivhed", "caasd", "minerd", "egehid", "fonper",
        "lubric", "aceite", "vehiculo", "vehículo", "medicament", "alimento",
        "equipo", "material", "uniforme", "pintura", "combustible", "diesel",
        "computadora", "laptop", "mobiliario", "limpieza", "seguridad",
    ]
    tiene_señal = any(s in msg_lower for s in SEÑALES)
    # También activar si el mensaje parece una pregunta de búsqueda de rubros
    parece_busqueda = len(mensaje.split()) >= 3 or "?" in mensaje
    if not tiene_señal and not parece_busqueda:
        return [], []

    prompt = f"""Eres el motor de búsqueda del sistema LicitacionLab para licitaciones públicas de República Dominicana.

CATÁLOGO UNSPSC COMPLETO DEL DGCP (familia: términos en español):
{_CATALOGO_TEXTO}

MENSAJE DEL USUARIO: "{mensaje}"

Tu tarea: generar keywords PRECISAS para buscar en títulos y descripciones de artículos del DGCP.

INSTRUCCIONES:
1. Identifica qué busca el usuario y mapéalo a los códigos de familia UNSPSC del catálogo de arriba
2. Devuelve los códigos de familia exactos (8 dígitos) que correspondan — máximo 4 familias
3. También puedes incluir keywords de texto como fallback si no hay familia exacta
4. Detecta instituciones por nombre coloquial: MIVHED, MOPC, INAPA, CAASD, MINERD, MSP, ADN, EGEHID, FONPER, etc.

EJEMPLOS:
- "lubricantes" → familias: ["15120000"], keywords: []
- "construcción" → familias: ["72100000", "72130000"], keywords: []
- "medicamentos" → familias: ["51100000","51140000","51160000"], keywords: []
- "computadoras" → familias: ["43200000","43210000"], keywords: []
- "alimentos viveres" → familias: ["10150000","50100000","50110000"], keywords: []
- "uniformes" → familias: ["53100000","11160000"], keywords: []
- "MIVHED construccion" → familias: ["72100000"], instituciones: ["MIVHED"]

Devuelve SOLO JSON:
{{"familias": ["15120000"], "keywords": [], "instituciones": ["SIGLA"]}}"""

    try:
        response = gemini_client.models.generate_content(
            model="gemini-2.5-flash",
            contents=prompt,
            config=types.GenerateContentConfig(
                max_output_tokens=800,
                temperature=0.05
            )
        )
        raw = (response.text or "").strip()
        if not raw:
            print("[Motor Semántico] Gemini devolvió texto vacío")
            return [], [], []
        # Parseo robusto — quitar markdown si Gemini lo agrega
        raw = raw.replace("```json", "").replace("```", "").strip()
        idx_i, idx_f = raw.find("{"), raw.rfind("}")
        if idx_i == -1 or idx_f == -1:
            # JSON cortado — intentar cerrar manualmente
            if idx_i != -1 and '"keywords"' in raw:
                # Extraer keywords parciales de texto cortado
                import re as _re
                kws_found = _re.findall(r'"([a-záéíóúñ ]{3,25})"', raw[raw.find('"keywords"'):])
                if kws_found:
                    terminos = [k.lower().strip() for k in kws_found[:7] if k not in ('keywords','instituciones')]
                    print(f"[Motor Semántico] JSON cortado — recuperado parcial: kw={terminos}")
                    return [], terminos, []
            print(f"[Motor Semántico] Sin JSON en respuesta: {raw[:80]}")
            return [], [], []
        datos = json.loads(raw[idx_i:idx_f + 1])
        # Familias UNSPSC (códigos exactos de 8 dígitos)
        familias = [str(f).strip() for f in (datos.get("familias") or []) if str(f).strip().isdigit() and len(str(f).strip()) == 8][:4]
        # Keywords de texto como fallback
        terminos = [str(k).lower().strip() for k in (datos.get("keywords") or []) if k][:4]
        instituciones = [str(i).upper().strip() for i in (datos.get("instituciones") or []) if i]
        print(f"[Motor Semántico] familias={familias} | kw={terminos} | inst={instituciones}")
        return familias, terminos, instituciones
    except Exception as e:
        print(f"[Motor Semántico] Error: {e}")
        return [], [], []


# ═══════════════════════════════════════════════════════════════════════
# HELPERS — GEMINI RESPUESTA
# ═══════════════════════════════════════════════════════════════════════

async def generar_respuesta_gemini(mensaje_cliente: str, historial: list, contexto_adicional: str = "", intencion: str = "consulta_general") -> str:
    if not gemini_client: return "Disculpa, tuve un problema técnico. Escríbeme en un momento."
    historial_texto = ""
    for msg in historial[-8:]:
        rol = "Cliente" if msg["rol"] == "cliente" else "Lab (tú)"
        historial_texto += f"{rol}: {msg['contenido']}\n"

    instruccion_intencion = {
        "consulta_proceso":    "El cliente pregunta por un proceso específico. Analiza el CONTEXTO con los datos del proceso (monto, entidad, fecha cierre). Menciona UN riesgo concreto de descalificación. Luego pregunta: '¿Quieres que te cotice la preparación de la oferta para este proceso?' — aquí SÍ corresponde cotizar. NUNCA termines con coma.",
        "busqueda_procesos":   "REGLA ESTRICTA: MUESTRA INMEDIATAMENTE la lista de procesos que están en el CONTEXTO. Usa viñetas con código, entidad, monto y fecha. Si no hay procesos, dilo claramente. Después de mostrar la lista pregunta: '¿Te interesa alguno de estos procesos?' — NO ofrezcas cotización ni asesoría todavía, solo muestra los procesos y deja que el cliente elija. NUNCA termines con coma.",
        "pregunta_precio":     "Explica la asesoría y los dos esquemas de precio (por proceso / mensualidad + comisión). Cierra pidiendo el proceso específico. NUNCA termines con coma — siempre oración completa.",
        "quiere_alerta":       "Confirma exactamente qué quedó registrado (menciona el rubro). Di que cada día a las 6pm revisamos y si hay algo nuevo le avisamos. Cierra con pregunta. NUNCA termines con coma — siempre oración completa.",
        "senal_cierre":        "El cliente está listo. Da el siguiente paso: coordinar con el Ing. Luis para cotizar. Sé directo. NUNCA termines con coma — siempre oración completa.",
        "consulta_consultoria":"Explica el servicio con el slogan al final. Cierra preguntando si tiene un proceso en mente. NUNCA termines con coma — siempre oración completa.",
        "objecion_lo_hago_yo": "Aplica validación + riesgo. Ofrece al menos revisar la oferta antes de entregar. NUNCA termines con coma — siempre oración completa.",
        "saludo":              "IMPORTANTE: Responde el saludo de forma NATURAL primero (ej: '¡Bien gracias! ¿Y tú?'). Luego en un SEGUNDO párrafo separado por doble salto de línea, pregunta en qué puedo ayudarle hoy o qué tipo de licitaciones le interesan. NO vendas nada en el primer párrafo. Si el HISTORIAL tiene contexto previo, úsalo en el segundo párrafo.",
        "consulta_general":    "Responde con valor y cierra con pregunta orientada a cotizar. NUNCA termines con coma — siempre oración completa.",
    }.get(intencion, "Responde con valor, cierra con CTA hacia la consultoría. NUNCA termines con coma.")

    prompt = f"""{SYSTEM_PROMPT}

═══ HISTORIAL DE CONVERSACIÓN ═══
{historial_texto if historial_texto else "(conversación nueva — preséntate)"}

═══ CONTEXTO / DATOS DISPONIBLES ═══
{contexto_adicional if contexto_adicional else "(sin contexto adicional)"}

═══ INSTRUCCIÓN ESPECÍFICA PARA ESTE MENSAJE ═══
{instruccion_intencion}

═══ MENSAJE DEL CLIENTE ═══
{mensaje_cliente}

═══ TU RESPUESTA (solo el texto, sin comillas, sin explicaciones) ═══
REGLA ABSOLUTA: NUNCA termines con una coma o a mitad de oración. Si el mensaje es largo,
cierra la última idea con punto antes de terminar. Mensaje incompleto = respuesta inválida."""

    try:
        response = gemini_client.models.generate_content(
            model="gemini-2.5-flash",
            contents=prompt,
            config=types.GenerateContentConfig(max_output_tokens=2048, temperature=0.72)
        )
        texto = (response.text or "").strip()
        # Detectar y corregir mensajes cortados a mitad de oración
        CIERRES_VALIDOS = (".", "!", "?", "…", ")", "*", "🤝", "👋", "💪")
        if texto and not texto.endswith(CIERRES_VALIDOS) and len(texto) > 80:
            ultimo_cierre = max(
                texto.rfind(". "), texto.rfind("? "), texto.rfind("! "),
                texto.rfind(".\n"), texto.rfind("?\n"), texto.rfind("!\n"),
            )
            if ultimo_cierre > len(texto) * 0.5:
                texto = texto[:ultimo_cierre + 1].strip()
        return texto if texto else "Disculpa, tuve un problema técnico. Escríbeme en un momento."
    except Exception as e:
        print(f"[Gemini] Error: {e}")
        return "Disculpa, tuve un problema técnico. Escríbeme en un momento."


async def generar_followup_gemini(historial: list, nombre: str, paso: int, estado: str, proceso_codigo: str = None) -> str:
    nombre_corto = nombre.split()[0] if nombre else "amigo"
    if not gemini_client:
        fallbacks = {0: f"Hola {nombre_corto}, ¿pudiste revisar la información?", 1: f"{nombre_corto}, hay un par de procesos activos. ¿Me das 2 minutos?", 2: f"{nombre_corto}, empresas están ganando contratos con nosotros. ¿Cuándo hablamos?", 3: f"Hola {nombre_corto}, entiendo que estás ocupado. Cuando estés listo, aquí estamos. 🤝"}
        return fallbacks.get(paso, fallbacks[3])

    contextos_paso = {
        0: "DÍA 2 — Primer seguimiento. Reactiva con valor. Pregunta qué tipo de licitaciones busca.",
        1: "DÍA 5 — Segundo seguimiento. Crea URGENCIA real mencionando fechas próximas.",
        2: "DÍA 7 — Tercer seguimiento. PRUEBA SOCIAL + objeción de riesgo en documentación.",
        3: "DÍA 14 — Último intento. Deja la puerta abierta sin presión."
    }
    historial_texto = "".join([f"{'Cliente' if msg['rol'] == 'cliente' else 'Lab'}: {msg['contenido']}\n" for msg in historial[-6:]])

    proceso_info = ""
    if proceso_codigo:
        proceso = buscar_proceso_dgcp(proceso_codigo)
        if proceso:
            monto = proceso.get("monto_estimado", 0)
            proceso_info = f"Proceso: {proceso_codigo} — {proceso.get('titulo', '')} | {f'RD${float(monto):,.0f}' if monto else ''}"

    prompt = f"""{SYSTEM_PROMPT}\n═══ CONTEXTO DE FOLLOWUP ═══\n{contextos_paso.get(paso, contextos_paso[3])}\n{proceso_info}\nEstado: {estado}\n═══ HISTORIAL ═══\n{historial_texto}\n- Escríbele a {nombre_corto}\n- Español dominicano natural\n- Máximo 3-4 oraciones\n- Cierra con pregunta\n- Solo texto"""
    try:
        response = gemini_client.models.generate_content(model="gemini-2.5-flash", contents=prompt, config=types.GenerateContentConfig(max_output_tokens=400, temperature=0.78))
        return response.text.strip()
    except Exception:
        return f"Hola {nombre_corto}, ¿cómo vas con los temas de licitaciones?"


async def generar_mensaje_alerta_proceso(nombre: str, procesos: list, keywords: list) -> str:
    if not gemini_client or not procesos: return ""
    procesos_texto = "".join([f"- {p.get('titulo', '')} ({p.get('unidad_compra', '')}) — RD${float(p.get('monto_estimado', 0)):,.0f}\n" for p in procesos[:3]])
    prompt = f"{SYSTEM_PROMPT}\nNotifica a {nombre} sobre procesos para: {', '.join(keywords)}.\nProcesos:\n{procesos_texto}\n- Mensaje de WhatsApp emocionante\n- Máximo 5 oraciones\n- Solo texto"
    try:
        response = gemini_client.models.generate_content(model="gemini-2.5-flash", contents=prompt, config=types.GenerateContentConfig(max_output_tokens=600, temperature=0.7))
        return response.text.strip()
    except Exception:
        return ""


# ═══════════════════════════════════════════════════════════════════════
# ENDPOINT PRINCIPAL — WEBHOOK Z-API
# ═══════════════════════════════════════════════════════════════════════

@closer_router.post("/webhook")
async def recibir_mensaje_zapi(request: Request, background_tasks: BackgroundTasks):
    try:
        body = await request.json()
    except Exception: return {"status": "ok"}
    if body.get("fromMe", True): return {"status": "ok", "skipped": "outbound"}

    phone = body.get("phone", "") or body.get("from", "")
    if not phone or "-" in phone: return {"status": "ok"}
    phone = phone.replace("+", "").replace("@s.whatsapp.net", "").replace("@c.us", "")
    texto = body.get("text", {}).get("message", "") or body.get("message", "") or body.get("body", "") or ""

    audio_data = body.get("audio")
    if not texto and audio_data and isinstance(audio_data, dict):
        if audio_url := audio_data.get("audioUrl", ""):
            nombre = body.get("senderName", "") or body.get("pushName", "") or ""
            background_tasks.add_task(procesar_audio_bg, phone, audio_url, nombre)
            return {"status": "recibido", "processing": True, "tipo": "audio"}

    image_data = body.get("image")
    if image_data and isinstance(image_data, dict):
        if image_url := image_data.get("imageUrl", ""):
            caption = image_data.get("caption", "") or ""
            nombre  = body.get("senderName", "") or body.get("pushName", "") or ""
            background_tasks.add_task(procesar_imagen_bg, phone, image_url, caption, nombre)
            return {"status": "recibido", "processing": True, "tipo": "imagen"}

    if not texto: return {"status": "ok"}
    nombre = body.get("senderName", "") or body.get("pushName", "") or ""
    background_tasks.add_task(procesar_mensaje_bg, phone, texto, nombre)
    return {"status": "recibido", "processing": True}


async def procesar_imagen_bg(phone: str, image_url: str, caption: str = "", nombre: str = ""):
    print(f"[Closer] 🖼️ Imagen recibida de {phone} — analizando...")
    try:
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.get(image_url)
            if resp.status_code != 200: return
            image_bytes   = resp.content
            content_type  = resp.headers.get("content-type", "image/jpeg").split(";")[0].strip()

        if not gemini_client: return

        from google.genai import types as _types
        prompt_extraccion = (
            "Analiza esta imagen de un documento de licitación dominicana.\n"
            f"El cliente escribió: '{caption}'\n"
            'Devuelve SOLO un JSON: {"codigo_proceso": "XXXX-0000 o null", "titulo": "...", "entidad": "...", "monto": "...", "fecha_cierre": "..."}'
        )
        extraccion_resp = gemini_client.models.generate_content(
            model="gemini-2.5-flash",
            contents=[prompt_extraccion, _types.Part.from_bytes(data=image_bytes, mime_type=content_type)],
            config=_types.GenerateContentConfig(max_output_tokens=600, temperature=0.05)
        )
        raw = (extraccion_resp.text or "").strip().replace("```json", "").replace("```", "").strip()
        try: datos_imagen = json.loads(raw)
        except Exception: datos_imagen = {"otros_datos": raw}

        codigo_raw       = datos_imagen.get("codigo_proceso") or ""
        codigo_detectado = ""
        if codigo_raw and codigo_raw.upper() not in ("NULL", "NONE", "N/A", ""):
            match_codigo = re.search(r'[A-Z]{2,15}-[A-Z]{2,5}-[A-Z]{2,5}-\d{4}-\d{4}', codigo_raw.upper())
            if match_codigo: codigo_detectado = match_codigo.group()

        partes = [caption] if caption else []
        if codigo_detectado: partes.append(f"Proceso: {codigo_detectado.upper()}")
        mensaje_sintetizado = " | ".join(partes) if partes else "El cliente envió una imagen de licitación"
        if codigo_detectado: mensaje_sintetizado = f"{codigo_detectado.upper()} — {mensaje_sintetizado}"

        await procesar_mensaje_bg(phone, mensaje_sintetizado, nombre)
    except Exception as e:
        print(f"[Closer] Error imagen: {e}")


async def procesar_audio_bg(phone: str, audio_url: str, nombre: str = ""):
    print(f"[Closer] 🎤 Audio de {phone} — transcribiendo...")
    try:
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.get(audio_url)
            if resp.status_code != 200: return
            audio_bytes = resp.content
            content_type = resp.headers.get("content-type", "audio/ogg")

        if not gemini_client: return
        mime_gemini  = "audio/ogg" if "ogg" in content_type else content_type.split(";")[0].strip()

        from google.genai import types as _types
        transcripcion_resp = gemini_client.models.generate_content(
            model="gemini-2.5-flash",
            contents=["Transcribe este audio en español. Devuelve SOLO el texto.", _types.Part.from_bytes(data=audio_bytes, mime_type=mime_gemini)],
            config=_types.GenerateContentConfig(max_output_tokens=500, temperature=0.1)
        )
        texto = transcripcion_resp.text.strip() if transcripcion_resp.text else ""
        if texto: await procesar_mensaje_bg(phone, texto, nombre)
    except Exception as e:
        print(f"[Closer] Error audio: {e}")



def _dividir_respuesta_saludo(respuesta: str) -> list:
    """
    Divide la respuesta de saludo en dos mensajes separados para sonar más humano.
    Mensaje 1: saludo natural + "bien y tú?" o similar
    Mensaje 2: la pregunta de negocio
    """
    for sep in ["\n\n", "\n", "! ", "? "]:
        partes = respuesta.split(sep, 1)
        if len(partes) == 2:
            sufijo = "!" if sep == "! " else "?" if sep == "? " else ""
            p1 = partes[0].strip() + sufijo
            p2 = partes[1].strip()
            if len(p1) > 10 and len(p2) > 15:
                return [p1, p2]
    return [respuesta]

async def procesar_mensaje_bg(phone: str, mensaje: str, nombre: str = ""):
    print(f"[Closer] Procesando phone={phone} msg={mensaje[:60]}")

    conv    = obtener_o_crear_conversacion(phone, nombre)
    conv_id = conv.get("id")
    guardar_mensaje(conv_id, "cliente", mensaje)

    contexto_adicional = ""
    intencion          = detectar_intencion(mensaje)
    codigo_proceso     = detectar_proceso_en_mensaje(mensaje)
    familias: list     = []  # familias UNSPSC del motor semántico

    # ── FIX: Si el mensaje es completamente off-topic, no responder ──────
    if intencion == "fuera_de_tema":
        print(f"[Closer] 🚫 Mensaje off-topic de {phone} — sin respuesta: '{mensaje[:50]}'")
        return
    # ─────────────────────────────────────────────────────────────────────

    # --- Promover intención basada en motor semántico ---
    familias, keywords, instituciones = await extraer_keywords_interes(mensaje)
    if intencion in ["consulta_general", "saludo"] and (familias or keywords or instituciones):
        intencion = "busqueda_procesos"
    # -------------------------------------------------------------------------------
        print(f"[Closer] Intención promovida a busqueda_procesos por motor semántico")

    if not intencion == "consulta_proceso" and codigo_proceso:
        intencion = "consulta_proceso"

    if conv_id and intencion not in ("saludo", "consulta_general"):
        supabase_admin.table("conversaciones_closer").update({"intencion_detectada": intencion}).eq("id", conv_id).execute()

    # 1. El cliente menciona un código de proceso específico
    if codigo_proceso:
        analisis = buscar_analisis_pliego(codigo_proceso)
        if analisis and analisis.get("resumen_ejecutivo"):
            resumen       = str(analisis.get("resumen_ejecutivo", ""))[:500]
            proceso       = buscar_proceso_dgcp(codigo_proceso)
            monto_fmt     = f"RD${float(proceso.get('monto_estimado', 0)):,.0f}" if proceso and proceso.get("monto_estimado") else "no publicado"
            fecha_cierre  = str((proceso or {}).get("fecha_fin_recepcion_ofertas", ""))[:10]
            entidad       = (proceso or {}).get("unidad_compra", "")

            checklist_raw = analisis.get("checklist_categorizado") or analisis.get("checklist_legal") or {}
            docs_lista = ""
            if isinstance(checklist_raw, dict):
                for k, v in checklist_raw.items():
                    if isinstance(v, list) and v: docs_lista += f"\n [{k}] " + ", ".join([str(i)[:30] for i in v[:3]])

            contexto_adicional = (
                f"ANÁLISIS COMPLETO — {codigo_proceso}\n"
                f"Entidad: {entidad} | Monto: {monto_fmt} | Cierre: {fecha_cierre}\n\n"
                f"RESUMEN: {resumen}\n\n"
                f"DOCS CLAVE: {docs_lista}\n\n"
                "Usa esto como argumento de venta y ofrece la preparación de la oferta."
            )
        else:
            proceso = buscar_proceso_dgcp(codigo_proceso)
            if proceso:
                contexto_adicional = f"El proceso {codigo_proceso} está en cola de análisis. Dile que en minutos le mandas el resumen."
                asyncio.create_task(disparar_analisis_pliego_bg(codigo_proceso, phone, conv_id))
            else:
                contexto_adicional = f"Proceso {codigo_proceso} no encontrado. Pide que verifique el código."

        if "[Imagen analizada" in mensaje or "|" in mensaje:
            contexto_adicional += "\nEl cliente envió una IMAGEN de esto. Ofrece preparar la oferta directamente."

        if conv_id: supabase_admin.table("conversaciones_closer").update({"proceso_codigo": codigo_proceso}).eq("id", conv_id).execute()

    # 2. Cliente busca procesos sin código específico
    elif intencion == "busqueda_procesos":
        procesos_encontrados = buscar_procesos_por_keywords(familias, keywords, instituciones)
        if procesos_encontrados:
            lista = ""
            for p in procesos_encontrados[:5]:
                monto   = p.get("monto_estimado", 0)
                monto_f = f"RD${float(monto):,.0f}" if monto else "monto no publicado"
                fecha   = str(p.get("fecha_fin_recepcion_ofertas", "?"))[:10]
                codigo  = p.get("codigo_proceso", "Sin código")
                titulo  = p.get("titulo", "Sin título")[:90]
                entidad = p.get("unidad_compra", "Sin entidad")[:55]
                lista  += f"• *{codigo}*\n  📋 {titulo}\n  🏛 {entidad}\n  💰 {monto_f}\n  📅 Entrega: {fecha}\n\n"

            contexto_adicional = (
                f"🚨 MUESTRA ESTA LISTA AL CLIENTE EXACTAMENTE ASÍ (copia y pega):\n\n{lista}"
                "Después de la lista, pregúntale si quiere que preparemos la oferta para alguno de ellos. "
                "NO agregues texto antes de la lista, NO hagas preguntas antes de mostrarla."
            )
        else:
            contexto_adicional = (
                "No encontré procesos activos para esa búsqueda. "
                "Dile claramente que no hay procesos activos con esos criterios ahora mismo, "
                "pero que si quiere te deja sus datos y le avisas cuando aparezca algo."
            )

    # 3. Quiere alertas automáticas (AWAIT AÑADIDO)
    if detectar_interes_alerta(mensaje):
        if not keywords and not instituciones:
            familias_a, keywords_a, instituciones_a = await extraer_keywords_interes(mensaje)
            keywords = keywords_a or keywords
            instituciones = instituciones_a or instituciones
            familias = familias_a
        if keywords or instituciones:
            registrar_alerta_cliente(conv_id, phone, nombre, keywords, instituciones)
            contexto_adicional += f"\nACCIÓN TOMADA: Alerta registrada para {', '.join(keywords + instituciones)}. Confirmale que le avisarás."

    perfil = obtener_perfil_prospecto(phone)
    if contexto_perfil := construir_contexto_perfil(perfil):
        contexto_adicional = contexto_perfil + "\n\n" + contexto_adicional

    if intencion == "pregunta_precio":
        contexto_adicional += "\nDa los 3 planes de LicitacionLab con precios y cierra con CTA de registro."

    historial = obtener_historial(conv_id)
    respuesta = await generar_respuesta_gemini(mensaje, historial, contexto_adicional, intencion)

    asyncio.create_task(extraer_y_actualizar_perfil(mensaje, historial, phone, conv_id, nombre, perfil))

    # Para saludos: dividir en dos mensajes (más humano)
    if intencion == "saludo":
        partes = _dividir_respuesta_saludo(respuesta)
        for i, parte in enumerate(partes):
            guardar_mensaje(conv_id, "agente", parte, generado_por_ia=True)
            await enviar_whatsapp(phone, parte)
            if i < len(partes) - 1:
                await asyncio.sleep(3)  # pausa natural entre mensajes
    else:
        guardar_mensaje(conv_id, "agente", respuesta, generado_por_ia=True)
        await enviar_whatsapp(phone, respuesta)

    if detectar_senal_cierre(mensaje) or intencion == "senal_cierre":
        await enviar_telegram(f"🔥 SEÑAL DE CIERRE\n👤 {nombre or phone}\n📱 +{phone}\n💬 \"{mensaje[:150]}\"")
        if conv_id: supabase_admin.table("conversaciones_closer").update({"etapa": "interesado", "estado": "hot"}).eq("id", conv_id).execute()
    elif conv_id and conv.get("etapa") == "nuevo":
        supabase_admin.table("conversaciones_closer").update({"etapa": "respondido", "estado": "engaged"}).eq("id", conv_id).execute()
    elif conv_id:
        supabase_admin.table("conversaciones_closer").update({"estado": "engaged"}).eq("id", conv_id).execute()


# ═══════════════════════════════════════════════════════════════════════
# CRONS Y UTILIDADES (FOLLOWUPS, ALERTAS, ETC.)
# ═══════════════════════════════════════════════════════════════════════
# Todo el código de abajo (Followups, Alertas, Panel) se mantiene intacto

@closer_router.post("/followup/run")
async def ejecutar_followups(background_tasks: BackgroundTasks, x_agent_secret: Optional[str] = Header(None)):
    if x_agent_secret != AGENT_SECRET: raise HTTPException(status_code=401)
    background_tasks.add_task(ejecutar_followups_bg)
    return {"status": "iniciado"}

async def ejecutar_followups_bg():
    ahora  = datetime.utcnow().isoformat()
    result = supabase_admin.table("conversaciones_closer").select("*").lte("proximo_followup_en", ahora).not_.in_("etapa", ["cerrado_ganado", "cerrado_perdido", "inactivo"]).not_.in_("estado", ["hot", "cerrado", "perdido"]).lt("followups_enviados", 4).execute()
    for conv in (result.data or []):
        await procesar_followup_individual(conv)
        await asyncio.sleep(10)

DIAS_FOLLOWUP = [2, 3, 2, 7]
async def procesar_followup_individual(conv: dict):
    conv_id, phone, nombre = conv.get("id"), conv.get("telefono"), (conv.get("nombre_contacto") or "").split()[0] or "amigo"
    paso, estado = conv.get("followups_enviados", 0), conv.get("estado", "silent")
    if not phone: return
    if paso >= 4:
        supabase_admin.table("conversaciones_closer").update({"etapa": "inactivo", "estado": "perdido"}).eq("id", conv_id).execute()
        return
    
    supabase_admin.table("conversaciones_closer").update({"estado": "silent"}).eq("id", conv_id).execute()
    mensaje = await generar_followup_gemini(obtener_historial(conv_id, limite=6), nombre, paso, estado, conv.get("proceso_codigo"))
    await enviar_whatsapp(phone, mensaje, es_followup=True)
    guardar_mensaje(conv_id, "agente", mensaje, generado_por_ia=True)

    dias_espera = DIAS_FOLLOWUP[paso] if paso < len(DIAS_FOLLOWUP) else 7
    supabase_admin.table("conversaciones_closer").update({
        "followups_enviados": paso + 1, "proximo_followup_en": (datetime.utcnow() + timedelta(days=dias_espera)).isoformat(), "ultimo_mensaje_en": datetime.utcnow().isoformat()
    }).eq("id", conv_id).execute()

@closer_router.post("/alertas/run")
async def ejecutar_alertas(background_tasks: BackgroundTasks, x_agent_secret: Optional[str] = Header(None)):
    if x_agent_secret != AGENT_SECRET: raise HTTPException(status_code=401)
    background_tasks.add_task(ejecutar_alertas_bg)
    return {"status": "iniciado"}

async def ejecutar_alertas_bg():
    result = supabase_admin.table("alertas_cliente").select("*").eq("activa", True).execute()
    for alerta in (result.data or []):
        await procesar_alerta_individual(alerta)
        await asyncio.sleep(8)

async def procesar_alerta_individual(alerta: dict):
    phone, keywords, instituciones = alerta.get("contact_phone"), alerta.get("keywords") or [], alerta.get("instituciones") or []
    if not phone or (not keywords and not instituciones): return
    procesos = buscar_procesos_por_keywords([], keywords, instituciones)
    if not procesos: return

    ya_notificados = alerta.get("procesos_notificados") or []
    procesos_nuevos = [p for p in procesos if p.get("codigo_proceso") and p.get("codigo_proceso") not in ya_notificados]
    if not procesos_nuevos: return

    mensaje = await generar_mensaje_alerta_proceso(alerta.get("contact_name", phone), procesos_nuevos, keywords)
    if not mensaje: return
    await enviar_whatsapp(phone, mensaje, es_followup=True)

    codigos_nuevos = [p.get("codigo_proceso") for p in procesos_nuevos]
    supabase_admin.table("alertas_cliente").update({"procesos_notificados": list(set(ya_notificados + codigos_nuevos))[-50:]}).eq("id", alerta["id"]).execute()
    if alerta.get("conversation_id"): guardar_mensaje(str(alerta.get("conversation_id")), "agente", mensaje, True)

@closer_router.get("/conversaciones")
async def listar_conversaciones(x_agent_secret: Optional[str] = Header(None), estado: Optional[str] = None, limit: int = 20):
    if x_agent_secret != AGENT_SECRET: raise HTTPException(status_code=401)
    query = supabase_admin.table("conversaciones_closer").select("*").not_.in_("etapa", ["cerrado_ganado", "cerrado_perdido", "inactivo"]).order("ultimo_mensaje_en", desc=True).limit(limit)
    if estado: query = query.eq("estado", estado)
    result = query.execute()
    return {"conversaciones": result.data or [], "total": len(result.data or [])}

@closer_router.post("/marcar/{conv_id}")
async def marcar_conversacion(conv_id: str, payload: MarcarEtapaPayload, x_agent_secret: Optional[str] = Header(None)):
    if x_agent_secret != AGENT_SECRET: raise HTTPException(status_code=401)
    try:
        update = {"etapa": payload.etapa}
        if payload.notas: update["notas"] = payload.notas
        supabase_admin.table("conversaciones_closer").update(update).eq("id", conv_id).execute()
        return {"status": "ok"}
    except Exception as e: raise HTTPException(status_code=500, detail=str(e))

@closer_router.post("/alerta/test")
async def test_alerta(x_agent_secret: Optional[str] = Header(None)):
    if x_agent_secret != AGENT_SECRET: raise HTTPException(status_code=401)
    try:
        alertas = supabase_admin.table("alertas_cliente").select("id", count="exact").eq("activa", True).execute()
        convs   = supabase_admin.table("conversaciones_closer").select("id", count="exact").not_.in_("etapa", ["cerrado_ganado", "cerrado_perdido", "inactivo"]).execute()
        await enviar_telegram(f"✅ Test Closer OK\nAlertas: {alertas.count}\nConversaciones: {convs.count}")
    except Exception as e: await enviar_telegram(f"Error test: {e}")
    return {"status": "ok"}

@closer_router.post("/resumen/diario")
async def generar_resumen_diario(background_tasks: BackgroundTasks, x_agent_secret: Optional[str] = Header(None)):
    if x_agent_secret != AGENT_SECRET: raise HTTPException(status_code=401)
    async def generar_resumen_diario_bg():
        hoy = datetime.utcnow().date().isoformat()
        msgs = supabase_admin.table("mensajes_closer").select("id", count="exact").gte("enviado_en", hoy).eq("rol", "cliente").execute()
        await enviar_telegram(f"Resumen Diario — {hoy}\nMensajes: {msgs.count}")
    background_tasks.add_task(generar_resumen_diario_bg)
    return {"status": "iniciado"}

async def disparar_analisis_pliego_bg(codigo_proceso: str, phone: str, conv_id: str):
    try:
        proceso = buscar_proceso_dgcp(codigo_proceso)
        if not proceso: return
        base_url = os.environ.get("APP_URL", f"https://{os.environ.get('RAILWAY_PUBLIC_DOMAIN', 'localhost:8080')}")
        await enviar_whatsapp(phone, f"Ya inicié el análisis del pliego {codigo_proceso} 🤖. Dame unos minutos.")
        async with httpx.AsyncClient(timeout=300) as client:
            await client.post(f"{base_url}/api/procesos/{codigo_proceso}/analizar", params={"user_id": "closer-agent"}, headers={"X-Admin-Key": os.environ.get("ADMIN_SECRET", "")})
        for _ in range(10):
            await asyncio.sleep(30)
            if analisis := buscar_analisis_pliego(codigo_proceso):
                if analisis.get("resumen_ejecutivo"):
                    await enviar_whatsapp(phone, f"✅ Listo el análisis de *{codigo_proceso}*\n\n{str(analisis.get('resumen_ejecutivo'))[:500]}")
                    return
    except Exception: pass
