"""
router_closer.py
Agente Closer — Vendedor IA para LicitacionLab
Canal principal: WhatsApp vía Evolution API

Endpoints:
  POST /closer/webhook              — recibe mensajes desde Evolution API (WhatsApp)
  POST /closer/followup/run         — cron diario 9am: dispara followups pendientes
  POST /closer/alertas/run          — cron diario 8am: revisa procesos nuevos por cliente
  GET  /closer/conversaciones       — lista conversaciones activas (panel Telegram)
  POST /closer/marcar/{id}          — Lonny marca etapa manualmente
  POST /closer/alerta/test          — prueba que el módulo de alertas funciona

Flujo principal:
  Cliente escribe WA → Evolution API webhook → Gemini busca contexto →
  responde → guarda historial → detecta señal cierre → alerta Telegram

Flujo followup:
  n8n cron 9am → /closer/followup/run → Gemini genera mensaje contextual →
  Evolution API envía → actualiza estado en Supabase

Flujo alertas:
  n8n cron 8am → /closer/alertas/run → busca procesos nuevos por keywords →
  Gemini genera mensaje personalizado → Evolution API envía al cliente
"""

import os
import re
import json
import asyncio
import httpx
from datetime import datetime, timedelta
from typing import Optional, List

from fastapi import APIRouter, Header, HTTPException, BackgroundTasks, Request
from pydantic import BaseModel, Field
from supabase import create_client
from google import genai
from google.genai import types

# ── Config ─────────────────────────────────────────────────────────────
SUPABASE_URL        = os.environ["SUPABASE_URL"]
SUPABASE_KEY        = os.environ["SUPABASE_KEY"]
GEMINI_API_KEY      = os.environ.get("GEMINI_API_KEY", "")
AGENT_SECRET        = os.environ.get("AGENT_SECRET", "licitacionlab-growth-2026")
TELEGRAM_BOT_TOKEN  = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID    = os.environ.get("TELEGRAM_CHAT_ID", "817596333")

# Z-API — WhatsApp
ZAPI_INSTANCE_ID    = os.environ.get("ZAPI_INSTANCE_ID", "")
ZAPI_TOKEN          = os.environ.get("ZAPI_TOKEN", "")
ZAPI_CLIENT_TOKEN   = os.environ.get("ZAPI_CLIENT_TOKEN", "")

supabase       = create_client(SUPABASE_URL, SUPABASE_KEY)
supabase_admin = create_client(SUPABASE_URL, os.environ.get("SUPABASE_SERVICE_KEY", SUPABASE_KEY))

# Cliente Gemini
gemini_client = None
if GEMINI_API_KEY:
    gemini_client = genai.Client(api_key=GEMINI_API_KEY)
else:
    print("⚠️ GEMINI_API_KEY no configurada — el agente no puede generar respuestas")

closer_router = APIRouter(prefix="/closer", tags=["Agente Closer"])

# ── Señales de cierre que disparan alerta a Lonny ──────────────────────
SENALES_CIERRE = [
    "cuánto cuesta", "cuanto cuesta", "precio", "plan", "suscripción", "subscripcion",
    "cómo me registro", "como me registro", "quiero registrarme", "quiero contratar",
    "cómo funciona", "como funciona", "quiero probarlo", "quiero el servicio",
    "tienen demo", "puedo ver", "me interesa contratar", "cuándo empiezo",
    "cuando empiezo", "cómo pago", "como pago", "tiene versión gratis",
    "tiene version gratis", "qué incluye", "que incluye", "quiero una cotización",
    "quiero una cotizacion", "mándame propuesta", "mandame propuesta",
    "cuánto cobran", "cuanto cobran", "necesito el servicio",
]

# ── Keywords que indican interés en tipo de proceso ────────────────────
KEYWORDS_ALERTA = [
    "avísame", "avisame", "notifícame", "notificame", "me interesa saber",
    "cuando haya", "cuando salga", "si aparece", "si sale algo de",
    "estoy buscando procesos", "quiero saber de licitaciones de",
    "me interesa participar en", "busco licitaciones de",
]

# ── System prompt para Gemini ──────────────────────────────────────────
SYSTEM_PROMPT = """Eres el asistente de ventas del Ing. Luis Antigua — ingeniero civil dominicano con más de 15 años de experiencia en licitaciones públicas del DGCP (Dirección General de Contrataciones Públicas) de República Dominicana.

El Ing. Luis Antigua es fundador de LicitacionLab, una plataforma SaaS que monitorea licitaciones del DGCP y analiza pliegos con inteligencia artificial. También ofrece servicios de consultoría para la preparación y presentación de ofertas en procesos públicos (MOPC, CAASD, INAPA, MIDEREC, MINERD y más).

Tu nombre es "Lab" y hablas EN NOMBRE del Ing. Luis Antigua. Cuando te presentes, di: "Soy Lab, asistente del Ing. Luis Antigua."

SERVICIOS QUE OFRECE EL ING. LUIS ANTIGUA:
1. LicitacionLab (plataforma SaaS):
   - Explorador: RD$1,490/mes — monitoreo básico, alertas email
   - Competidor: RD$3,990/mes — análisis de pliego con IA, alertas Telegram
   - Ganador: RD$8,500/mes — todo incluido + soporte prioritario
   - Registro: https://app.licitacionlab.com/

2. Consultoría de licitaciones (servicio personalizado):
   - Preparación completa de ofertas técnicas y económicas
   - Análisis de pliegos y requisitos
   - Elaboración de APUs, memorias de cálculo, cronogramas
   - Para información de precios y disponibilidad, el Ing. Luis Antigua los atiende directamente

CAPACIDADES DE LICITACIONLAB:
- Monitoreo automático de todas las licitaciones DGCP en tiempo real
- Análisis de pliegos con IA (requisitos, alertas de fraude, checklist categorizado)
- Alertas instantáneas por email y Telegram cuando aparece tu sector
- Panel de seguimiento de procesos ganados/perdidos
- Score "¿Vale la pena?" por proceso

CONTEXTO DE REPÚBLICA DOMINICANA:
- Ley 340-06 y nueva Ley 47-25 de contrataciones públicas
- Portal DGCP: dgcp.gob.do
- Instituciones clave: MOPC, CAASD, INAPA, MIDEREC, MINERD, Ayuntamientos
- Modalidades: LPN, LPI, CP, CCC, SFO, CD
- Registros necesarios: RPE (Registro de Proveedores del Estado), RNCE

REGLAS DE COMUNICACIÓN:
1. Responde en español dominicano, natural y cercano — NUNCA robótico ni corporativo
2. Mensajes cortos: máximo 3-4 oraciones. WhatsApp no es un email.
3. Si preguntan por un proceso específico, usa el CONTEXTO provisto — nunca inventes datos
4. Si no tienes info de un proceso, ofrece analizarlo en LicitacionLab
5. Si piden cotización del servicio de consultoría, di que el Ing. Luis Antigua los contactará personalmente
6. Cierra siempre con una pregunta o CTA suave — nunca presiones
7. Usa emojis con moderación — uno o dos por mensaje máximo
8. Cuando detectes interés real en la plataforma, invita a registrarse gratis

PERFILAMIENTO NATURAL DEL CLIENTE (CRÍTICO):
Tu objetivo secundario es conocer al cliente para enviarle los procesos correctos.
Extrae esta info de forma NATURAL — NUNCA como formulario ni cuestionario:
- ¿A qué se dedica su empresa? ¿Construcción, ingeniería, suministros?
- ¿Qué tipo de licitaciones le interesan? (obras, servicios, bienes, consultoría)
- ¿Qué instituciones le interesan? (MOPC, CAASD, INAPA, Ayuntamientos, etc.)
- ¿Han participado antes en el DGCP?
- ¿Tienen RPE o RNCE? ¿Tienen estados financieros de los últimos 2 años?

CÓMO PERFILAR SIN PRESIONAR:
- Si dicen "quiero participar", pregunta naturalmente: "¿En qué tipo de procesos? ¿Obras, servicios o bienes?"
- Si mencionan una institución, confirma: "¿Trabajan principalmente con el MOPC?"
- Si parece empresa nueva: "¿Ya están inscritos en el RPE?"
- Si el CONTEXTO ya tiene su perfil, NO repitas preguntas ya respondidas
- Usa la info del perfil para respuestas más personalizadas y relevantes

TONO: Experto dominicano en contrataciones públicas, accesible, confiable, cercano. Como un colega que conoce el DGCP por dentro."""


# ═══════════════════════════════════════════════════════════════════════
# MODELOS PYDANTIC
# ═══════════════════════════════════════════════════════════════════════

class EvolutionWebhookPayload(BaseModel):
    """Payload que llega desde Evolution API cuando un cliente escribe"""
    event: Optional[str] = None
    instance: Optional[str] = None
    data: Optional[dict] = None

class MarcarEtapaPayload(BaseModel):
    etapa: str
    notas: Optional[str] = None


# ═══════════════════════════════════════════════════════════════════════
# HELPERS — ENVÍO DE MENSAJES
# ═══════════════════════════════════════════════════════════════════════

async def enviar_telegram(mensaje: str):
    """Envía alerta a Telegram de Lonny"""
    if not TELEGRAM_BOT_TOKEN:
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    async with httpx.AsyncClient(timeout=10) as client:
        try:
            await client.post(url, json={
                "chat_id": TELEGRAM_CHAT_ID,
                "text": mensaje,
                "parse_mode": "HTML"
            })
        except Exception as e:
            print(f"[Telegram] Error: {e}")


async def enviar_whatsapp(phone: str, mensaje: str):
    """
    Envía mensaje de WhatsApp vía Z-API.
    phone: número con código de país sin + (ej: 18091234567)
    """
    if not ZAPI_INSTANCE_ID or not ZAPI_TOKEN:
        print(f"[Z-API] Sin config — mensaje que se enviaría a {phone}: {mensaje[:80]}")
        return

    # Asegurar formato correcto del número
    phone_clean = phone.replace("+", "").replace("-", "").replace(" ", "")

    url = f"https://api.z-api.io/instances/{ZAPI_INSTANCE_ID}/token/{ZAPI_TOKEN}/send-text"
    headers = {"Client-Token": ZAPI_CLIENT_TOKEN} if ZAPI_CLIENT_TOKEN else {}

    async with httpx.AsyncClient(timeout=15) as client:
        try:
            resp = await client.post(url, headers=headers, json={
                "phone": phone_clean,
                "message": mensaje
            })
            if resp.status_code not in (200, 201):
                print(f"[Z-API] Error {resp.status_code}: {resp.text[:200]}")
            else:
                print(f"[Z-API] Mensaje enviado a {phone_clean}")
        except Exception as e:
            print(f"[Z-API] Error enviando: {e}")


# ═══════════════════════════════════════════════════════════════════════
# HELPERS — SUPABASE
# ═══════════════════════════════════════════════════════════════════════

def buscar_analisis_pliego(codigo_o_texto: str) -> Optional[dict]:
    """Busca análisis de pliego en Supabase por código o texto"""
    try:
        # Buscar por código exacto
        result = supabase_admin.table("analisis_pliego") \
            .select("resumen_ejecutivo, alertas_fraude, checklist_categorizado, plazos_clave, proceso_id") \
            .eq("proceso_id", codigo_o_texto) \
            .limit(1) \
            .execute()
        if result.data:
            return result.data[0]

        # Buscar por texto parcial en proceso_id
        result2 = supabase_admin.table("analisis_pliego") \
            .select("resumen_ejecutivo, alertas_fraude, checklist_categorizado, plazos_clave, proceso_id") \
            .ilike("proceso_id", f"%{codigo_o_texto}%") \
            .limit(1) \
            .execute()
        if result2.data:
            return result2.data[0]
    except Exception as e:
        print(f"[Closer] Error buscando analisis_pliego: {e}")
    return None


def buscar_proceso_dgcp(codigo_o_texto: str) -> Optional[dict]:
    """Busca proceso en la tabla procesos por código o título"""
    try:
        result = supabase_admin.table("procesos") \
            .select("codigo_proceso, titulo, unidad_compra, monto_estimado, fecha_fin_recepcion_ofertas, estado_proceso") \
            .ilike("codigo_proceso", f"%{codigo_o_texto}%") \
            .limit(1) \
            .execute()
        if result.data:
            return result.data[0]

        result2 = supabase_admin.table("procesos") \
            .select("codigo_proceso, titulo, unidad_compra, monto_estimado, fecha_fin_recepcion_ofertas, estado_proceso") \
            .ilike("titulo", f"%{codigo_o_texto}%") \
            .limit(1) \
            .execute()
        if result2.data:
            return result2.data[0]
    except Exception as e:
        print(f"[Closer] Error buscando proceso: {e}")
    return None


def buscar_procesos_por_keywords(keywords: list, instituciones: list = None, monto_min: float = None) -> list:
    """Busca procesos activos que coincidan con los intereses del cliente"""
    try:
        query = supabase_admin.table("procesos") \
            .select("codigo_proceso, titulo, unidad_compra, monto_estimado, fecha_fin_recepcion_ofertas") \
            .eq("estado_proceso", "Proceso publicado")

        if monto_min:
            query = query.gte("monto_estimado", monto_min)

        # Filtro por institución si aplica
        if instituciones:
            for inst in instituciones[:2]:  # máximo 2 filtros
                query = query.ilike("unidad_compra", f"%{inst}%")

        result = query.order("fecha_fin_recepcion_ofertas", desc=False).limit(5).execute()
        procesos = result.data or []

        # Filtrar por keywords en Python (más flexible)
        if keywords and procesos:
            filtrados = []
            for p in procesos:
                titulo = (p.get("titulo") or "").lower()
                for kw in keywords:
                    if kw.lower() in titulo:
                        filtrados.append(p)
                        break
            return filtrados if filtrados else procesos[:3]

        return procesos[:3]
    except Exception as e:
        print(f"[Closer] Error buscando procesos por keywords: {e}")
    return []


def obtener_o_crear_conversacion(phone: str, nombre: str = None) -> dict:
    """Obtiene conversación activa o crea una nueva"""
    try:
        result = supabase_admin.table("conversaciones_closer") \
            .select("*") \
            .eq("canal", "whatsapp") \
            .eq("telefono", phone) \
            .not_.in_("etapa", ["cerrado_ganado", "cerrado_perdido"]) \
            .order("creado_en", desc=True) \
            .limit(1) \
            .execute()

        if result.data:
            conv = result.data[0]
            supabase_admin.table("conversaciones_closer") \
                .update({"ultimo_mensaje_en": datetime.utcnow().isoformat()}) \
                .eq("id", conv["id"]) \
                .execute()
            return conv

        # Nueva conversación
        nueva = {
            "canal": "whatsapp",
            "contacto_id": phone,
            "telefono": phone,
            "nombre_contacto": nombre or "Desconocido",
            "etapa": "nuevo",
            "estado": "engaged",
            "ultimo_mensaje_en": datetime.utcnow().isoformat(),
            "proximo_followup_en": (datetime.utcnow() + timedelta(days=2)).isoformat(),
            "followups_enviados": 0,
        }
        res = supabase_admin.table("conversaciones_closer").insert(nueva).execute()
        return res.data[0] if res.data else nueva

    except Exception as e:
        print(f"[Closer] Error conversación: {e}")
        return {"id": None, "etapa": "nuevo", "estado": "engaged", "followups_enviados": 0}


def obtener_historial(conversacion_id: str, limite: int = 10) -> list:
    """Obtiene últimos N mensajes de la conversación en orden cronológico"""
    if not conversacion_id:
        return []
    try:
        result = supabase_admin.table("mensajes_closer") \
            .select("rol, contenido, enviado_en") \
            .eq("conversacion_id", conversacion_id) \
            .order("enviado_en", desc=True) \
            .limit(limite) \
            .execute()
        return list(reversed(result.data or []))
    except Exception as e:
        print(f"[Closer] Error historial: {e}")
        return []


def guardar_mensaje(conversacion_id: str, rol: str, contenido: str, generado_por_ia: bool = False):
    """Guarda mensaje en el historial"""
    if not conversacion_id:
        return
    try:
        supabase_admin.table("mensajes_closer").insert({
            "conversacion_id": conversacion_id,
            "rol": rol,
            "contenido": contenido,
            "canal": "whatsapp",
            "generado_por_ia": generado_por_ia,
            "enviado_en": datetime.utcnow().isoformat()
        }).execute()
    except Exception as e:
        print(f"[Closer] Error guardando mensaje: {e}")

def obtener_perfil_prospecto(phone: str) -> Optional[dict]:
    """Obtiene el perfil del prospecto si existe"""
    try:
        result = supabase_admin.table("perfiles_prospectos") \
            .select("*").eq("contact_phone", phone).limit(1).execute()
        return result.data[0] if result.data else None
    except Exception as e:
        print(f"[Closer] Error obteniendo perfil: {e}")
    return None


def actualizar_perfil_prospecto(phone: str, conv_id: str, nombre: str, datos: dict):
    """Crea o actualiza el perfil del prospecto"""
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
        print(f"[Closer] Perfil actualizado para {phone}")
    except Exception as e:
        print(f"[Closer] Error actualizando perfil: {e}")


async def extraer_y_actualizar_perfil(mensaje: str, historial: list, phone: str, conv_id: str, nombre: str, perfil_actual: Optional[dict]):
    """Extrae datos del perfil con Gemini y actualiza Supabase"""
    if not gemini_client:
        return
    perfil_json = json.dumps(perfil_actual or {}, ensure_ascii=False, default=str)
    historial_texto = "\n".join([f"{'Cliente' if m['rol']=='cliente' else 'Lab'}: {m['contenido']}" for m in historial[-6:]])
    prompt = f"""Analiza esta conversación y extrae datos del perfil del prospecto dominicano.

HISTORIAL:
{historial_texto}
MENSAJE ACTUAL: {mensaje}
PERFIL YA CONOCIDO: {perfil_json}

Extrae SOLO lo que puedas inferir con certeza. NO inventes nada. Devuelve SOLO JSON válido sin markdown:
{{"nombre_empresa":null,"tipo_empresa":null,"sector":null,"provincia":null,"anos_experiencia":null,"tiene_estados_financieros":null,"anos_estados_financieros":null,"tiene_rnce":null,"tiene_rpe":null,"tipos_proceso":[],"instituciones_interes":[],"ha_participado_antes":null,"procesos_ganados":null,"notas_agente":null}}"""
    try:
        response = gemini_client.models.generate_content(
            model="gemini-2.5-flash", contents=prompt,
            config=types.GenerateContentConfig(max_output_tokens=400, temperature=0.1)
        )
        texto = response.text.strip().replace("```json","").replace("```","")
        datos = {k: v for k, v in json.loads(texto).items() if v is not None and v != []}
        if datos:
            actualizar_perfil_prospecto(phone, conv_id, nombre, datos)
            if datos.get("tipos_proceso") or datos.get("instituciones_interes"):
                registrar_alerta_cliente(conv_id, phone, nombre, datos.get("tipos_proceso",[]), datos.get("instituciones_interes",[]))
    except Exception as e:
        print(f"[Closer] Error extrayendo perfil: {e}")


def construir_contexto_perfil(perfil: Optional[dict]) -> str:
    """Convierte el perfil en texto de contexto para Gemini"""
    if not perfil:
        return ""
    campos = [
        ("nombre_empresa", "Empresa"), ("tipo_empresa", "Tipo"), ("sector", "Sector"),
        ("provincia", "Provincia"), ("anos_experiencia", "Experiencia (años)"),
        ("notas_agente", "Notas"),
    ]
    bool_campos = [
        ("tiene_estados_financieros", "Estados financieros"),
        ("tiene_rnce", "RNCE"), ("tiene_rpe", "RPE"), ("ha_participado_antes", "Ha participado antes"),
    ]
    partes = []
    for k, label in campos:
        if perfil.get(k):
            partes.append(f"{label}: {perfil[k]}")
    for k, label in bool_campos:
        if perfil.get(k) is not None:
            partes.append(f"{label}: {'sí' if perfil[k] else 'no'}")
    if perfil.get("tipos_proceso"):
        partes.append(f"Tipos de proceso: {', '.join(perfil['tipos_proceso'])}")
    if perfil.get("instituciones_interes"):
        partes.append(f"Instituciones interés: {', '.join(perfil['instituciones_interes'])}")
    return ("PERFIL DEL CLIENTE:\n" + "\n".join(partes)) if partes else ""

def registrar_alerta_cliente(conv_id: str, phone: str, nombre: str, keywords: list, instituciones: list = None):
    """Registra o actualiza la alerta de procesos para un cliente"""
    try:
        # Verificar si ya existe una alerta para este número
        existing = supabase_admin.table("alertas_cliente") \
            .select("id, keywords, instituciones") \
            .eq("contact_phone", phone) \
            .eq("activa", True) \
            .limit(1) \
            .execute()

        if existing.data:
            # Actualizar keywords existentes (merge)
            alerta = existing.data[0]
            kws_actuales = alerta.get("keywords") or []
            kws_nuevos = list(set(kws_actuales + keywords))
            insts_actuales = alerta.get("instituciones") or []
            insts_nuevas = list(set(insts_actuales + (instituciones or [])))

            supabase_admin.table("alertas_cliente") \
                .update({"keywords": kws_nuevos, "instituciones": insts_nuevas}) \
                .eq("id", alerta["id"]) \
                .execute()
            print(f"[Closer] Alerta actualizada para {phone}: {kws_nuevos}")
        else:
            # Crear nueva alerta
            supabase_admin.table("alertas_cliente").insert({
                "conversation_id": conv_id,
                "contact_phone": phone,
                "contact_name": nombre,
                "keywords": keywords,
                "instituciones": instituciones or [],
                "activa": True,
                "canal": "whatsapp"
            }).execute()
            print(f"[Closer] Alerta creada para {phone}: {keywords}")
    except Exception as e:
        print(f"[Closer] Error registrando alerta: {e}")


# ═══════════════════════════════════════════════════════════════════════
# HELPERS — DETECCIÓN
# ═══════════════════════════════════════════════════════════════════════

def detectar_proceso_en_mensaje(mensaje: str) -> Optional[str]:
    """Detecta código de proceso DGCP en el mensaje"""
    patron = r'[A-Z]{2,10}-[A-Z]{2,5}-[A-Z]{2,5}-\d{4}-\d{4}'
    match = re.search(patron, mensaje.upper())
    return match.group() if match else None


def detectar_senal_cierre(mensaje: str) -> bool:
    """Detecta señal de intención de compra"""
    msg_lower = mensaje.lower()
    return any(senal in msg_lower for senal in SENALES_CIERRE)


def detectar_interes_alerta(mensaje: str) -> bool:
    """Detecta si el cliente quiere alertas de procesos"""
    msg_lower = mensaje.lower()
    return any(kw in msg_lower for kw in KEYWORDS_ALERTA)


def extraer_keywords_interes(mensaje: str) -> list:
    """
    Extrae palabras clave de interés del mensaje.
    Ej: "avísame de licitaciones de construcción del MOPC"
    → ['construcción', 'MOPC']
    """
    # Instituciones conocidas
    instituciones_conocidas = ["mopc", "caasd", "inapa", "miderec", "minerd",
                                "salud", "obras", "ayuntamiento", "intrant"]
    # Tipos de obra comunes
    tipos_obra = ["construcción", "construccion", "infraestructura", "alcantarillado",
                  "drenaje", "acueducto", "carretera", "puente", "edificio",
                  "rehabilitación", "rehabilitacion", "mantenimiento", "saneamiento",
                  "electricidad", "electricidad", "plomería", "plomeria",
                  "consultoría", "consultoria", "supervisión", "supervision"]

    msg_lower = mensaje.lower()
    keywords = []

    for tipo in tipos_obra:
        if tipo in msg_lower:
            keywords.append(tipo)

    instituciones = []
    for inst in instituciones_conocidas:
        if inst in msg_lower:
            instituciones.append(inst.upper())

    return keywords, instituciones


# ═══════════════════════════════════════════════════════════════════════
# HELPERS — GEMINI
# ═══════════════════════════════════════════════════════════════════════

async def generar_respuesta_gemini(
    mensaje_cliente: str,
    historial: list,
    contexto_adicional: str = ""
) -> str:
    """Genera respuesta conversacional usando Gemini 2.0 Flash"""
    if not gemini_client:
        return "Disculpa, tuve un problema técnico. Escríbeme en un momento. 🙏"

    # Construir historial como texto para el prompt
    historial_texto = ""
    for msg in historial[-8:]:
        rol = "Cliente" if msg["rol"] == "cliente" else "Lab (tú)"
        historial_texto += f"{rol}: {msg['contenido']}\n"

    prompt = f"""{SYSTEM_PROMPT}

--- HISTORIAL DE CONVERSACIÓN ---
{historial_texto if historial_texto else "(conversación nueva)"}

--- CONTEXTO ADICIONAL ---
{contexto_adicional if contexto_adicional else "(ninguno)"}

--- MENSAJE ACTUAL DEL CLIENTE ---
{mensaje_cliente}

--- TU RESPUESTA (solo el texto, sin comillas, sin explicaciones) ---"""

    try:
        response = gemini_client.models.generate_content(
            model="gemini-2.5-flash",
            contents=prompt,
            config=types.GenerateContentConfig(
                max_output_tokens=300,
                temperature=0.75,
            )
        )
        return response.text.strip()
    except Exception as e:
        print(f"[Gemini] Error generando respuesta: {e}")
        return "Disculpa, tuve un problema técnico. Escríbeme en un momento. 🙏"


async def generar_followup_gemini(
    historial: list,
    nombre: str,
    paso: int,
    estado: str,
    proceso_codigo: str = None
) -> str:
    """Genera mensaje de followup contextual con Gemini según el paso y estado"""
    if not gemini_client:
        return f"Hola {nombre}, ¿pudiste revisar la información que te compartí? 👋"

    contextos_paso = {
        0: "Es el primer followup (día 2). El cliente no ha respondido o respondió pero no avanzó. Muestra valor con información útil sobre procesos activos o el servicio.",
        1: "Es el segundo followup (día 5). Crea urgencia suave mencionando fechas próximas de procesos o una ventaja competitiva de LicitacionLab.",
        2: "Es el tercer followup (día 7). Usa prueba social o un caso de éxito genérico de empresas dominicanas que ganan licitaciones con apoyo.",
        3: "Es el cuarto y último followup (día 14). Mensaje de despedida amigable, deja la puerta abierta, sin presión.",
    }

    historial_texto = ""
    for msg in historial[-6:]:
        rol = "Cliente" if msg["rol"] == "cliente" else "Lab"
        historial_texto += f"{rol}: {msg['contenido']}\n"

    proceso_info = ""
    if proceso_codigo:
        proceso = buscar_proceso_dgcp(proceso_codigo)
        if proceso:
            proceso_info = f"El cliente preguntó por el proceso {proceso_codigo} — {proceso.get('titulo', '')} de {proceso.get('unidad_compra', '')}."

    prompt = f"""{SYSTEM_PROMPT}

Estás enviando un mensaje de seguimiento a {nombre}.
Contexto del paso: {contextos_paso.get(paso, contextos_paso[3])}
Estado del cliente: {estado}
{proceso_info}

Historial reciente:
{historial_texto if historial_texto else "(sin historial previo)"}

Instrucciones:
- Escribe UN solo mensaje de WhatsApp, natural, en español dominicano
- Máximo 3 oraciones
- No menciones que es un "seguimiento" o "followup"
- Cierra con una pregunta abierta o CTA suave
- Solo devuelve el texto del mensaje, sin comillas"""

    try:
        response = gemini_client.models.generate_content(
            model="gemini-2.5-flash",
            contents=prompt,
            config=types.GenerateContentConfig(
                max_output_tokens=200,
                temperature=0.8,
            )
        )
        return response.text.strip()
    except Exception as e:
        print(f"[Gemini] Error generando followup: {e}")
        return f"Hola {nombre}, ¿cómo vas con los temas de licitaciones? 👋"


async def generar_mensaje_alerta_proceso(
    nombre: str,
    procesos: list,
    keywords: list
) -> str:
    """Genera mensaje personalizado cuando hay procesos nuevos para el cliente"""
    if not gemini_client or not procesos:
        return ""

    procesos_texto = ""
    for p in procesos[:3]:
        monto = p.get("monto_estimado", 0)
        monto_fmt = f"RD${float(monto):,.0f}" if monto else "monto no publicado"
        fecha = p.get("fecha_fin_recepcion_ofertas", "fecha por confirmar")
        if fecha and len(str(fecha)) > 10:
            fecha = str(fecha)[:10]
        procesos_texto += f"- {p.get('titulo', 'Sin título')} ({p.get('unidad_compra', '')}) — {monto_fmt} — cierra {fecha}\n"

    prompt = f"""{SYSTEM_PROMPT}

Tienes que notificar a {nombre} sobre procesos nuevos que coinciden con sus intereses: {', '.join(keywords)}.

Procesos encontrados:
{procesos_texto}

Instrucciones:
- Escribe un mensaje de WhatsApp emocionante pero no exagerado
- Menciona los procesos de forma concisa (título + monto + fecha cierre)
- Al final pregunta si quieres que analice alguno de los pliegos
- Máximo 5 oraciones en total
- Solo el texto del mensaje, sin comillas"""

    try:
        response = gemini_client.models.generate_content(
            model="gemini-2.5-flash",
            contents=prompt,
            config=types.GenerateContentConfig(
                max_output_tokens=300,
                temperature=0.7,
            )
        )
        return response.text.strip()
    except Exception as e:
        print(f"[Gemini] Error generando alerta proceso: {e}")
        return ""


# ═══════════════════════════════════════════════════════════════════════
# ENDPOINT PRINCIPAL — WEBHOOK EVOLUTION API
# ═══════════════════════════════════════════════════════════════════════

@closer_router.post("/webhook")
async def recibir_mensaje_zapi(
    request: Request,
    background_tasks: BackgroundTasks,
):
    """
    Webhook principal — Z-API llama este endpoint cuando llega un mensaje de WhatsApp.
    Responde rápido (< 1s) y procesa en background.
    """
    try:
        body = await request.json()
    except Exception:
        return {"status": "ok"}

    # Z-API payload structure
    # Solo procesar mensajes entrantes (fromMe=false)
    if body.get("fromMe", True):
        return {"status": "ok", "skipped": "outbound"}

    # Ignorar mensajes de grupos
    phone = body.get("phone", "") or body.get("from", "")
    if not phone or "-" in phone:  # grupos tienen formato número-timestamp
        return {"status": "ok", "skipped": "group_or_no_phone"}

    # Limpiar número
    phone = phone.replace("+", "").replace("@s.whatsapp.net", "").replace("@c.us", "")

    # Extraer texto del mensaje
    texto = (
        body.get("text", {}).get("message", "") or
        body.get("message", "") or
        body.get("body", "") or
        ""
    )

    if not texto:
        return {"status": "ok", "skipped": "no_text"}

    # Nombre del contacto
    nombre = body.get("senderName", "") or body.get("pushName", "") or ""

    background_tasks.add_task(procesar_mensaje_bg, phone, texto, nombre)
    return {"status": "recibido", "processing": True}


async def procesar_mensaje_bg(phone: str, mensaje: str, nombre: str = ""):
    """Procesa el mensaje en background"""
    print(f"[Closer] Procesando — phone={phone}, msg={mensaje[:60]}")

    # 1. Obtener/crear conversación
    conv = obtener_o_crear_conversacion(phone, nombre)
    conv_id = conv.get("id")

    # 2. Guardar mensaje del cliente
    guardar_mensaje(conv_id, "cliente", mensaje)

    # 3. Detectar si menciona código de proceso DGCP
    contexto_adicional = ""
    codigo_proceso = detectar_proceso_en_mensaje(mensaje)

    if codigo_proceso:
        analisis = buscar_analisis_pliego(codigo_proceso)
        if analisis:
            resumen = str(analisis.get("resumen_ejecutivo", ""))[:600]
            contexto_adicional = f"""ANÁLISIS DISPONIBLE del proceso {codigo_proceso}:
{resumen}
Usa esta info para responder y menciona que en LicitacionLab pueden ver el análisis completo."""
        else:
            proceso = buscar_proceso_dgcp(codigo_proceso)
            if proceso:
                monto = proceso.get("monto_estimado", 0)
                monto_fmt = f"RD${float(monto):,.0f}" if monto else "no publicado"
                contexto_adicional = f"""DATOS del proceso {codigo_proceso}:
- Entidad: {proceso.get('unidad_compra', '—')}
- Título: {proceso.get('titulo', '—')}
- Monto estimado: {monto_fmt}
- Estado: {proceso.get('estado_proceso', '—')}
- Fecha límite ofertas: {proceso.get('fecha_fin_recepcion_ofertas', '—')}
Menciona que pueden ver el análisis completo del pliego en LicitacionLab."""
            else:
                contexto_adicional = f"El cliente pregunta por {codigo_proceso}. No está en nuestra base de datos. Ofrece analizarlo."

        # Guardar código de proceso en la conversación
        if conv_id:
            supabase_admin.table("conversaciones_closer") \
                .update({"proceso_codigo": codigo_proceso}) \
                .eq("id", conv_id) \
                .execute()

    # 4. Detectar si el cliente quiere alertas de procesos
    if detectar_interes_alerta(mensaje):
        keywords, instituciones = extraer_keywords_interes(mensaje)
        if keywords or instituciones:
            registrar_alerta_cliente(conv_id, phone, nombre, keywords, instituciones)
            kw_texto = ", ".join(keywords + instituciones)
            contexto_adicional += f"\nEl cliente acaba de pedir alertas sobre: {kw_texto}. Confírmale que lo tienes anotado y que le avisarás cuando aparezcan procesos de ese tipo."

    # 5. Obtener perfil del prospecto y agregarlo al contexto
    perfil = obtener_perfil_prospecto(phone)
    contexto_perfil = construir_contexto_perfil(perfil)
    if contexto_perfil:
        contexto_adicional = contexto_perfil + chr(10) + chr(10) + contexto_adicional

    # 6. Obtener historial
    historial = obtener_historial(conv_id)

    # 7. Generar respuesta con Gemini
    respuesta = await generar_respuesta_gemini(mensaje, historial, contexto_adicional)

    # 7b. Extraer perfil en background (no bloquea la respuesta)
    import asyncio
    asyncio.create_task(extraer_y_actualizar_perfil(mensaje, historial, phone, conv_id, nombre, perfil))

    # 8. Guardar respuesta del agente
    guardar_mensaje(conv_id, "agente", respuesta, generado_por_ia=True)

    # 8. Enviar respuesta al cliente vía Evolution API
    await enviar_whatsapp(phone, respuesta)

    # 9. Detectar señal de cierre → alerta Telegram + actualizar estado
    if detectar_senal_cierre(mensaje):
        nombre_display = nombre or phone
        alerta = f"""🔥 <b>SEÑAL DE CIERRE DETECTADA</b>

👤 <b>Contacto:</b> {nombre_display}
📱 <b>WhatsApp:</b> {phone}
💬 <b>Mensaje:</b> {mensaje[:200]}

🤖 <b>Respuesta del agente:</b>
{respuesta[:200]}

<i>👆 Entra tú a cerrar la venta.</i>"""
        await enviar_telegram(alerta)

        if conv_id:
            supabase_admin.table("conversaciones_closer") \
                .update({"etapa": "interesado", "estado": "hot"}) \
                .eq("id", conv_id) \
                .execute()

    # 10. Actualizar estado y próximo followup si es conversación nueva
    elif conv_id and conv.get("etapa") == "nuevo":
        supabase_admin.table("conversaciones_closer") \
            .update({
                "etapa": "respondido",
                "estado": "engaged",
                "proximo_followup_en": (datetime.utcnow() + timedelta(days=2)).isoformat()
            }) \
            .eq("id", conv_id) \
            .execute()
    elif conv_id:
        # Cliente respondió — resetear el contador de followup si estaba en silent
        supabase_admin.table("conversaciones_closer") \
            .update({
                "estado": "engaged",
                "ultimo_mensaje_en": datetime.utcnow().isoformat()
            }) \
            .eq("id", conv_id) \
            .execute()

    print(f"[Closer] Procesado OK — conv_id={conv_id}")


# ═══════════════════════════════════════════════════════════════════════
# FOLLOWUPS — CRON DIARIO 9AM
# ═══════════════════════════════════════════════════════════════════════

@closer_router.post("/followup/run")
async def ejecutar_followups(
    background_tasks: BackgroundTasks,
    x_agent_secret: Optional[str] = Header(None)
):
    """Cron diario 9am — n8n llama este endpoint. Dispara followups pendientes."""
    if x_agent_secret != AGENT_SECRET:
        raise HTTPException(status_code=401, detail="Unauthorized")

    background_tasks.add_task(ejecutar_followups_bg)
    return {"status": "iniciado", "mensaje": "Followups procesándose en background"}


async def ejecutar_followups_bg():
    """Procesa followups pendientes según secuencia 2/5/7/14 días"""
    print("[Closer] Iniciando cron de followups...")

    try:
        ahora = datetime.utcnow().isoformat()

        # Conversaciones con followup pendiente que no están cerradas ni son hot
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
            await asyncio.sleep(3)  # pausa entre mensajes

    except Exception as e:
        print(f"[Closer] Error en followups: {e}")


# Días entre cada followup: 2, 5, 7, 14
DIAS_FOLLOWUP = [2, 3, 2, 7]  # días desde el anterior (2→5→7→14)

async def procesar_followup_individual(conv: dict):
    """Genera y envía el followup contextual con Gemini"""
    conv_id     = conv.get("id")
    phone       = conv.get("telefono")
    nombre      = (conv.get("nombre_contacto") or "").split()[0] or "amigo"
    paso        = conv.get("followups_enviados", 0)
    estado      = conv.get("estado", "silent")
    proceso_cod = conv.get("proceso_codigo")

    if not phone:
        print(f"[Closer] Conv {conv_id} sin teléfono — saltando")
        return

    if paso >= 4:
        # Secuencia agotada — marcar como perdido
        supabase_admin.table("conversaciones_closer") \
            .update({"etapa": "inactivo", "estado": "perdido"}) \
            .eq("id", conv_id) \
            .execute()
        return

    # Actualizar estado a silent si no ha respondido
    supabase_admin.table("conversaciones_closer") \
        .update({"estado": "silent"}) \
        .eq("id", conv_id) \
        .execute()

    # Obtener historial para contexto
    historial = obtener_historial(conv_id, limite=6)

    # Generar mensaje contextual con Gemini
    mensaje = await generar_followup_gemini(historial, nombre, paso, estado, proceso_cod)

    # Enviar vía Evolution API
    await enviar_whatsapp(phone, mensaje)

    # Guardar en historial
    guardar_mensaje(conv_id, "agente", mensaje, generado_por_ia=True)

    # Calcular próximo followup
    dias = DIAS_FOLLOWUP[paso] if paso < len(DIAS_FOLLOWUP) else 7
    proximo = (datetime.utcnow() + timedelta(days=dias)).isoformat()

    supabase_admin.table("conversaciones_closer").update({
        "followups_enviados": paso + 1,
        "proximo_followup_en": proximo,
        "ultimo_mensaje_en": datetime.utcnow().isoformat()
    }).eq("id", conv_id).execute()

    print(f"[Closer] Followup {paso + 1}/4 enviado a {nombre} ({phone})")


# ═══════════════════════════════════════════════════════════════════════
# ALERTAS DE PROCESOS — CRON DIARIO 8AM
# ═══════════════════════════════════════════════════════════════════════

@closer_router.post("/alertas/run")
async def ejecutar_alertas(
    background_tasks: BackgroundTasks,
    x_agent_secret: Optional[str] = Header(None)
):
    """Cron diario 8am — busca procesos nuevos por cliente y notifica."""
    if x_agent_secret != AGENT_SECRET:
        raise HTTPException(status_code=401, detail="Unauthorized")

    background_tasks.add_task(ejecutar_alertas_bg)
    return {"status": "iniciado", "mensaje": "Alertas de procesos procesándose en background"}


async def ejecutar_alertas_bg():
    """Revisa alertas activas y notifica si hay procesos nuevos"""
    print("[Closer] Iniciando cron de alertas de procesos...")

    try:
        result = supabase_admin.table("alertas_cliente") \
            .select("*") \
            .eq("activa", True) \
            .execute()

        alertas = result.data or []
        print(f"[Closer] {len(alertas)} alertas activas")

        for alerta in alertas:
            await procesar_alerta_individual(alerta)
            await asyncio.sleep(2)

    except Exception as e:
        print(f"[Closer] Error en alertas: {e}")


async def procesar_alerta_individual(alerta: dict):
    """Busca procesos para una alerta y notifica si hay nuevos"""
    phone        = alerta.get("contact_phone")
    nombre       = alerta.get("contact_name") or "amigo"
    keywords     = alerta.get("keywords") or []
    instituciones = alerta.get("instituciones") or []
    ultimo_notif = alerta.get("ultimo_proceso_notificado")

    if not phone or not keywords:
        return

    # Buscar procesos activos que coincidan
    procesos = buscar_procesos_por_keywords(keywords, instituciones)
    if not procesos:
        print(f"[Closer] Sin procesos para {phone} con keywords {keywords}")
        return

    # Filtrar procesos ya notificados
    if ultimo_notif:
        procesos = [p for p in procesos if p.get("codigo_proceso") != ultimo_notif]

    if not procesos:
        print(f"[Closer] Solo procesos ya notificados para {phone}")
        return

    # Generar mensaje personalizado con Gemini
    nombre_corto = nombre.split()[0] if nombre else "amigo"
    mensaje = await generar_mensaje_alerta_proceso(nombre_corto, procesos, keywords)

    if not mensaje:
        return

    # Enviar vía Evolution API
    await enviar_whatsapp(phone, mensaje)

    # Actualizar último proceso notificado
    supabase_admin.table("alertas_cliente") \
        .update({"ultimo_proceso_notificado": procesos[0].get("codigo_proceso")}) \
        .eq("id", alerta["id"]) \
        .execute()

    print(f"[Closer] Alerta enviada a {phone} — {len(procesos)} proceso(s)")


# ═══════════════════════════════════════════════════════════════════════
# ENDPOINTS ADMIN
# ═══════════════════════════════════════════════════════════════════════

@closer_router.get("/conversaciones")
async def listar_conversaciones(
    etapa: Optional[str] = None,
    estado: Optional[str] = None,
    limite: int = 20,
    x_agent_secret: Optional[str] = Header(None)
):
    """Lista conversaciones activas — para el panel de Lonny"""
    if x_agent_secret != AGENT_SECRET:
        raise HTTPException(status_code=401, detail="Unauthorized")

    query = supabase_admin.table("conversaciones_closer") \
        .select("*") \
        .not_.in_("etapa", ["cerrado_perdido", "inactivo"]) \
        .order("ultimo_mensaje_en", desc=True) \
        .limit(limite)

    if etapa:
        query = query.eq("etapa", etapa)
    if estado:
        query = query.eq("estado", estado)

    result = query.execute()
    return {
        "total": len(result.data or []),
        "conversaciones": result.data or []
    }


@closer_router.post("/marcar/{conversacion_id}")
async def marcar_conversacion(
    conversacion_id: str,
    payload: MarcarEtapaPayload,
    x_agent_secret: Optional[str] = Header(None)
):
    """Lonny marca manualmente una etapa desde Telegram"""
    if x_agent_secret != AGENT_SECRET:
        raise HTTPException(status_code=401, detail="Unauthorized")

    etapas_validas = ["nuevo", "respondido", "interesado", "demo_dada",
                      "propuesta_enviada", "cerrado_ganado", "cerrado_perdido", "inactivo"]
    if payload.etapa not in etapas_validas:
        raise HTTPException(status_code=400, detail=f"Etapa inválida. Opciones: {etapas_validas}")

    update_data = {"etapa": payload.etapa}
    if payload.notas:
        update_data["notas"] = payload.notas

    # Si Lonny cierra la venta, también actualizar estado
    if payload.etapa == "cerrado_ganado":
        update_data["estado"] = "cerrado"
    elif payload.etapa == "cerrado_perdido":
        update_data["estado"] = "perdido"

    supabase_admin.table("conversaciones_closer") \
        .update(update_data) \
        .eq("id", conversacion_id) \
        .execute()

    return {"success": True, "conversacion_id": conversacion_id, "etapa": payload.etapa}


class RegistrarInteresPayload(BaseModel):
    keywords: List[str] = []
    instituciones: List[str] = []
    notas: Optional[str] = None


@closer_router.post("/cliente/{phone}/interes")
async def registrar_interes_manual(
    phone: str,
    payload: RegistrarInteresPayload,
    x_agent_secret: Optional[str] = Header(None)
):
    """
    Lonny registra manualmente el interés de un cliente desde Telegram o panel.
    Ejemplo: POST /closer/cliente/18091234567/interes
    Body: {"keywords": ["alcantarillado", "drenaje"], "instituciones": ["CAASD"]}
    """
    if x_agent_secret != AGENT_SECRET:
        raise HTTPException(status_code=401, detail="Unauthorized")

    # Buscar conversación activa del cliente
    conv = obtener_o_crear_conversacion(phone)
    conv_id = conv.get("id")
    nombre = conv.get("nombre_contacto", "Cliente")

    # Registrar alerta
    registrar_alerta_cliente(conv_id, phone, nombre, payload.keywords, payload.instituciones)

    # Notificar al cliente por WhatsApp que estará pendiente
    if payload.keywords or payload.instituciones:
        kw_texto = ", ".join(payload.keywords + payload.instituciones)
        mensaje = f"Hola, el Ing. Luis Antigua me indicó que te interesa estar al tanto de procesos de {kw_texto}. Te avisaré cuando aparezca algo que aplique para ti 👍"
        await enviar_whatsapp(phone, mensaje)

    return {
        "success": True,
        "phone": phone,
        "conv_id": conv_id,
        "keywords": payload.keywords,
        "instituciones": payload.instituciones,
        "mensaje": f"Interés registrado para {nombre}"
    }
async def test_alerta(x_agent_secret: Optional[str] = Header(None)):
    """Prueba que el módulo de alertas funciona correctamente"""
    if x_agent_secret != AGENT_SECRET:
        raise HTTPException(status_code=401, detail="Unauthorized")

    await enviar_telegram("✅ <b>Agente Closer</b> — test de conectividad OK\n\nGemini: " +
                          ("✅ configurado" if gemini_client else "❌ sin API key") +
                          "\nEvolution API: " +
                          ("✅ configurado" if EVOLUTION_API_URL else "❌ sin URL"))

    return {
        "gemini": bool(gemini_client),
        "evolution_api": bool(EVOLUTION_API_URL),
        "telegram": bool(TELEGRAM_BOT_TOKEN),
        "status": "ok"
    }
