"""
router_closer.py
Agente Closer — Vendedor IA para LicitacionLab
Canales: WhatsApp + Instagram DM vía ManyChat

Endpoints:
  POST /closer/webhook          — recibe mensajes desde ManyChat (WS + IG)
  POST /closer/followup/run     — cron diario que dispara followups pendientes
  GET  /closer/conversaciones   — lista conversaciones activas (panel Telegram)
  POST /closer/marcar/{id}      — marca conversación como etapa específica

Flujo:
  ManyChat recibe msg → webhook → Claude busca en Supabase → responde → guarda historial
  Si detecta señal de cierre → alerta Telegram a Lonny
"""

import os
import json
import asyncio
import httpx
from datetime import datetime, timedelta
from typing import Optional

from fastapi import APIRouter, Header, HTTPException, BackgroundTasks
from pydantic import BaseModel, Field
import anthropic
from supabase import create_client

# ── Config ─────────────────────────────────────────────────────────────
SUPABASE_URL        = os.environ["SUPABASE_URL"]
SUPABASE_KEY        = os.environ["SUPABASE_KEY"]
CLAUDE_KEY          = os.environ["ANTHROPIC_API_KEY"]
AGENT_SECRET        = os.environ.get("AGENT_SECRET", "licitacionlab-growth-2026")
TELEGRAM_BOT_TOKEN  = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID    = os.environ.get("TELEGRAM_CHAT_ID", "817596333")
MANYCHAT_API_KEY    = os.environ.get("MANYCHAT_API_KEY", "")  # se agrega después

supabase     = create_client(SUPABASE_URL, SUPABASE_KEY)
supabase_admin = create_client(SUPABASE_URL, os.environ.get("SUPABASE_SERVICE_KEY", SUPABASE_KEY))
claude       = anthropic.Anthropic(api_key=CLAUDE_KEY)

closer_router = APIRouter(prefix="/closer", tags=["Agente Closer"])

# ── Señales de cierre que disparan alerta a Lonny ──────────────────────
SENALES_CIERRE = [
    "cuánto cuesta", "cuanto cuesta", "precio", "plan", "suscripción",
    "cómo me registro", "como me registro", "quiero registrarme",
    "cómo funciona", "como funciona", "quiero probarlo",
    "tienen demo", "puedo ver", "me interesa", "cuándo empiezo",
    "cuando empiezo", "cómo pago", "como pago", "tiene versión gratis",
    "tiene version gratis", "qué incluye", "que incluye",
]

# ── Contexto del sistema para Claude ───────────────────────────────────
SYSTEM_PROMPT = """Eres el asistente de ventas de LicitacionLab, una plataforma SaaS dominicana que monitorea licitaciones del DGCP (Dirección General de Contrataciones Públicas) y analiza pliegos con inteligencia artificial.

Tu nombre es "Lab" y representas a Lonny Antigua, fundador de LicitacionLab.

TU OBJETIVO: Ayudar a empresas constructoras y de ingeniería de República Dominicana a ganar más licitaciones públicas. Responder sus preguntas, generar confianza, y guiarlos a registrarse en https://app.licitacionlab.com/

PLANES Y PRECIOS:
- Explorador: RD$1,490/mes — monitoreo básico, alertas por email
- Competidor: RD$3,990/mes — análisis de pliego con IA, alertas Telegram
- Ganador: RD$8,500/mes — todo incluido + soporte prioritario

CAPACIDADES DE LA PLATAFORMA:
- Monitoreo automático de todas las licitaciones del DGCP en tiempo real
- Análisis de pliegos de condiciones con IA (detecta requisitos, alertas de fraude, checklist)
- Alertas instantáneas por email y Telegram cuando aparece una licitación de tu sector
- Panel de seguimiento de procesos
- Análisis "¿Vale la pena?" con score de oportunidad

REGLAS DE COMUNICACIÓN:
1. Responde en español dominicano, natural y cercano — NO robótico ni corporativo
2. Mensajes cortos: máximo 3-4 oraciones por respuesta
3. Si alguien pregunta por un proceso específico (ej: MOPC-LPN-2026-0010), di que vas a buscar la información y en tu respuesta incluye los datos clave
4. Nunca inventes información sobre procesos — solo usa lo que tienes en el contexto
5. Si no tienes información de un proceso, ofrece analizarlo en la plataforma
6. Cierra siempre con una pregunta o CTA suave — no presiones
7. Cuando alguien muestre interés real, invítalos a registrarse gratis: https://app.licitacionlab.com/

TONO: Experto en licitaciones dominicanas, accesible, confiable. Como un asesor que conoce el DGCP por dentro."""


# ═══════════════════════════════════════════════════════════════════════
# MODELOS
# ═══════════════════════════════════════════════════════════════════════

class ManyCharWebhookPayload(BaseModel):
    """
    Payload que llega desde ManyChat vía webhook.
    Acepta tanto nombres personalizados como los nativos de ManyChat.
    """
    # Nombres personalizados (si se renombran en ManyChat)
    contact_id: Optional[str] = None
    message: Optional[str] = None
    name: Optional[str] = None

    # Nombres nativos de ManyChat
    id_de_contacto: Optional[str] = Field(None, alias="Id de contacto")
    nombre_completo: Optional[str] = Field(None, alias="Nombre completo")
    ultima_entrada: Optional[str] = Field(None, alias="Última entrada de texto")

    # Canal y otros
    channel: str = "instagram"
    phone: Optional[str] = None
    timestamp: Optional[str] = None

    class Config:
        populate_by_name = True

    def get_contact_id(self) -> str:
        return self.contact_id or self.id_de_contacto or "unknown"

    def get_message(self) -> str:
        return self.message or self.ultima_entrada or ""

    def get_name(self) -> str:
        return self.name or self.nombre_completo or "Desconocido"

class MarcarEtapaPayload(BaseModel):
    etapa: str
    notas: Optional[str] = None


# ═══════════════════════════════════════════════════════════════════════
# HELPERS
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


async def responder_manychat(contact_id: str, canal: str, mensaje: str):
    """
    Envía respuesta al contacto vía ManyChat API.
    Canal: whatsapp | instagram
    """
    if not MANYCHAT_API_KEY:
        print(f"[ManyChat] Sin API key — mensaje que se enviaría: {mensaje[:80]}")
        return

    # ManyChat usa el mismo endpoint para WS e IG — diferencia por subscriber_id
    url = "https://api.manychat.com/fb/sending/sendContent"
    headers = {
        "Authorization": f"Bearer {MANYCHAT_API_KEY}",
        "Content-Type": "application/json"
    }
    payload = {
        "subscriber_id": contact_id,
        "data": {
            "version": "v2",
            "content": {
                "messages": [{"type": "text", "text": mensaje}]
            }
        },
        "message_tag": "ACCOUNT_UPDATE"
    }

    async with httpx.AsyncClient(timeout=15) as client:
        try:
            resp = await client.post(url, headers=headers, json=payload)
            if resp.status_code != 200:
                print(f"[ManyChat] Error {resp.status_code}: {resp.text[:200]}")
        except Exception as e:
            print(f"[ManyChat] Error enviando: {e}")


def buscar_analisis_pliego(proceso_codigo: str) -> Optional[dict]:
    """Busca análisis de pliego en Supabase si existe"""
    try:
        result = supabase.table("analisis_pliego") \
            .select("resumen_ejecutivo, alertas_fraude, checklist_categorizado, plazos_clave") \
            .eq("proceso_id", proceso_codigo) \
            .limit(1) \
            .execute()
        if result.data:
            return result.data[0]
    except Exception as e:
        print(f"[Closer] Error buscando pliego {proceso_codigo}: {e}")
    return None


def buscar_proceso_dgcp(codigo_o_texto: str) -> Optional[dict]:
    """Busca un proceso en la tabla procesos por código o título"""
    try:
        # Primero buscar por código exacto
        result = supabase.table("procesos") \
            .select("codigo_proceso, titulo, unidad_compra, monto_estimado, fecha_fin_recepcion_ofertas, estado_proceso") \
            .ilike("codigo_proceso", f"%{codigo_o_texto}%") \
            .limit(1) \
            .execute()
        if result.data:
            return result.data[0]

        # Si no, buscar por título
        result2 = supabase.table("procesos") \
            .select("codigo_proceso, titulo, unidad_compra, monto_estimado, fecha_fin_recepcion_ofertas, estado_proceso") \
            .ilike("titulo", f"%{codigo_o_texto}%") \
            .limit(1) \
            .execute()
        if result2.data:
            return result2.data[0]
    except Exception as e:
        print(f"[Closer] Error buscando proceso: {e}")
    return None


def obtener_o_crear_conversacion(contact_id: str, canal: str, nombre: str = None, telefono: str = None) -> dict:
    """Obtiene conversación existente o crea una nueva"""
    try:
        result = supabase_admin.table("conversaciones_closer") \
            .select("*") \
            .eq("canal", canal) \
            .eq("contacto_id", contact_id) \
            .neq("etapa", "cerrado_ganado") \
            .neq("etapa", "cerrado_perdido") \
            .order("creado_en", desc=True) \
            .limit(1) \
            .execute()

        if result.data:
            conv = result.data[0]
            # Actualizar último mensaje
            supabase_admin.table("conversaciones_closer") \
                .update({"ultimo_mensaje_en": datetime.utcnow().isoformat()}) \
                .eq("id", conv["id"]) \
                .execute()
            return conv

        # Crear nueva conversación
        nueva = {
            "canal": canal,
            "contacto_id": contact_id,
            "nombre_contacto": nombre or "Desconocido",
            "telefono": telefono,
            "etapa": "nuevo",
            "ultimo_mensaje_en": datetime.utcnow().isoformat(),
            "proximo_followup_en": (datetime.utcnow() + timedelta(days=1)).isoformat(),
            "followups_enviados": 0,
        }
        res = supabase_admin.table("conversaciones_closer").insert(nueva).execute()
        return res.data[0] if res.data else nueva

    except Exception as e:
        print(f"[Closer] Error conversación: {e}")
        return {"id": None, "etapa": "nuevo", "followups_enviados": 0}


def obtener_historial(conversacion_id: str, limite: int = 10) -> list:
    """Obtiene últimos N mensajes de la conversación"""
    if not conversacion_id:
        return []
    try:
        result = supabase_admin.table("mensajes_closer") \
            .select("rol, contenido, enviado_en") \
            .eq("conversacion_id", conversacion_id) \
            .order("enviado_en", desc=True) \
            .limit(limite) \
            .execute()
        # Invertir para orden cronológico
        return list(reversed(result.data or []))
    except Exception as e:
        print(f"[Closer] Error historial: {e}")
        return []


def guardar_mensaje(conversacion_id: str, rol: str, contenido: str, canal: str, generado_por_ia: bool = False):
    """Guarda mensaje en el historial"""
    if not conversacion_id:
        return
    try:
        supabase_admin.table("mensajes_closer").insert({
            "conversacion_id": conversacion_id,
            "rol": rol,
            "contenido": contenido,
            "canal": canal,
            "generado_por_ia": generado_por_ia,
            "enviado_en": datetime.utcnow().isoformat()
        }).execute()
    except Exception as e:
        print(f"[Closer] Error guardando mensaje: {e}")


def detectar_proceso_en_mensaje(mensaje: str) -> Optional[str]:
    """Detecta si el mensaje menciona un código de proceso DGCP"""
    import re
    # Patrones: MOPC-LPN-2026-0001, CAASD-CCC-CP-2026-0010, etc.
    patron = r'[A-Z]{2,10}-[A-Z]{2,5}-[A-Z]{2,5}-\d{4}-\d{4}'
    match = re.search(patron, mensaje.upper())
    if match:
        return match.group()
    return None


def detectar_senal_cierre(mensaje: str) -> bool:
    """Detecta si el mensaje contiene señal de intención de compra"""
    msg_lower = mensaje.lower()
    return any(senal in msg_lower for senal in SENALES_CIERRE)


async def generar_respuesta_claude(
    mensaje_cliente: str,
    historial: list,
    contexto_adicional: str = ""
) -> str:
    """Genera respuesta usando Claude con historial de conversación"""

    # Construir mensajes del historial
    messages = []
    for msg in historial[-8:]:  # Últimos 8 mensajes para contexto
        rol_claude = "user" if msg["rol"] == "cliente" else "assistant"
        messages.append({"role": rol_claude, "content": msg["contenido"]})

    # Añadir contexto adicional si existe (datos de proceso, etc.)
    prompt_final = mensaje_cliente
    if contexto_adicional:
        prompt_final = f"{contexto_adicional}\n\nMensaje del cliente: {mensaje_cliente}"

    messages.append({"role": "user", "content": prompt_final})

    try:
        response = claude.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=300,
            system=SYSTEM_PROMPT,
            messages=messages
        )
        return response.content[0].text.strip()
    except Exception as e:
        print(f"[Claude] Error generando respuesta: {e}")
        return "Disculpa, tuve un problema técnico. Escríbeme en un momento y te ayudo. 🙏"


# ═══════════════════════════════════════════════════════════════════════
# ENDPOINTS
# ═══════════════════════════════════════════════════════════════════════

@closer_router.post("/webhook")
async def recibir_mensaje_manychat(
    payload: ManyCharWebhookPayload,
    background_tasks: BackgroundTasks,
    x_agent_secret: Optional[str] = Header(None)
):
    """
    Webhook principal — ManyChat llama este endpoint cuando llega un mensaje.
    Responde rápido (< 2s) y procesa en background.
    """
    if x_agent_secret != AGENT_SECRET:
        raise HTTPException(status_code=401, detail="Unauthorized")

    background_tasks.add_task(
        procesar_mensaje_bg,
        payload.get_contact_id(),
        payload.channel,
        payload.get_message(),
        payload.get_name(),
        payload.phone
    )

    return {"status": "recibido", "processing": True}


async def procesar_mensaje_bg(
    contact_id: str,
    canal: str,
    mensaje: str,
    nombre: str = None,
    telefono: str = None
):
    """Procesa el mensaje en background: busca contexto, genera respuesta, responde"""
    print(f"[Closer] Procesando — canal={canal}, contacto={contact_id}, msg={mensaje[:50]}")

    # 1. Obtener/crear conversación
    conv = obtener_o_crear_conversacion(contact_id, canal, nombre, telefono)
    conv_id = conv.get("id")

    # 2. Guardar mensaje del cliente
    guardar_mensaje(conv_id, "cliente", mensaje, canal)

    # 3. Detectar si menciona un proceso DGCP
    contexto_adicional = ""
    codigo_proceso = detectar_proceso_en_mensaje(mensaje)
    if codigo_proceso:
        # Primero buscar si ya está analizado
        analisis = buscar_analisis_pliego(codigo_proceso)
        if analisis:
            resumen = analisis.get("resumen_ejecutivo", "")[:500]
            contexto_adicional = f"""
CONTEXTO — El cliente pregunta por el proceso {codigo_proceso}.
Tenemos este análisis en la plataforma:
{resumen}
Usa esta información para responder de forma útil y menciona que en LicitacionLab pueden ver el análisis completo.
"""
        else:
            # Buscar datos básicos del proceso
            proceso = buscar_proceso_dgcp(codigo_proceso)
            if proceso:
                contexto_adicional = f"""
CONTEXTO — Datos del proceso {codigo_proceso}:
- Entidad: {proceso.get('unidad_compra', '—')}
- Título: {proceso.get('titulo', '—')}
- Monto: RD$ {float(proceso.get('monto_estimado', 0)):,.0f}
- Estado: {proceso.get('estado_proceso', '—')}
- Fecha límite: {proceso.get('fecha_fin_recepcion_ofertas', '—')}
Menciona que pueden ver el análisis completo del pliego en LicitacionLab.
"""
            else:
                contexto_adicional = f"""
CONTEXTO — El cliente pregunta por {codigo_proceso}.
No tenemos este proceso en nuestra base de datos. Ofrece analizarlo en la plataforma.
"""
        # Actualizar proceso en la conversación
        if conv_id:
            supabase_admin.table("conversaciones_closer") \
                .update({"proceso_codigo": codigo_proceso}) \
                .eq("id", conv_id) \
                .execute()

    # 4. Obtener historial de conversación
    historial = obtener_historial(conv_id)

    # 5. Generar respuesta con Claude
    respuesta = await generar_respuesta_claude(mensaje, historial, contexto_adicional)

    # 6. Guardar respuesta del agente
    guardar_mensaje(conv_id, "agente", respuesta, canal, generado_por_ia=True)

    # 7. Responder al cliente vía ManyChat
    await responder_manychat(contact_id, canal, respuesta)

    # 8. Detectar señal de cierre → alerta Telegram
    if detectar_senal_cierre(mensaje):
        nombre_display = nombre or contact_id
        alerta = f"""🔥 <b>SEÑAL DE CIERRE DETECTADA</b>

👤 <b>Contacto:</b> {nombre_display}
📱 <b>Canal:</b> {canal.upper()}
💬 <b>Mensaje:</b> {mensaje[:200]}

🤖 <b>Respuesta del agente:</b>
{respuesta[:200]}

<i>Considera entrar tú a cerrar la venta.</i>"""
        await enviar_telegram(alerta)

        # Actualizar etapa a "interesado"
        if conv_id:
            supabase_admin.table("conversaciones_closer") \
                .update({"etapa": "interesado"}) \
                .eq("id", conv_id) \
                .execute()

    # 9. Programar próximo followup (día +2 si es nuevo)
    if conv.get("etapa") == "nuevo" and conv_id:
        supabase_admin.table("conversaciones_closer") \
            .update({
                "etapa": "respondido",
                "proximo_followup_en": (datetime.utcnow() + timedelta(days=2)).isoformat()
            }) \
            .eq("id", conv_id) \
            .execute()

    print(f"[Closer] Procesado OK — conv_id={conv_id}")


@closer_router.post("/followup/run")
async def ejecutar_followups(
    background_tasks: BackgroundTasks,
    x_agent_secret: Optional[str] = Header(None)
):
    """
    Cron diario (n8n lo llama cada mañana a las 9am).
    Detecta conversaciones que necesitan followup y envía mensajes.
    """
    if x_agent_secret != AGENT_SECRET:
        raise HTTPException(status_code=401, detail="Unauthorized")

    background_tasks.add_task(ejecutar_followups_bg)
    return {"status": "iniciado", "mensaje": "Followups procesándose en background"}


async def ejecutar_followups_bg():
    """Procesa followups pendientes"""
    print("[Closer] Iniciando followups...")

    try:
        ahora = datetime.utcnow().isoformat()
        result = supabase_admin.table("conversaciones_closer") \
            .select("*") \
            .lte("proximo_followup_en", ahora) \
            .not_.in_("etapa", ["cerrado_ganado", "cerrado_perdido", "inactivo"]) \
            .lt("followups_enviados", 4) \
            .execute()

        conversaciones = result.data or []
        print(f"[Closer] {len(conversaciones)} followups pendientes")

        for conv in conversaciones:
            await procesar_followup(conv)
            await asyncio.sleep(2)  # Pausa entre followups

    except Exception as e:
        print(f"[Closer] Error en followups: {e}")


FOLLOWUP_MENSAJES = [
    # Followup 1 — día 2
    "Hola {nombre}, soy Lab de LicitacionLab 👋 ¿Pudiste revisar la información sobre las licitaciones activas del DGCP? Te puedo ayudar a encontrar oportunidades para tu empresa.",

    # Followup 2 — día 4
    "Hola {nombre}, esta semana hay {total_procesos} procesos nuevos en el DGCP. ¿Tu empresa participa en licitaciones de infraestructura o construcción? Te puedo mostrar las que aplican.",

    # Followup 3 — día 7
    "Hola {nombre}, muchas empresas constructoras en RD están perdiendo licitaciones por no enterarse a tiempo. LicitacionLab las detecta automáticamente y analiza los pliegos. ¿Te interesa ver cómo funciona? Es gratis registrarse 👉 app.licitacionlab.com",

    # Followup 4 — día 14
    "Hola {nombre}, último mensaje de mi parte. Si en algún momento necesitas apoyo con licitaciones del DGCP, aquí estaré. El registro en LicitacionLab es gratis: app.licitacionlab.com — Éxitos 🤝",
]


async def procesar_followup(conv: dict):
    """Envía el followup correspondiente a una conversación"""
    conv_id = conv.get("id")
    followups_enviados = conv.get("followups_enviados", 0)

    if followups_enviados >= len(FOLLOWUP_MENSAJES):
        # Marcar como inactivo
        supabase_admin.table("conversaciones_closer") \
            .update({"etapa": "inactivo"}) \
            .eq("id", conv_id) \
            .execute()
        return

    # Obtener total de procesos activos para personalizar
    try:
        res = supabase.table("procesos") \
            .select("id", count="exact") \
            .eq("estado_proceso", "Proceso publicado") \
            .execute()
        total_procesos = res.count or 0
    except:
        total_procesos = 25

    nombre = conv.get("nombre_contacto") or "amigo"
    mensaje = FOLLOWUP_MENSAJES[followups_enviados].format(
        nombre=nombre.split()[0],  # Solo primer nombre
        total_procesos=total_procesos
    )

    # Enviar vía ManyChat
    await responder_manychat(conv["contacto_id"], conv["canal"], mensaje)

    # Guardar en historial
    guardar_mensaje(conv_id, "agente", mensaje, conv["canal"], generado_por_ia=True)

    # Calcular próximo followup
    dias_siguientes = [2, 2, 3, 7]  # días entre followups
    dias = dias_siguientes[followups_enviados] if followups_enviados < len(dias_siguientes) else 7
    proximo = (datetime.utcnow() + timedelta(days=dias)).isoformat()

    supabase_admin.table("conversaciones_closer").update({
        "followups_enviados": followups_enviados + 1,
        "proximo_followup_en": proximo,
        "ultimo_mensaje_en": datetime.utcnow().isoformat()
    }).eq("id", conv_id).execute()

    print(f"[Closer] Followup {followups_enviados + 1} enviado a {nombre} ({conv['canal']})")


@closer_router.get("/conversaciones")
async def listar_conversaciones(
    etapa: Optional[str] = None,
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
    """Lonny marca manualmente una etapa (ej: cerrado_ganado)"""
    if x_agent_secret != AGENT_SECRET:
        raise HTTPException(status_code=401, detail="Unauthorized")

    etapas_validas = ["nuevo", "respondido", "interesado", "demo_dada",
                      "propuesta_enviada", "cerrado_ganado", "cerrado_perdido", "inactivo"]
    if payload.etapa not in etapas_validas:
        raise HTTPException(status_code=400, detail=f"Etapa inválida. Opciones: {etapas_validas}")

    update_data = {"etapa": payload.etapa}
    if payload.notas:
        update_data["notas"] = payload.notas

    supabase_admin.table("conversaciones_closer") \
        .update(update_data) \
        .eq("id", conversacion_id) \
        .execute()

    return {"success": True, "conversacion_id": conversacion_id, "etapa": payload.etapa}
