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
    "cuanto cuesta", "cuánto cuesta", "precio", "plan", "suscripcion", "suscripción",
    "como me registro", "cómo me registro", "quiero registrarme", "quiero contratar",
    "como funciona", "cómo funciona", "quiero probarlo", "quiero el servicio",
    "tienen demo", "puedo ver", "me interesa contratar", "cuando empiezo",
    "cuándo empiezo", "como pago", "cómo pago", "tiene version gratis",
    "tiene versión gratis", "que incluye", "qué incluye", "quiero una cotizacion",
    "quiero una cotización", "mandame propuesta", "mándame propuesta",
    "cuanto cobran", "cuánto cobran", "necesito el servicio",
]

KEYWORDS_ALERTA = [
    "avisame", "avísame", "notificame", "notifícame", "me interesa saber",
    "cuando haya", "cuando salga", "si aparece", "si sale algo de",
    "estoy buscando procesos", "quiero saber de licitaciones de",
    "me interesa participar en", "busco licitaciones de",
]

SYSTEM_PROMPT = """Eres el asistente de ventas del Ing. Luis Antigua — ingeniero civil dominicano con más de 15 años de experiencia en licitaciones públicas del DGCP de República Dominicana.

El Ing. Luis Antigua es fundador de LicitacionLab, una plataforma SaaS que monitorea licitaciones del DGCP y analiza pliegos con IA. También ofrece consultoría para preparación de ofertas (MOPC, CAASD, INAPA, MIDEREC, MINERD y más).

Tu nombre es "Lab". Cuando te presentes di: "Soy Lab, asistente del Ing. Luis Antigua."

SERVICIOS:
1. LicitacionLab (SaaS):
- Explorador: RD$1,490/mes
- Competidor: RD$3,990/mes
- Ganador: RD$8,500/mes
- Registro: https://app.licitacionlab.com/

2. Consultoría: preparación completa de ofertas técnicas y económicas.
Para precios de consultoría, el Ing. Luis Antigua atiende directamente.

REGLAS:
1. Español dominicano, natural — nunca robótico
2. Mensajes cortos: máximo 3-4 oraciones
3. Nunca inventes datos de procesos
4. Cierra siempre con pregunta o CTA suave
5. Máximo 1-2 emojis por mensaje
6. Si el CONTEXTO tiene el perfil del cliente, úsalo — no repitas preguntas

PERFILAMIENTO NATURAL: Extrae sin formulario —
- A qué se dedica su empresa
- Qué tipo de licitaciones le interesan
- Qué instituciones le interesan
- Si tienen RPE/RNCE y estados financieros

TONO: Experto dominicano en contrataciones públicas, accesible, confiable."""


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


async def enviar_whatsapp(phone: str, mensaje: str):
    if not ZAPI_INSTANCE_ID or not ZAPI_TOKEN:
        print(f"[Z-API] Sin config — msg para {phone}: {mensaje[:80]}")
        return

    phone_clean = phone.replace("+", "").replace("-", "").replace(" ", "")
    url     = f"https://api.z-api.io/instances/{ZAPI_INSTANCE_ID}/token/{ZAPI_TOKEN}/send-text"
    headers = {"Client-Token": ZAPI_CLIENT_TOKEN} if ZAPI_CLIENT_TOKEN else {}

    async with httpx.AsyncClient(timeout=15) as client:
        try:
            resp = await client.post(url, headers=headers, json={
                "phone":   phone_clean,
                "message": mensaje
            })
            if resp.status_code not in (200, 201):
                print(f"[Z-API] Error {resp.status_code}: {resp.text[:200]}")
            else:
                print(f"[Z-API] Enviado a {phone_clean}")
        except Exception as e:
            print(f"[Z-API] Error: {e}")


# ═══════════════════════════════════════════════════════════════════════
# HELPERS — SUPABASE
# ═══════════════════════════════════════════════════════════════════════

def buscar_analisis_pliego(codigo_o_texto: str) -> Optional[dict]:
    try:
        result = supabase_admin.table("analisis_pliego") \
            .select("resumen_ejecutivo, alertas_fraude, checklist_categorizado, plazos_clave, proceso_id") \
            .eq("proceso_id", codigo_o_texto) \
            .limit(1).execute()
        if result.data:
            return result.data[0]

        result2 = supabase_admin.table("analisis_pliego") \
            .select("resumen_ejecutivo, alertas_fraude, checklist_categorizado, plazos_clave, proceso_id") \
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
    try:
        query = supabase_admin.table("procesos") \
            .select("codigo_proceso, titulo, unidad_compra, monto_estimado, fecha_fin_recepcion_ofertas") \
            .eq("estado_proceso", "Proceso publicado")

        if monto_min:
            query = query.gte("monto_estimado", monto_min)
        if instituciones:
            for inst in instituciones[:2]:
                query = query.ilike("unidad_compra", f"%{inst}%")

        result   = query.order("fecha_fin_recepcion_ofertas", desc=False).limit(5).execute()
        procesos = result.data or []

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
    contexto_adicional: str = ""
) -> str:
    if not gemini_client:
        return "Disculpa, tuve un problema tecnico. Escribeme en un momento."

    historial_texto = ""
    for msg in historial[-8:]:
        rol = "Cliente" if msg["rol"] == "cliente" else "Lab (tu)"
        historial_texto += f"{rol}: {msg['contenido']}\n"

    prompt = f"""{SYSTEM_PROMPT}

--- HISTORIAL ---
{historial_texto if historial_texto else "(conversacion nueva)"}

--- CONTEXTO ADICIONAL ---
{contexto_adicional if contexto_adicional else "(ninguno)"}

--- MENSAJE DEL CLIENTE ---
{mensaje_cliente}

--- TU RESPUESTA (solo el texto) ---"""

    try:
        response = gemini_client.models.generate_content(
            model="gemini-2.5-flash",
            contents=prompt,
            config=types.GenerateContentConfig(max_output_tokens=300, temperature=0.75)
        )
        return response.text.strip()
    except Exception as e:
        print(f"[Gemini] Error: {e}")
        return "Disculpa, tuve un problema tecnico. Escribeme en un momento."


async def generar_followup_gemini(
    historial: list, nombre: str, paso: int,
    estado: str, proceso_codigo: str = None
) -> str:
    if not gemini_client:
        return f"Hola {nombre}, pudiste revisar la informacion que te compartí?"

    contextos_paso = {
        0: "Primer followup (dia 2). Muestra valor con info util sobre procesos activos.",
        1: "Segundo followup (dia 5). Crea urgencia suave con fechas proximas.",
        2: "Tercer followup (dia 7). Prueba social: empresas dominicanas que ganan licitaciones.",
        3: "Cuarto followup (dia 14). Despedida amigable, deja la puerta abierta.",
    }

    historial_texto = ""
    for msg in historial[-6:]:
        rol = "Cliente" if msg["rol"] == "cliente" else "Lab"
        historial_texto += f"{rol}: {msg['contenido']}\n"

    proceso_info = ""
    if proceso_codigo:
        proceso = buscar_proceso_dgcp(proceso_codigo)
        if proceso:
            proceso_info = f"El cliente pregunto por {proceso_codigo} — {proceso.get('titulo', '')} de {proceso.get('unidad_compra', '')}."

    prompt = f"""{SYSTEM_PROMPT}

Envias un mensaje de seguimiento a {nombre}.
Contexto: {contextos_paso.get(paso, contextos_paso[3])}
Estado del cliente: {estado}
{proceso_info}

Historial:
{historial_texto if historial_texto else "(sin historial)"}

Instrucciones:
- Un solo mensaje de WhatsApp, natural, español dominicano
- Maximo 3 oraciones
- No menciones que es un seguimiento
- Cierra con pregunta o CTA suave
- Solo el texto del mensaje, sin comillas"""

    try:
        response = gemini_client.models.generate_content(
            model="gemini-2.5-flash",
            contents=prompt,
            config=types.GenerateContentConfig(max_output_tokens=200, temperature=0.8)
        )
        return response.text.strip()
    except Exception as e:
        print(f"[Gemini] Error followup: {e}")
        return f"Hola {nombre}, como vas con los temas de licitaciones?"


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
    if not texto:
        return {"status": "ok", "skipped": "no_text"}

    nombre = body.get("senderName", "") or body.get("pushName", "") or ""
    background_tasks.add_task(procesar_mensaje_bg, phone, texto, nombre)
    return {"status": "recibido", "processing": True}


async def procesar_mensaje_bg(phone: str, mensaje: str, nombre: str = ""):
    print(f"[Closer] Procesando phone={phone} msg={mensaje[:60]}")

    conv    = obtener_o_crear_conversacion(phone, nombre)
    conv_id = conv.get("id")

    guardar_mensaje(conv_id, "cliente", mensaje)

    contexto_adicional = ""
    codigo_proceso     = detectar_proceso_en_mensaje(mensaje)

    if codigo_proceso:
        analisis = buscar_analisis_pliego(codigo_proceso)
        if analisis:
            resumen            = str(analisis.get("resumen_ejecutivo", ""))[:600]
            contexto_adicional = (
                f"ANALISIS DISPONIBLE del proceso {codigo_proceso}:\n{resumen}\n"
                "Usa esta info y menciona que en LicitacionLab pueden ver el analisis completo."
            )
        else:
            proceso = buscar_proceso_dgcp(codigo_proceso)
            if proceso:
                monto     = proceso.get("monto_estimado", 0)
                monto_fmt = f"RD${float(monto):,.0f}" if monto else "no publicado"
                contexto_adicional = (
                    f"DATOS del proceso {codigo_proceso}:\n"
                    f"- Entidad: {proceso.get('unidad_compra', '---')}\n"
                    f"- Titulo: {proceso.get('titulo', '---')}\n"
                    f"- Monto: {monto_fmt}\n"
                    f"- Estado: {proceso.get('estado_proceso', '---')}\n"
                    f"- Cierre ofertas: {proceso.get('fecha_fin_recepcion_ofertas', '---')}\n"
                    "Menciona que pueden ver el analisis completo en LicitacionLab."
                )
            else:
                contexto_adicional = (
                    f"El cliente pregunta por {codigo_proceso}. No esta analizado aun, "
                    "pero ya iniciamos el analisis automaticamente. "
                    "Dile que en unos minutos tendra el resumen."
                )
                asyncio.create_task(disparar_analisis_pliego_bg(codigo_proceso, phone, conv_id))

        if conv_id:
            supabase_admin.table("conversaciones_closer") \
                .update({"proceso_codigo": codigo_proceso}) \
                .eq("id", conv_id).execute()

    if detectar_interes_alerta(mensaje):
        keywords, instituciones = extraer_keywords_interes(mensaje)
        if keywords or instituciones:
            registrar_alerta_cliente(conv_id, phone, nombre, keywords, instituciones)
            kw_texto           = ", ".join(keywords + instituciones)
            contexto_adicional += (
                f"\nEl cliente pide alertas sobre: {kw_texto}. "
                "Confirmale que lo tienes anotado."
            )

    perfil          = obtener_perfil_prospecto(phone)
    contexto_perfil = construir_contexto_perfil(perfil)
    if contexto_perfil:
        contexto_adicional = contexto_perfil + "\n\n" + contexto_adicional

    historial = obtener_historial(conv_id)
    respuesta = await generar_respuesta_gemini(mensaje, historial, contexto_adicional)

    asyncio.create_task(
        extraer_y_actualizar_perfil(mensaje, historial, phone, conv_id, nombre, perfil)
    )

    guardar_mensaje(conv_id, "agente", respuesta, generado_por_ia=True)
    await enviar_whatsapp(phone, respuesta)

    if detectar_senal_cierre(mensaje):
        nombre_display = nombre or phone
        alerta = (
            f"SENAL DE CIERRE DETECTADA\n\n"
            f"Contacto: {nombre_display}\n"
            f"WhatsApp: {phone}\n"
            f"Mensaje: {mensaje[:200]}\n\n"
            f"Respuesta del agente:\n{respuesta[:200]}\n\n"
            f"Entra tu a cerrar la venta."
        )
        await enviar_telegram(alerta)
        if conv_id:
            supabase_admin.table("conversaciones_closer") \
                .update({"etapa": "interesado", "estado": "hot"}) \
                .eq("id", conv_id).execute()
    elif conv_id and conv.get("etapa") == "nuevo":
        supabase_admin.table("conversaciones_closer") \
            .update({
                "etapa":               "respondido",
                "estado":              "engaged",
                "proximo_followup_en": (datetime.utcnow() + timedelta(days=2)).isoformat()
            }) \
            .eq("id", conv_id).execute()
    elif conv_id:
        supabase_admin.table("conversaciones_closer") \
            .update({
                "estado":            "engaged",
                "ultimo_mensaje_en": datetime.utcnow().isoformat()
            }) \
            .eq("id", conv_id).execute()

    print(f"[Closer] OK conv_id={conv_id}")


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
            await asyncio.sleep(3)
    except Exception as e:
        print(f"[Closer] Error followups: {e}")


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

    await enviar_whatsapp(phone, mensaje)
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
    if x_agent_secret != AGENT_SECRET:
        raise HTTPException(status_code=401, detail="Unauthorized")
    background_tasks.add_task(ejecutar_alertas_bg)
    return {"status": "iniciado"}


async def ejecutar_alertas_bg():
    print("[Closer] Cron alertas...")
    try:
        result  = supabase_admin.table("alertas_cliente").select("*").eq("activa", True).execute()
        alertas = result.data or []
        print(f"[Closer] {len(alertas)} alertas activas")
        for alerta in alertas:
            await procesar_alerta_individual(alerta)
            await asyncio.sleep(2)
    except Exception as e:
        print(f"[Closer] Error alertas: {e}")


async def procesar_alerta_individual(alerta: dict):
    phone         = alerta.get("contact_phone")
    nombre        = alerta.get("contact_name", "")
    keywords      = alerta.get("keywords") or []
    instituciones = alerta.get("instituciones") or []

    if not phone or not keywords:
        return

    procesos = buscar_procesos_por_keywords(keywords, instituciones)
    if not procesos:
        return

    ya_notificados = alerta.get("procesos_notificados") or []
    nuevos         = [p.get("codigo_proceso") for p in procesos if p.get("codigo_proceso") not in ya_notificados]

    if not nuevos:
        return

    procesos_nuevos = [p for p in procesos if p.get("codigo_proceso") in nuevos]
    mensaje         = await generar_mensaje_alerta_proceso(nombre or phone, procesos_nuevos, keywords)

    if not mensaje:
        return

    await enviar_whatsapp(phone, mensaje)

    actualizados = list(set(ya_notificados + nuevos))[-20:]
    supabase_admin.table("alertas_cliente") \
        .update({"procesos_notificados": actualizados}) \
        .eq("id", alerta["id"]).execute()

    print(f"[Closer] Alerta enviada a {phone}: {nuevos}")


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
    await enviar_telegram("Test Agente Closer — modulo de alertas funcionando correctamente.")
    return {"status": "ok"}


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
    try:
        print(f"[Closer] Analisis automatico de {codigo_proceso}")

        proceso = buscar_proceso_dgcp(codigo_proceso)
        if not proceso:
            return

        base_url = os.environ.get("RAILWAY_PUBLIC_DOMAIN", "http://localhost:8000")
        if not base_url.startswith("http"):
            base_url = f"https://{base_url}"

        async with httpx.AsyncClient(timeout=120) as client:
            resp = await client.post(
                f"{base_url}/analisis/procesar",
                json={"proceso_id": codigo_proceso},
                headers={"X-Admin-Key": os.environ.get("ADMIN_KEY", "")}
            )
            if resp.status_code == 200:
                await asyncio.sleep(5)
                analisis = buscar_analisis_pliego(codigo_proceso)
                if analisis:
                    resumen = str(analisis.get("resumen_ejecutivo", ""))[:400]
                    mensaje = (
                        f"Listo! Aqui el resumen del proceso {codigo_proceso}:\n\n"
                        f"{resumen}\n\n"
                        f"Para ver el analisis completo con alertas de fraude y checklist, "
                        f"registrate en https://app.licitacionlab.com/"
                    )
                    await enviar_whatsapp(phone, mensaje)
                    guardar_mensaje(conv_id, "agente", mensaje, generado_por_ia=True)
            else:
                print(f"[Closer] Error analisis: {resp.status_code}")
    except Exception as e:
        print(f"[Closer] Error analisis_bg: {e}")
