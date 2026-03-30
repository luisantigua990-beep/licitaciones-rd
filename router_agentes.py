"""
router_agentes.py
Router FastAPI para los agentes de growth marketing.
Agregar a main.py con: app.include_router(agentes_router)

Endpoints:
  POST /api/agentes/prospector/run        — ejecuta scraping + scoring
  GET  /api/agentes/prospector/cola       — lista prospectos pendientes de enriquecer
  POST /agente-social/generar             — genera posts para Instagram
  POST /agente-social/publicar/{post_id} — publica post aprobado (MOCK por ahora)
  POST /agente-social/rechazar/{post_id} — descarta post rechazado
"""

import os
import io
import base64
import random
import asyncio
import httpx
import json
import re
from datetime import datetime
from typing import Optional

from fastapi import APIRouter, BackgroundTasks, Header, HTTPException
from pydantic import BaseModel
from supabase import create_client
from PIL import Image, ImageDraw, ImageFont
import anthropic

from scraper_rnce import run_prospector

# ── Config ───────────────────────────────────────────
SUPABASE_URL      = os.environ["SUPABASE_URL"]
SUPABASE_KEY      = os.environ["SUPABASE_KEY"]
CLAUDE_KEY        = os.environ["ANTHROPIC_API_KEY"]
AGENT_SECRET      = os.environ.get("AGENT_SECRET", "licitacionlab-growth-2026")

supabase          = create_client(SUPABASE_URL, SUPABASE_KEY)
agentes_router    = APIRouter(prefix="/api/agentes", tags=["agentes"])
social_router     = APIRouter(prefix="/agente-social", tags=["Agente Social"])


# ════════════════════════════════════════════════════════
# AGENTE 1 — PROSPECTOR
# ════════════════════════════════════════════════════════

class ProspectorConfig(BaseModel):
    max_rnce:  int = 80
    max_maps:  int = 40
    score_min: int = 60


async def score_empresa(empresa: dict) -> dict:
    prompt = f"""Eres un analista de ventas B2B especializado en el sector construcción dominicano.
Evalúa esta empresa como prospecto para LicitacionLab, plataforma SaaS que monitorea licitaciones DGCP y analiza pliegos con IA.

Datos de la empresa:
- Nombre: {empresa.get('nombre', '')}
- RNC: {empresa.get('rnc', 'No disponible')}
- Categoría RNCE: {empresa.get('categoria', 'No disponible')}
- Región: {empresa.get('region', 'No disponible')}
- Fuente: {empresa.get('fuente', '')}

Criterios de scoring (total 100 pts):
- Está en RNCE (ya participa en licitaciones): +35 pts automático
- Sector construcción / ingeniería civil / infraestructura: +25 pts
- Región Santo Domingo o Santiago (mayor actividad licitaciones): +20 pts
- Tiene datos de contacto disponibles: +10 pts
- Nombre sugiere empresa formal (SRL, SA, CxA): +10 pts

Devuelve SOLO un JSON válido, sin texto adicional:
{{"score": <0-100>, "justificacion": "<máximo 15 palabras>", "prioridad": "<alta|media|baja>"}}"""

    try:
        async with httpx.AsyncClient(timeout=20.0) as client:
            resp = await client.post(
                "https://api.anthropic.com/v1/messages",
                headers={
                    "x-api-key": CLAUDE_KEY,
                    "anthropic-version": "2023-06-01",
                    "content-type": "application/json",
                },
                json={
                    "model": "claude-haiku-4-5-20251001",
                    "max_tokens": 150,
                    "messages": [{"role": "user", "content": prompt}]
                }
            )
            data = resp.json()
            texto = data["content"][0]["text"].strip()
            match = re.search(r'\{.*\}', texto, re.DOTALL)
            if match:
                resultado = json.loads(match.group())
                empresa["score"]         = int(resultado.get("score", 0))
                empresa["justificacion"] = resultado.get("justificacion", "")
                empresa["prioridad"]     = resultado.get("prioridad", "baja")
            else:
                empresa["score"]         = 30
                empresa["justificacion"] = "No se pudo evaluar"
                empresa["prioridad"]     = "baja"
    except Exception as e:
        print(f"[Scoring] Error en {empresa.get('nombre')}: {e}")
        empresa["score"]         = 20
        empresa["justificacion"] = "Error en evaluación"
        empresa["prioridad"]     = "baja"

    return empresa


async def guardar_prospectos(empresas: list[dict], score_min: int) -> dict:
    guardados = 0
    descartados = 0
    errores = 0

    for e in empresas:
        if e.get("score", 0) < score_min:
            descartados += 1
            continue

        registro = {
            "nombre":        e.get("nombre", ""),
            "rnc":           e.get("rnc") or None,
            "categoria":     e.get("categoria", ""),
            "region":        e.get("region", ""),
            "telefono":      e.get("telefono") or None,
            "email":         e.get("email") or None,
            "web":           e.get("web") or None,
            "fuente":        e.get("fuente", ""),
            "score":         e.get("score", 0),
            "justificacion": e.get("justificacion", ""),
            "prioridad":     e.get("prioridad", "baja"),
            "estado":        "pendiente",
        }

        try:
            clave = {"rnc": registro["rnc"]} if registro["rnc"] else {"nombre": registro["nombre"]}
            supabase.table("prospectos").upsert(
                {**registro, **clave},
                on_conflict="rnc" if registro["rnc"] else "nombre"
            ).execute()
            guardados += 1
        except Exception as ex:
            print(f"[Supabase] Error guardando {e.get('nombre')}: {ex}")
            errores += 1

    return {"guardados": guardados, "descartados": descartados, "errores": errores}


async def ejecutar_prospector_bg(config: ProspectorConfig):
    print(f"[Prospector] Iniciando — max_rnce={config.max_rnce}, max_maps={config.max_maps}")
    empresas = await run_prospector(max_rnce=config.max_rnce, max_maps=config.max_maps)

    LOTE = 10
    empresas_scored = []
    for i in range(0, len(empresas), LOTE):
        lote = empresas[i:i+LOTE]
        tareas = [score_empresa(e) for e in lote]
        resultado = await asyncio.gather(*tareas)
        empresas_scored.extend(resultado)
        await asyncio.sleep(1)

    resumen = await guardar_prospectos(empresas_scored, config.score_min)
    print(f"[Prospector] Completado: {resumen}")


@agentes_router.post("/prospector/run")
async def run_prospector_endpoint(
    config: ProspectorConfig,
    background_tasks: BackgroundTasks,
    x_agent_secret: Optional[str] = Header(None)
):
    if x_agent_secret != AGENT_SECRET:
        raise HTTPException(status_code=401, detail="Unauthorized")

    background_tasks.add_task(ejecutar_prospector_bg, config)
    return {
        "status": "iniciado",
        "mensaje": "Prospector corriendo en background. Resultado en Supabase tabla prospectos.",
        "config": config.dict()
    }


@agentes_router.get("/prospector/cola")
async def get_cola_prospectos(
    limite: int = 20,
    score_min: int = 60,
    x_agent_secret: Optional[str] = Header(None)
):
    if x_agent_secret != AGENT_SECRET:
        raise HTTPException(status_code=401, detail="Unauthorized")

    result = supabase.table("prospectos") \
        .select("*") \
        .eq("estado", "pendiente") \
        .gte("score", score_min) \
        .order("score", desc=True) \
        .limit(limite) \
        .execute()

    return {
        "total": len(result.data),
        "prospectos": result.data
    }


# ════════════════════════════════════════════════════════
# AGENTE 4 — SOCIAL
# ════════════════════════════════════════════════════════

COLORES = {
    "primario":  (30, 64, 175),
    "acento":    (59, 130, 246),
    "fondo":     (15, 23, 42),
    "texto":     (248, 250, 252),
    "subtexto":  (148, 163, 184),
    "verde":     (34, 197, 94),
    "amarillo":  (234, 179, 8),
}


class SocialRequest(BaseModel):
    tipo_contenido: str
    cantidad: int = 1


class SocialResponse(BaseModel):
    posts_generados: int
    posts: list


def generar_caption(tipo: str, datos_contexto: dict) -> dict:
    client = anthropic.Anthropic(api_key=CLAUDE_KEY)

    prompts = {
        "licitaciones_activas": f"""Eres el social media manager de LicitacionLab, plataforma SaaS que monitorea licitaciones del DGCP en República Dominicana.

Genera un post de Instagram sobre esta licitación activa:
- Entidad: {datos_contexto.get('entidad', 'DGCP')}
- Descripción: {datos_contexto.get('descripcion', 'Obras de infraestructura')}
- Monto referencial: {datos_contexto.get('monto', 'No especificado')}
- Fecha límite: {datos_contexto.get('fecha_limite', 'Próximamente')}

El caption debe:
1. Empezar con un emoji relevante y frase de gancho (ej: "⚠️ OPORTUNIDAD ACTIVA:")
2. Describir la licitación en 2-3 líneas claras
3. Mencionar que LicitacionLab la detectó automáticamente
4. CTA: "Regístrate gratis en licitacionlab.com"
5. Máximo 150 palabras

Responde SOLO en JSON con este formato exacto:
{{"titulo": "texto del título para la imagen", "caption": "texto completo del post", "hashtags": "#licitacion #construccion #dgcp #republicadominicana #licitacionlab"}}""",

        "analisis_semanal": f"""Eres el social media manager de LicitacionLab, plataforma SaaS de licitaciones en República Dominicana.

Genera un post de análisis semanal del mercado de licitaciones con estos datos:
- Semana: {datos_contexto.get('semana', datetime.now().strftime('%d/%m/%Y'))}
- Total licitaciones publicadas: {datos_contexto.get('total', random.randint(15, 45))}
- Sector con más actividad: {datos_contexto.get('sector_top', 'Infraestructura vial')}
- Monto total aproximado: RD$ {datos_contexto.get('monto_total', f"{random.randint(200, 800)}M")}
- Tendencia: {datos_contexto.get('tendencia', 'al alza vs semana anterior')}

El caption debe:
1. Empezar con "📊 RESUMEN SEMANAL DE LICITACIONES RD"
2. Presentar los números clave de forma visual (usa emojis como bullets)
3. Una insight/conclusión de negocio en 1 línea
4. CTA hacia licitacionlab.com
5. Máximo 120 palabras

Responde SOLO en JSON:
{{"titulo": "RESUMEN SEMANAL\\nLICITACIONES RD", "caption": "texto", "hashtags": "#licitaciones #construccionrd #dgcp #republicadominicana #licitacionlab"}}""",

        "educativo": f"""Eres el social media manager de LicitacionLab, experto en licitaciones públicas de República Dominicana.

Genera un post educativo sobre el tema: {datos_contexto.get('tema', 'Cómo calcular el margen mínimo en una oferta económica')}

El caption debe:
1. Empezar con "💡 SABÍAS QUE..." o "📚 TIP LICITADOR:"
2. Explicar el concepto de forma simple y práctica (3-4 puntos)
3. Mencionar cómo LicitacionLab ayuda con esto
4. CTA: "Síguenos para más tips"
5. Máximo 130 palabras

Responde SOLO en JSON:
{{"titulo": "texto corto para imagen (máx 4 palabras)", "caption": "texto", "hashtags": "#tiplicitador #licitaciones #construccion #dgcp #licitacionlab #republicadominicana"}}"""
    }

    mensaje = client.messages.create(
        model="claude-haiku-4-5-20251001",
        max_tokens=600,
        messages=[{"role": "user", "content": prompts.get(tipo, prompts["educativo"])}]
    )

    texto = mensaje.content[0].text.strip()
    texto = texto.replace("```json", "").replace("```", "").strip()
    return json.loads(texto)


def generar_imagen_post(tipo: str, datos_caption: dict) -> str:
    W, H = 1080, 1080
    img = Image.new("RGB", (W, H), COLORES["fondo"])
    draw = ImageDraw.Draw(img)

    for i in range(0, H // 2, 4):
        color = (
            COLORES["primario"][0],
            COLORES["primario"][1],
            min(255, COLORES["primario"][2] + i // 4)
        )
        draw.rectangle([0, i, W, i + 4], fill=color)

    draw.rectangle([0, 0, W, 8], fill=COLORES["acento"])
    draw.rectangle([0, H - 8, W, H], fill=COLORES["acento"])

    try:
        font_logo     = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf", 32)
        font_titulo   = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf", 64)
        font_subtitulo= ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf", 36)
        font_small    = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf", 28)
    except:
        font_logo      = ImageFont.load_default()
        font_titulo    = font_logo
        font_subtitulo = font_logo
        font_small     = font_logo

    draw.text((60, 40), "LICITACIONLAB", font=font_logo, fill=COLORES["acento"])
    draw.rectangle([60, 100, W - 60, 104], fill=COLORES["acento"])

    titulo = datos_caption.get("titulo", "LICITACIONES RD").upper()
    lineas_titulo = titulo.split("\\n") if "\\n" in titulo else [titulo]

    y_titulo = 180
    for linea in lineas_titulo:
        font_use = font_titulo
        while True:
            bbox = draw.textbbox((0, 0), linea, font=font_use)
            tw = bbox[2] - bbox[0]
            if tw < W - 120 or font_use.size <= 30:
                break
            try:
                font_use = ImageFont.truetype(
                    "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
                    font_use.size - 4
                )
            except:
                break

        bbox = draw.textbbox((0, 0), linea, font=font_use)
        tw = bbox[2] - bbox[0]
        draw.text(((W - tw) // 2, y_titulo), linea, font=font_use, fill=COLORES["texto"])
        y_titulo += bbox[3] - bbox[1] + 20

    draw.rectangle([W // 2 - 80, y_titulo + 20, W // 2 + 80, y_titulo + 26], fill=COLORES["verde"])

    subtitulos = {
        "licitaciones_activas": "OPORTUNIDAD ACTIVA · DGCP",
        "analisis_semanal": f"SEMANA {datetime.now().strftime('%d/%m/%Y')} · REPÚBLICA DOMINICANA",
        "educativo": "TIP PARA LICITADORES · RD"
    }
    subtitulo = subtitulos.get(tipo, "LICITACIONES · REPÚBLICA DOMINICANA")
    bbox = draw.textbbox((0, 0), subtitulo, font=font_subtitulo)
    tw = bbox[2] - bbox[0]
    draw.text(((W - tw) // 2, y_titulo + 50), subtitulo, font=font_subtitulo, fill=COLORES["subtexto"])

    cta = "licitacionlab.com"
    bbox = draw.textbbox((0, 0), cta, font=font_subtitulo)
    tw = bbox[2] - bbox[0]
    draw.text(((W - tw) // 2, H - 100), cta, font=font_subtitulo, fill=COLORES["acento"])

    fecha = datetime.now().strftime("%d/%m/%Y")
    draw.text((W - 200, H - 100), fecha, font=font_small, fill=COLORES["subtexto"])

    buffer = io.BytesIO()
    img.save(buffer, format="PNG", optimize=True)
    buffer.seek(0)
    return base64.b64encode(buffer.read()).decode("utf-8")


def obtener_contexto(tipo: str) -> dict:
    if tipo == "licitaciones_activas":
        try:
            result = supabase.table("prospectos") \
                .select("empresa, sector") \
                .order("created_at", desc=True) \
                .limit(1) \
                .execute()
            if result.data:
                empresa = result.data[0]
                return {
                    "entidad": empresa.get("empresa", "Entidad pública"),
                    "descripcion": "Obras de infraestructura vial y mantenimiento",
                    "monto": "RD$ 45,000,000",
                    "fecha_limite": datetime.now().strftime("%d/%m/%Y")
                }
        except:
            pass
        return {
            "entidad": "MOPC",
            "descripcion": "Construcción y rehabilitación de carreteras",
            "monto": "RD$ 85,000,000",
            "fecha_limite": datetime.now().strftime("%d/%m/%Y")
        }

    elif tipo == "analisis_semanal":
        try:
            result = supabase.table("prospectos") \
                .select("score", count="exact") \
                .gte("score", 60) \
                .execute()
            total = result.count or random.randint(20, 40)
        except:
            total = random.randint(20, 40)
        return {
            "semana": datetime.now().strftime("%d/%m/%Y"),
            "total": total,
            "sector_top": "Infraestructura y obras civiles",
            "monto_total": f"{random.randint(300, 900)}M",
            "tendencia": "+12% vs semana anterior"
        }

    else:
        temas = [
            "Qué es el RNCE y por qué necesitas estar registrado",
            "Cómo calcular el margen mínimo en una oferta económica",
            "Los 5 documentos que nunca pueden faltar en tu oferta",
            "Diferencia entre LPN, LPC y Comparación de Precios",
            "Cómo leer un pliego de condiciones en 30 minutos",
            "Qué es la Ley 47-25 y cómo te afecta como contratista",
        ]
        return {"tema": random.choice(temas)}


@social_router.post("/generar", response_model=SocialResponse)
async def generar_posts_sociales(
    req: SocialRequest,
    x_agent_secret: Optional[str] = Header(None)
):
    if x_agent_secret != AGENT_SECRET:
        raise HTTPException(status_code=401, detail="No autorizado")

    cantidad = max(1, min(req.cantidad, 3))
    tipos_disponibles = ["licitaciones_activas", "analisis_semanal", "educativo"]

    if req.tipo_contenido == "rotativo":
        dia = datetime.now().weekday()
        tipo_map = {0: "licitaciones_activas", 2: "analisis_semanal", 4: "educativo"}
        tipo = tipo_map.get(dia, "licitaciones_activas")
    else:
        tipo = req.tipo_contenido
        if tipo not in tipos_disponibles:
            raise HTTPException(status_code=400, detail=f"tipo_contenido debe ser uno de {tipos_disponibles}")

    posts_generados = []

    for i in range(cantidad):
        tipo_actual = tipos_disponibles[i % len(tipos_disponibles)] if cantidad > 1 else tipo
        contexto = obtener_contexto(tipo_actual)
        datos_caption = generar_caption(tipo_actual, contexto)
        imagen_b64 = generar_imagen_post(tipo_actual, datos_caption)

        post_data = {
            "tipo_contenido": tipo_actual,
            "caption":        datos_caption.get("caption"),
            "hashtags":       datos_caption.get("hashtags"),
            "titulo_imagen":  datos_caption.get("titulo"),
            "imagen_b64":     imagen_b64,
            "estado":         "pendiente_aprobacion",
            "plataforma":     "instagram",
            "created_at":     datetime.utcnow().isoformat()
        }

        result = supabase.table("social_log").insert(post_data).execute()
        post_id = result.data[0]["id"] if result.data else None

        posts_generados.append({
            "id":         post_id,
            "tipo":       tipo_actual,
            "titulo":     datos_caption.get("titulo"),
            "caption":    datos_caption.get("caption"),
            "hashtags":   datos_caption.get("hashtags"),
            "imagen_b64": imagen_b64,
            "estado":     "pendiente_aprobacion"
        })

    return SocialResponse(posts_generados=len(posts_generados), posts=posts_generados)


@social_router.post("/publicar/{post_id}")
async def publicar_post(
    post_id: int,
    x_agent_secret: Optional[str] = Header(None)
):
    """
    MOCK por ahora. Cuando tengas Meta API reemplaza el bloque MOCK con:
      IG_USER_ID    = os.environ["IG_USER_ID"]
      IG_ACCESS_TOKEN = os.environ["IG_ACCESS_TOKEN"]
      1. Subir imagen a URL pública (Supabase Storage)
      2. POST graph.facebook.com/v19.0/{IG_USER_ID}/media  → creation_id
      3. POST graph.facebook.com/v19.0/{IG_USER_ID}/media_publish → ig_post_id
    """
    if x_agent_secret != AGENT_SECRET:
        raise HTTPException(status_code=401, detail="No autorizado")

    result = supabase.table("social_log").select("*").eq("id", post_id).single().execute()
    if not result.data:
        raise HTTPException(status_code=404, detail="Post no encontrado")

    post = result.data
    mock_ig_post_id = f"MOCK_{post_id}_{int(datetime.now().timestamp())}"

    supabase.table("social_log").update({
        "estado":       "publicado",
        "ig_post_id":   mock_ig_post_id,
        "publicado_at": datetime.utcnow().isoformat()
    }).eq("id", post_id).execute()

    return {
        "success":        True,
        "post_id":        post_id,
        "ig_post_id":     mock_ig_post_id,
        "mensaje":        "Post publicado (MOCK — conectar Meta API cuando esté disponible)",
        "caption_preview": post["caption"][:100] + "..."
    }


@social_router.post("/rechazar/{post_id}")
async def rechazar_post(
    post_id: int,
    x_agent_secret: Optional[str] = Header(None)
):
    if x_agent_secret != AGENT_SECRET:
        raise HTTPException(status_code=401, detail="No autorizado")

    supabase.table("social_log").update({
        "estado":     "rechazado",
        "updated_at": datetime.utcnow().isoformat()
    }).eq("id", post_id).execute()

    return {"success": True, "mensaje": "Post rechazado y descartado"}
