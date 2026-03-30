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

# Paleta de colores LicitacionLab
VERDE_OSCURO  = (26, 92, 42)
VERDE_CLARO   = (76, 175, 80)
VERDE_MEDIO   = (45, 138, 62)
VERDE_TEXTO   = (190, 225, 195)
VERDE_ACENTO  = (200, 235, 205)
BLANCO        = (255, 255, 255)
FONDO_DER     = (244, 247, 244)
GRIS_LABEL    = (120, 140, 120)
SEP_COLOR     = (220, 235, 220)

# Poppins desde repo — fallback a DejaVu si no existe
import os as _os
_BASE = _os.path.dirname(_os.path.abspath(__file__))
F_BOLD  = _os.path.join(_BASE, "fonts", "Poppins-Bold.ttf")
F_REG   = _os.path.join(_BASE, "fonts", "Poppins-Regular.ttf")
F_LIGHT = _os.path.join(_BASE, "fonts", "Poppins-Light.ttf")
F_MED   = _os.path.join(_BASE, "fonts", "Poppins-Medium.ttf")
if not _os.path.exists(F_BOLD):
    F_BOLD  = "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf"
    F_REG   = "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf"
    F_LIGHT = "/usr/share/fonts/truetype/dejavu/DejaVuSans-ExtraLight.ttf"
    F_MED   = F_REG

def _font(path, size):
    try: return ImageFont.truetype(path, size)
    except: return ImageFont.load_default()

def _draw_wrapped(draw, text, x, y, max_w, fnt, fill, lh=None):
    words = text.split()
    lines, cur = [], ""
    for w in words:
        test = (cur + " " + w).strip()
        bbox = draw.textbbox((0,0), test, font=fnt)
        if bbox[2] - bbox[0] <= max_w: cur = test
        else:
            if cur: lines.append(cur)
            cur = w
    if cur: lines.append(cur)
    lh = lh or (draw.textbbox((0,0), "A", font=fnt)[3] + 8)
    for line in lines:
        draw.text((x, y), line, font=fnt, fill=fill)
        y += lh
    return y


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
    """Genera imagen 1080x1080 con diseño profesional split diagonal LicitacionLab."""
    W, H = 1080, 1080

    # Mapear datos_caption al formato de imagen
    titulo_raw = datos_caption.get("titulo", "LICITACIONES RD")
    tipo_labels = {
        "licitaciones_activas": ("OPORTUNIDAD ACTIVA · DGCP", "LICITACIÓN", "TIPO DE PROCESO", "Licitación pública"),
        "analisis_semanal":     ("ANÁLISIS SEMANAL · DGCP",  "ANÁLISIS",  "PERÍODO",         datetime.now().strftime("%d/%m/%Y")),
        "educativo":            ("TIPS PARA LICITADORES · RD","EDUCATIVO", "CATEGORÍA",       "Capacitación"),
    }
    subtitulo_inst, codigo, campo3_label, campo3_valor = tipo_labels.get(
        tipo, ("LICITACIONES · RD", "INFO", "TIPO", "General"))

    monto_map = {
        "licitaciones_activas": ("85,000,000.00", "Infraestructura"),
        "analisis_semanal":     (f"{random.randint(300,900)},000,000.00", "Múltiples sectores"),
        "educativo":            ("—",             "Educación"),
    }
    monto, sector = monto_map.get(tipo, ("—", "General"))

    img = Image.new("RGB", (W, H), FONDO_DER)
    draw = ImageDraw.Draw(img, "RGBA")

    # Panel izquierdo diagonal
    draw.polygon([(0,0),(500,0),(600,H),(0,H)], fill=VERDE_OSCURO)
    # Franja diagonal verde claro
    draw.polygon([(500,0),(540,0),(640,H),(600,H)], fill=VERDE_CLARO)

    # — Institución
    _draw_wrapped(draw, subtitulo_inst, 40, 40, 440,
                  _font(F_LIGHT, 24), VERDE_TEXTO, lh=34)
    draw.rectangle([40, 115, 480, 117], fill=(255,255,255,100))

    # Logo
    draw.text((40, 128), "LICITACIONES LAB", font=_font(F_BOLD, 26), fill=VERDE_CLARO)
    draw.text((40, 162), "La hacemos por ti", font=_font(F_LIGHT, 18), fill=VERDE_TEXTO)

    # Código
    cod_size = 28 if len(codigo) <= 20 else 24 if len(codigo) <= 26 else 20
    draw.text((40, 210), codigo, font=_font(F_REG, cod_size), fill=VERDE_CLARO)

    # Título principal
    titulo = titulo_raw.upper()
    y_tit = _draw_wrapped(draw, titulo, 40, 265, 440,
                           _font(F_BOLD, 38), BLANCO, lh=52)

    # Doble acento
    draw.rectangle([40, y_tit+12, 90, y_tit+22], fill=VERDE_CLARO)
    draw.rectangle([95, y_tit+12, 120, y_tit+22], fill=(255,255,255,160))

    # Fecha / presentación
    fecha = datetime.now().strftime("%d/%m/%Y")
    draw.text((40, 800), "PRESENTACIÓN DE OFERTAS", font=_font(F_LIGHT, 20), fill=VERDE_CLARO)
    draw.text((40, 830), fecha, font=_font(F_BOLD, 58), fill=BLANCO)
    draw.text((40, 900), "09:00 AM", font=_font(F_BOLD, 32), fill=VERDE_ACENTO)

    # — Panel derecho
    draw.rectangle([668, 40, 673, H-40], fill=VERDE_CLARO)
    draw.text((695, 60),  "MONTO ESTIMADO",  font=_font(F_LIGHT, 22), fill=GRIS_LABEL)
    draw.text((695, 95),  "RD$",             font=_font(F_REG, 40),   fill=VERDE_MEDIO)
    draw.text((695, 140), monto,             font=_font(F_BOLD, 42),  fill=VERDE_OSCURO)
    draw.text((695, 196), "Pesos Dominicanos",font=_font(F_LIGHT, 20),fill=GRIS_LABEL)
    draw.rectangle([695, 242, 1040, 244], fill=SEP_COLOR)

    bloques = [
        ("TIPO DE CONTENIDO", tipo.replace("_"," ").title()),
        ("SECTOR",            sector),
        (campo3_label,        campo3_valor),
    ]
    y_bloque = 265
    for label, valor in bloques:
        draw.text((695, y_bloque), label, font=_font(F_LIGHT, 20), fill=GRIS_LABEL)
        y_bloque = _draw_wrapped(draw, valor, 695, y_bloque+28, 360,
                                  _font(F_REG, 26), VERDE_OSCURO, lh=34)
        y_bloque += 16
        draw.rectangle([695, y_bloque, 1040, y_bloque+1], fill=SEP_COLOR)
        y_bloque += 20

    # Círculos decorativos
    cx, cy = 1060, 60
    for r, alpha in [(90,12),(65,18),(40,25)]:
        overlay = Image.new("RGBA", (W, H), (0,0,0,0))
        ov = ImageDraw.Draw(overlay)
        ov.ellipse([cx-r, cy-r, cx+r, cy+r], outline=(*VERDE_CLARO, alpha), width=2)
        img = Image.alpha_composite(img.convert("RGBA"), overlay).convert("RGB")
        draw = ImageDraw.Draw(img, "RGBA")

    # Watermark
    siglas_map = {"licitaciones_activas":"LIC","analisis_semanal":"EST","educativo":"EDU"}
    siglas = siglas_map.get(tipo, "LL")
    ov2 = Image.new("RGBA", (W, H), (0,0,0,0))
    ov2d = ImageDraw.Draw(ov2)
    ov2d.text((660, 750), siglas, font=_font(F_BOLD, 200), fill=(*VERDE_CLARO, 18))
    img = Image.alpha_composite(img.convert("RGBA"), ov2).convert("RGB")
    draw = ImageDraw.Draw(img, "RGBA")

    # Contacto
    draw.rectangle([660, H-80, W, H-78], fill=VERDE_CLARO)
    draw.text((700, H-68), "@licitacioneslab",  font=_font(F_REG,  22), fill=VERDE_MEDIO)
    draw.text((700, H-42), "Tel: 809-772-5928", font=_font(F_BOLD, 26), fill=VERDE_OSCURO)

    buf = io.BytesIO()
    img.save(buf, format="PNG", optimize=True)
    buf.seek(0)
    return base64.b64encode(buf.read()).decode("utf-8")


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
