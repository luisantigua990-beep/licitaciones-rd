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
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", os.environ.get("TELEGRAM_BOT_TOKEN", ""))
TELEGRAM_CHAT_ID   = os.environ.get("TELEGRAM_CHAT_ID", "817596333")

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
- Código: {datos_contexto.get('codigo', '')}
- Descripción: {datos_contexto.get('descripcion', 'Obras de infraestructura')}
- Sector: {datos_contexto.get('sector', 'Infraestructura')}
- Fecha límite: {datos_contexto.get('fecha_limite', 'Próximamente')}

El caption debe:
1. Comenzar con "OPORTUNIDAD ACTIVA:" sin emojis
2. Describir la licitación usando la descripción exacta proporcionada, en 2-3 líneas sin mencionar el monto (ya va en la imagen)
3. Mencionar que LicitacionLab la detectó automáticamente
4. CTA: "Regístrate gratis en https://app.licitacionlab.com/"
5. Sin emojis en todo el caption
6. Máximo 130 palabras

Responde SOLO en JSON con este formato exacto:
{{"titulo": "texto del título para la imagen (máx 5 palabras)", "caption": "texto completo del post SIN emojis y SIN monto", "hashtags": "#licitacion #construccion #dgcp #republicadominicana #licitacionlab"}}""",

        "analisis_semanal": f"""Eres el social media manager de LicitacionLab, plataforma SaaS de licitaciones en República Dominicana.

Genera un post de análisis semanal del mercado de licitaciones con estos datos:
- Semana: {datos_contexto.get('semana', datetime.now().strftime('%d/%m/%Y'))}
- Total licitaciones publicadas: {datos_contexto.get('total', random.randint(15, 45))}
- Sector con más actividad: {datos_contexto.get('sector_top', 'Infraestructura vial')}
- Monto total aproximado: RD$ {datos_contexto.get('monto_total', f"{random.randint(200, 800)}M")}
- Tendencia: {datos_contexto.get('tendencia', 'al alza vs semana anterior')}

El caption debe:
1. Empezar con "RESUMEN SEMANAL DE LICITACIONES RD" sin emojis
2. Presentar los números clave de forma clara sin emojis
3. Una insight/conclusión de negocio en 1 línea
4. CTA: "Accede en https://app.licitacionlab.com/"
5. Sin emojis en todo el caption
6. Máximo 120 palabras

Responde SOLO en JSON:
{{"titulo": "RESUMEN SEMANAL\\nLICITACIONES RD", "caption": "texto", "hashtags": "#licitaciones #construccionrd #dgcp #republicadominicana #licitacionlab"}}""",

        "educativo": f"""Eres el social media manager de LicitacionLab, experto en licitaciones públicas de República Dominicana.

Genera contenido educativo para un carrusel de Instagram sobre:
TEMA: {datos_contexto.get('tema', 'Cómo participar en licitaciones públicas en RD')}
CATEGORÍA: {datos_contexto.get('categoria', 'general')}

Decide cuántos puntos (entre 3 y 5) son necesarios para explicar bien el tema.
Cada punto va en una imagen separada del carrusel.

Reglas:
- Lenguaje simple y directo para empresas dominicanas
- Sin emojis en ningún campo
- Cada punto debe ser autónomo y legible solo

Responde SOLO en JSON válido (sin backticks ni texto extra):
{{
  "titulo_portada": "Título impactante para la portada (máx 5 palabras, mayúsculas)",
  "subtitulo_portada": "Subtítulo de apoyo (máx 10 palabras)",
  "puntos": [
    {{"numero": 1, "titulo": "Título del punto (máx 5 palabras)", "texto": "Explicación clara en 2-3 oraciones concretas para el contexto dominicano"}},
    {{"numero": 2, "titulo": "...", "texto": "..."}},
    {{"numero": 3, "titulo": "...", "texto": "..."}}
  ],
  "caption": "Caption corto para Instagram: empieza con TIP LICITADOR: o SABIAS QUE, resume el tema, menciona LicitacionLab, sin emojis, máx 80 palabras",
  "hashtags": "#tiplicitador #licitaciones #dgcp #republicadominicana #licitacionlab #construccion"
}}"""
    }

    mensaje = client.messages.create(
        model="claude-haiku-4-5-20251001",
        max_tokens=1200,
        messages=[{"role": "user", "content": prompts.get(tipo, prompts["educativo"])}]
    )

    texto = mensaje.content[0].text.strip()
    texto = texto.replace("```json", "").replace("```", "").strip()
    return json.loads(texto)



# ════════════════════════════════════════════════════════
# CARRUSEL EDUCATIVO — generación de múltiples imágenes
# ════════════════════════════════════════════════════════

def _generar_imagen_portada_edu(titulo: str, subtitulo: str, categoria: str) -> str:
    """Imagen 1: portada del carrusel educativo. Fondo sólido verde + franja inferior."""
    W, H = 1080, 1080
    img = Image.new("RGB", (W, H), VERDE_OSCURO)
    draw = ImageDraw.Draw(img, "RGBA")

    # Franja inferior 22%
    draw.rectangle([(0, int(H * 0.78)), (W, H)], fill=VERDE_MEDIO)
    # Línea acento
    draw.rectangle([(0, int(H * 0.78)), (W, int(H * 0.78) + 5)], fill=VERDE_CLARO)

    # Etiqueta categoría
    cat_text = categoria.upper()
    draw.rectangle([(60, 55), (60 + len(cat_text) * 18 + 20, 102)], fill=VERDE_CLARO)
    draw.text((70, 60), cat_text, font=_font(F_BOLD, 28), fill=VERDE_OSCURO)

    # Título principal centrado
    y = 170
    for linea in _wrap_lines(titulo.upper(), draw, _font(F_BOLD, 72), W - 120):
        bbox = draw.textbbox((0, 0), linea, font=_font(F_BOLD, 72))
        x = (W - (bbox[2] - bbox[0])) // 2
        draw.text((x, y), linea, font=_font(F_BOLD, 72), fill=BLANCO)
        y += 88

    # Subtítulo centrado
    y += 20
    for linea in _wrap_lines(subtitulo, draw, _font(F_MED, 36), W - 160):
        bbox = draw.textbbox((0, 0), linea, font=_font(F_MED, 36))
        x = (W - (bbox[2] - bbox[0])) // 2
        draw.text((x, y), linea, font=_font(F_MED, 36), fill=VERDE_TEXTO)
        y += 50

    # Indicador DESLIZA con flecha
    desliza = "DESLIZA  ▶"
    bbox = draw.textbbox((0, 0), desliza, font=_font(F_BOLD, 30))
    x_d = (W - (bbox[2] - bbox[0])) // 2
    draw.text((x_d, int(H * 0.80) + 18), desliza, font=_font(F_BOLD, 30), fill=BLANCO)

    # Logo LicitacionLab
    logo = "LicitacionLab"
    bbox = draw.textbbox((0, 0), logo, font=_font(F_BOLD, 32))
    x_l = (W - (bbox[2] - bbox[0])) // 2
    draw.text((x_l, int(H * 0.80) + 62), logo, font=_font(F_BOLD, 32), fill=VERDE_CLARO)

    # Watermark EDU
    ov = Image.new("RGBA", (W, H), (0, 0, 0, 0))
    ovd = ImageDraw.Draw(ov)
    ovd.text((550, 600), "EDU", font=_font(F_BOLD, 260), fill=(*VERDE_CLARO, 15))
    img = Image.alpha_composite(img.convert("RGBA"), ov).convert("RGB")

    buf = io.BytesIO()
    img.save(buf, format="PNG", optimize=True)
    return base64.b64encode(buf.getvalue()).decode("utf-8")


def _wrap_lines(text: str, draw, fnt, max_w: int) -> list:
    """Divide texto en líneas respetando max_w píxeles."""
    words = text.split()
    lines, cur = [], ""
    for w in words:
        test = (cur + " " + w).strip()
        bbox = draw.textbbox((0, 0), test, font=fnt)
        if bbox[2] - bbox[0] <= max_w:
            cur = test
        else:
            if cur:
                lines.append(cur)
            cur = w
    if cur:
        lines.append(cur)
    return lines or [text]


def _generar_imagen_punto_edu(numero: int, titulo_punto: str, texto_punto: str, total_puntos: int) -> str:
    """Imagen de punto clave: número grande a la izquierda, título y texto a la derecha."""
    W, H = 1080, 1080
    img = Image.new("RGB", (W, H), FONDO_DER)
    draw = ImageDraw.Draw(img, "RGBA")

    # Panel izquierdo con número
    draw.polygon([(0, 0), (420, 0), (360, H), (0, H)], fill=VERDE_OSCURO)
    # Franja diagonal acento
    draw.polygon([(420, 0), (460, 0), (400, H), (360, H)], fill=VERDE_CLARO)

    # Número grande centrado en el panel izquierdo
    num_str = str(numero)
    fnt_num = _font(F_BOLD, 280)
    bbox_n = draw.textbbox((0, 0), num_str, font=fnt_num)
    x_num = (380 - (bbox_n[2] - bbox_n[0])) // 2
    y_num = (H - (bbox_n[3] - bbox_n[1])) // 2 - 40
    draw.text((x_num, y_num), num_str, font=fnt_num, fill=(*VERDE_CLARO, 220))

    # Indicador de progreso (puntos) en panel izquierdo abajo
    dot_y = H - 80
    dot_spacing = 24
    total_dot_w = total_puntos * dot_spacing
    dot_x_start = (380 - total_dot_w) // 2
    for i in range(total_puntos):
        color = BLANCO if i + 1 == numero else (*VERDE_TEXTO, 120)
        cx = dot_x_start + i * dot_spacing + 8
        draw.ellipse([cx - 7, dot_y - 7, cx + 7, dot_y + 7], fill=color)

    # Panel derecho: título del punto
    x_right = 490
    draw.text((x_right, 100), f"PUNTO {numero}", font=_font(F_LIGHT, 26), fill=GRIS_LABEL)

    y_tit = 148
    fnt_tit = _font(F_BOLD, 52)
    for linea in _wrap_lines(titulo_punto.upper(), draw, fnt_tit, W - x_right - 60):
        draw.text((x_right, y_tit), linea, font=fnt_tit, fill=VERDE_OSCURO)
        y_tit += 64

    # Línea separadora
    draw.rectangle([x_right, y_tit + 10, W - 60, y_tit + 13], fill=VERDE_CLARO)

    # Texto explicativo
    y_txt = y_tit + 40
    fnt_txt = _font(F_REG, 34)
    for linea in _wrap_lines(texto_punto, draw, fnt_txt, W - x_right - 60):
        draw.text((x_right, y_txt), linea, font=fnt_txt, fill=VERDE_OSCURO)
        y_txt += 48

    # Footer con desliza (excepto último punto, ese no dice desliza)
    draw.rectangle([x_right, H - 90, W - 40, H - 88], fill=VERDE_CLARO)
    draw.text((x_right, H - 80), "LicitacionLab · app.licitacionlab.com",
              font=_font(F_REG, 22), fill=GRIS_LABEL)

    buf = io.BytesIO()
    img.save(buf, format="PNG", optimize=True)
    return base64.b64encode(buf.getvalue()).decode("utf-8")


def _generar_imagen_cta_edu() -> str:
    """Última imagen del carrusel: CTA con call to action claro."""
    W, H = 1080, 1080
    img = Image.new("RGB", (W, H), VERDE_OSCURO)
    draw = ImageDraw.Draw(img, "RGBA")

    # Fondo decorativo
    ov = Image.new("RGBA", (W, H), (0, 0, 0, 0))
    ovd = ImageDraw.Draw(ov)
    for r, a in [(480, 8), (380, 12), (280, 18), (180, 25)]:
        ovd.ellipse([W//2 - r, H//2 - r, W//2 + r, H//2 + r],
                    outline=(*VERDE_CLARO, a), width=3)
    img = Image.alpha_composite(img.convert("RGBA"), ov).convert("RGB")
    draw = ImageDraw.Draw(img)

    # Franja superior
    draw.rectangle([(0, 0), (W, 12)], fill=VERDE_CLARO)

    # Texto CTA centrado
    lines_cta = [
        ("EMPIEZA A GANAR", _font(F_BOLD, 68), BLANCO, 260),
        ("LICITACIONES HOY", _font(F_BOLD, 68), VERDE_CLARO, 350),
        ("", None, None, 0),
        ("Monitorea todas las licitaciones del DGCP", _font(F_REG, 34), VERDE_TEXTO, 480),
        ("en tiempo real con inteligencia artificial.", _font(F_REG, 34), VERDE_TEXTO, 528),
    ]
    for texto, fnt, color, y in lines_cta:
        if not texto:
            continue
        bbox = draw.textbbox((0, 0), texto, font=fnt)
        x = (W - (bbox[2] - bbox[0])) // 2
        draw.text((x, y), texto, font=fnt, fill=color)

    # Caja URL
    draw.rectangle([(140, 640), (940, 730)], fill=VERDE_CLARO)
    url = "app.licitacionlab.com"
    bbox = draw.textbbox((0, 0), url, font=_font(F_BOLD, 46))
    x_url = (W - (bbox[2] - bbox[0])) // 2
    draw.text((x_url, 655), url, font=_font(F_BOLD, 46), fill=VERDE_OSCURO)

    # Registrate gratis
    reg = "REGISTRATE GRATIS"
    bbox = draw.textbbox((0, 0), reg, font=_font(F_BOLD, 38))
    x_reg = (W - (bbox[2] - bbox[0])) // 2
    draw.text((x_reg, 768), reg, font=_font(F_BOLD, 38), fill=BLANCO)

    # Logo
    logo = "LicitacionLab"
    bbox = draw.textbbox((0, 0), logo, font=_font(F_BOLD, 32))
    x_l = (W - (bbox[2] - bbox[0])) // 2
    draw.text((x_l, H - 80), logo, font=_font(F_BOLD, 32), fill=VERDE_CLARO)

    buf = io.BytesIO()
    img.save(buf, format="PNG", optimize=True)
    return base64.b64encode(buf.getvalue()).decode("utf-8")


def generar_carrusel_educativo(datos_caption: dict) -> list:
    """
    Genera lista de imágenes b64 para el carrusel educativo.
    Retorna: [portada_b64, punto1_b64, punto2_b64, ..., cta_b64]
    """
    ctx = datos_caption.get("_imagen_datos", {})
    categoria = ctx.get("categoria", "general")
    titulo_portada = datos_caption.get("titulo_portada") or datos_caption.get("titulo", "APRENDE A LICITAR")
    subtitulo_portada = datos_caption.get("subtitulo_portada", "")
    puntos = datos_caption.get("puntos", [])

    imagenes = []

    # Imagen 1: portada
    imagenes.append(_generar_imagen_portada_edu(titulo_portada, subtitulo_portada, categoria))

    # Imágenes por punto
    total = len(puntos)
    for p in puntos:
        imagenes.append(_generar_imagen_punto_edu(
            numero=p.get("numero", 1),
            titulo_punto=p.get("titulo", ""),
            texto_punto=p.get("texto", ""),
            total_puntos=total
        ))

    # Última imagen: CTA
    imagenes.append(_generar_imagen_cta_edu())

    return imagenes

def generar_imagen_post(tipo: str, datos_caption: dict) -> str:
    """Genera imagen 1080x1080 con diseño profesional split diagonal LicitacionLab."""
    W, H = 1080, 1080

    # Usar datos reales del contexto si están disponibles
    ctx = datos_caption.get("_imagen_datos", {})
    titulo_raw = ctx.get("titulo") or datos_caption.get("titulo", "LICITACIONES RD")

    if tipo == "licitaciones_activas" and ctx:
        subtitulo_inst = ctx.get("entidad", "DGCP")
        codigo         = ctx.get("codigo", "DGCP-2026")
        monto_raw      = ctx.get("monto_raw", "")
        # Formatear monto con separadores de miles
        try:
            m = float(monto_raw)
            monto = f"{m:,.2f}"
        except:
            monto = ctx.get("monto", "—").replace("RD$ ", "")
        sector         = ctx.get("sector", "Infraestructura")
        campo3_label   = ctx.get("campo3_label", "PROVINCIA")
        campo3_valor   = ctx.get("campo3_valor", "Nacional")
    elif tipo == "analisis_semanal" and ctx:
        subtitulo_inst = "RESUMEN SEMANAL · DGCP"
        codigo         = f"SEMANA {datetime.now().strftime('%d/%m/%Y')}"
        monto          = ctx.get("monto_total", "—").replace("RD$ ", "")
        sector         = ctx.get("sector_top", "Infraestructura")
        campo3_label   = "PERÍODO"
        campo3_valor   = datetime.now().strftime("%d/%m/%Y")
    else:  # educativo
        categoria_edu  = (ctx.get("categoria") or "general").upper()
        subtitulo_inst = "EDUCACIÓN · LICITACIONES RD"
        codigo         = f"CATEGORÍA: {categoria_edu}"
        monto          = "—"
        sector         = "Capacitación"
        campo3_label   = "CATEGORÍA"
        campo3_valor   = (ctx.get("categoria") or "general").capitalize()

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

    # Fecha / presentación — usar fecha real del proceso
    fecha_raw = ctx.get("fecha_limite", "")
    hora_str = "10:00 AM"
    if fecha_raw:
        try:
            # fecha_raw puede venir como "20/05/2026" o con hora "20/05/2026 10:30"
            if " " in fecha_raw:
                partes = fecha_raw.split(" ")
                fecha_display = partes[0]
                hora_raw = partes[1] if len(partes) > 1 else ""
                if hora_raw:
                    h, m = hora_raw.split(":")[:2]
                    hora_int = int(h)
                    ampm = "AM" if hora_int < 12 else "PM"
                    hora_int_12 = hora_int if hora_int <= 12 else hora_int - 12
                    hora_str = f"{hora_int_12:02d}:{m} {ampm}"
            else:
                fecha_display = fecha_raw
        except Exception:
            fecha_display = fecha_raw
    else:
        fecha_display = datetime.now().strftime("%d/%m/%Y")
    draw.text((40, 800), "PRESENTACIÓN DE OFERTAS", font=_font(F_LIGHT, 20), fill=VERDE_CLARO)
    draw.text((40, 830), fecha_display, font=_font(F_BOLD, 58), fill=BLANCO)
    draw.text((40, 900), hora_str, font=_font(F_BOLD, 32), fill=VERDE_ACENTO)

    # — Panel derecho
    draw.rectangle([668, 40, 673, H-40], fill=VERDE_CLARO)
    draw.text((695, 60),  "MONTO ESTIMADO",  font=_font(F_LIGHT, 22), fill=GRIS_LABEL)
    draw.text((695, 95),  "RD$",             font=_font(F_REG, 40),   fill=VERDE_MEDIO)
    draw.text((695, 140), monto,             font=_font(F_BOLD, 42),  fill=VERDE_OSCURO)
    draw.text((695, 196), "Pesos Dominicanos",font=_font(F_LIGHT, 20),fill=GRIS_LABEL)
    draw.rectangle([695, 242, 1040, 244], fill=SEP_COLOR)

    # Tipo de proceso: usar modalidad real, o inferirla del código del proceso
    tipo_proceso_label = ctx.get("modalidad", "")
    if not tipo_proceso_label:
        codigo_proc = ctx.get("codigo", "")
        # Inferir modalidad desde el código del proceso (ej: SENASA-CCC-LPN-2026-0001)
        if "-LPN-" in codigo_proc:
            tipo_proceso_label = "Licitación Pública Nacional"
        elif "-LPC-" in codigo_proc:
            tipo_proceso_label = "Licitación Pública Comparativa"
        elif "-CP-" in codigo_proc or "-CMP-" in codigo_proc:
            tipo_proceso_label = "Comparación de Precios"
        elif "-CD-" in codigo_proc:
            tipo_proceso_label = "Compra Directa"
        elif "-CM-" in codigo_proc:
            tipo_proceso_label = "Contratación Menor"
        elif "-SI-" in codigo_proc:
            tipo_proceso_label = "Sorteo de Obras"
        elif "-SO-" in codigo_proc:
            tipo_proceso_label = "Sorteo de Obras"
        elif "-PEPU-" in codigo_proc or "-PEPB-" in codigo_proc or "-PEEX-" in codigo_proc or "-PEOR-" in codigo_proc:
            tipo_proceso_label = "Procedimiento Especial"
        elif "-LPI-" in codigo_proc:
            tipo_proceso_label = "Licitación Pública Internacional"
        else:
            tipo_proceso_label = "Proceso Público"
    bloques = [
        ("TIPO DE PROCESO", tipo_proceso_label),
        ("SECTOR",          sector),
        (campo3_label,      campo3_valor),
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
            from datetime import timedelta

            # Procesos ya usados hoy en social_log (evitar repetir)
            ya_usados = supabase.table("social_log") \
                .select("codigo_proceso") \
                .gte("created_at", datetime.utcnow().date().isoformat()) \
                .not_.is_("codigo_proceso", "null") \
                .execute()
            codigos_usados = [
                r["codigo_proceso"] for r in (ya_usados.data or [])
                if r.get("codigo_proceso")
            ]

            # Contar posts de licitaciones activas generados hoy para calcular turno 3/1
            conteo_hoy = supabase.table("social_log") \
                .select("id", count="exact") \
                .eq("tipo_contenido", "licitaciones_activas") \
                .gte("created_at", datetime.utcnow().date().isoformat()) \
                .execute()
            total_hoy = conteo_hoy.count or 0

            # Lógica 3/1: cada 4to post (índice 3, 7, 11...) busca sector diferente
            es_turno_diverso = (total_hoy % 4 == 3)

            # Sectores de construcción e infraestructura
            sectores_construccion = ["Obras", "Infraestructura", "Construcción",
                                     "Edificaciones", "Saneamiento", "Vialidad"]

            # Buscar todos los procesos candidatos (limit 40 para tener opciones)
            query = supabase.table("procesos") \
                .select("codigo_proceso, titulo, unidad_compra, monto_estimado, objeto_proceso, provincia, fecha_fin_recepcion_ofertas, modalidad") \
                .eq("estado_proceso", "Proceso publicado") \
                .gte("detectado_en", (datetime.utcnow() - timedelta(days=3)).isoformat()) \
                .gt("fecha_fin_recepcion_ofertas", datetime.utcnow().isoformat()) \
                .not_.is_("monto_estimado", "null") \
                .order("monto_estimado", desc=True) \
                .limit(40) \
                .execute()

            todos = [
                p for p in (query.data or [])
                if p.get("codigo_proceso") not in codigos_usados
            ]

            if es_turno_diverso:
                # Turno diverso (cada 4to): preferir sector NO construcción
                candidatos = [
                    p for p in todos
                    if not any(s.lower() in (p.get("objeto_proceso") or "").lower()
                               for s in sectores_construccion)
                ]
                if not candidatos:
                    candidatos = todos  # fallback si no hay otro sector
            else:
                # Turnos normales (3 de cada 4): preferir construcción/infraestructura
                candidatos_construccion = [
                    p for p in todos
                    if any(s.lower() in (p.get("objeto_proceso") or "").lower()
                           for s in sectores_construccion)
                ]
                candidatos = candidatos_construccion if candidatos_construccion else todos

            if candidatos:
                proceso = candidatos[0]

                # Fecha de presentación de ofertas formateada
                fecha_limite = ""
                if proceso.get("fecha_fin_recepcion_ofertas"):
                    try:
                        fecha_limite = datetime.fromisoformat(
                            str(proceso["fecha_fin_recepcion_ofertas"]).replace("Z", "")
                        ).strftime("%d/%m/%Y")
                    except Exception:
                        fecha_limite = str(proceso["fecha_fin_recepcion_ofertas"])[:10]

                monto_raw = proceso.get("monto_estimado", 0)

                # Extraer también la hora de presentación
                hora_presentacion = ""
                if proceso.get("fecha_fin_recepcion_ofertas"):
                    try:
                        dt_raw = str(proceso["fecha_fin_recepcion_ofertas"]).replace("Z", "")
                        dt_obj = datetime.fromisoformat(dt_raw)
                        hora_presentacion = dt_obj.strftime("%H:%M")
                        fecha_con_hora = f"{fecha_limite} {hora_presentacion}"
                    except Exception:
                        fecha_con_hora = fecha_limite
                else:
                    fecha_con_hora = fecha_limite

                return {
                    "codigo":          proceso.get("codigo_proceso", ""),
                    "entidad":         proceso.get("unidad_compra", "Entidad pública"),
                    "descripcion":     (proceso.get("titulo") or "")[:120],
                    "sector":          proceso.get("objeto_proceso") or "Infraestructura",
                    "monto":           f"RD$ {float(monto_raw):,.0f}" if monto_raw else "—",
                    "monto_raw":       str(monto_raw),
                    "fecha_limite":    fecha_con_hora,
                    "modalidad":       proceso.get("modalidad") or "",
                    "provincia":       proceso.get("provincia") or "Nacional",
                    "_codigo_proceso": proceso.get("codigo_proceso"),
                }

        except Exception as e:
            print(f"[Social] Error obteniendo contexto licitacion: {e}")

        # Fallback solo si no hay procesos recientes disponibles
        return {
            "codigo":          "SIN-PROCESO-HOY",
            "entidad":         "DGCP",
            "descripcion":     "Monitorea las licitaciones activas en tiempo real",
            "sector":          "Infraestructura",
            "monto":           "—",
            "monto_raw":       "0",
            "fecha_limite":    datetime.now().strftime("%d/%m/%Y"),
            "provincia":       "Nacional",
            "_codigo_proceso": None,
        }

    elif tipo == "analisis_semanal":
        try:
            from datetime import timedelta as _timedelta
            result = supabase.table("procesos") \
                .select("id", count="exact") \
                .eq("estado_proceso", "Proceso publicado") \
                .gte("detectado_en", (datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)).isoformat()) \
                .execute()
            total_hoy = result.count or 0

            result_sem = supabase.table("procesos") \
                .select("monto_estimado", count="exact") \
                .eq("estado_proceso", "Proceso publicado") \
                .gte("detectado_en", (datetime.utcnow() - _timedelta(days=7)).isoformat()) \
                .not_.is_("monto_estimado", "null") \
                .execute()
            total_sem = result_sem.count or random.randint(20, 40)
            montos = [float(r["monto_estimado"]) for r in (result_sem.data or []) if r.get("monto_estimado")]
            monto_total = f"{int(sum(montos)/1_000_000)}M" if montos else f"{random.randint(300, 900)}M"
        except Exception as e:
            print(f"[Social] Error analisis_semanal: {e}")
            total_sem = random.randint(20, 40)
            monto_total = f"{random.randint(300, 900)}M"

        return {
            "semana":      datetime.now().strftime("%d/%m/%Y"),
            "total":       total_sem,
            "sector_top":  "Infraestructura y obras civiles",
            "monto_total": monto_total,
            "tendencia":   "+12% vs semana anterior",
            "_codigo_proceso": None,
        }

    else:
        # ── EDUCATIVO: obtener tema real desde Supabase ──────────────────
        try:
            from datetime import timedelta as _td
            hace_7_dias = (datetime.utcnow() - _td(days=7)).isoformat()

            # Buscar temas no usados en los últimos 7 días (primero los nunca usados)
            res = supabase.table("temas_educativos") \
                .select("id, tema, categoria") \
                .eq("activo", True) \
                .or_(f"usado_en.is.null,usado_en.lt.{hace_7_dias}") \
                .order("usado_en", desc=False, nullsfirst=True) \
                .limit(10) \
                .execute()

            if res.data:
                tema_row = random.choice(res.data)
                # Marcar como usado ahora mismo
                supabase.table("temas_educativos") \
                    .update({"usado_en": datetime.utcnow().isoformat()}) \
                    .eq("id", tema_row["id"]) \
                    .execute()
                return {
                    "tema":      tema_row["tema"],
                    "categoria": tema_row.get("categoria", "general"),
                    "_codigo_proceso": None,
                }
        except Exception as e:
            print(f"[Social] Error obteniendo tema educativo de Supabase: {e}")

        # Fallback local si falla Supabase
        temas_fallback = [
            "Qué es el RNCE y por qué necesitas estar registrado",
            "Los 5 documentos que nunca pueden faltar en tu oferta",
            "Diferencia entre LPN, CP y Compra Directa",
            "Cómo leer un pliego de condiciones en 30 minutos",
            "Qué es la Ley 47-25 y cómo te afecta como contratista",
        ]
        return {"tema": random.choice(temas_fallback), "categoria": "general", "_codigo_proceso": None}



def enviar_carrusel_telegram(imagenes_b64: list, post_id: int):
    """
    Envía las imágenes del carrusel (índices 1 en adelante) directamente
    a Telegram desde Railway usando requests síncrono.
    La portada (índice 0) ya la envía n8n con los botones de aprobación.
    """
    import requests as _requests
    import time

    if len(imagenes_b64) <= 1:
        return  # Solo portada, nada extra que enviar

    url   = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendPhoto"
    total = len(imagenes_b64)

    for i, img_b64 in enumerate(imagenes_b64[1:], start=2):
        try:
            img_bytes   = base64.b64decode(img_b64)
            caption_img = f"Imagen {i} de {total} — Post #{post_id}"

            resp = _requests.post(
                url,
                data={"chat_id": TELEGRAM_CHAT_ID, "caption": caption_img},
                files={"photo": (f"slide_{i}.png", img_bytes, "image/png")},
                timeout=30
            )
            if resp.status_code != 200:
                print(f"[Telegram] Error slide {i}: {resp.text}")
            else:
                print(f"[Telegram] Slide {i}/{total} enviado OK")

            time.sleep(0.5)  # pausa entre imágenes

        except Exception as e:
            print(f"[Telegram] Error enviando slide {i}: {e}")


@social_router.post("/generar", response_model=SocialResponse)
async def generar_posts_sociales(
    req: SocialRequest,
    background_tasks: BackgroundTasks,
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

        # Inyectar contexto real para que la imagen muestre los datos del proceso
        titulo_imagen = contexto.get("descripcion", "") if tipo_actual == "licitaciones_activas" and contexto.get("descripcion") else datos_caption.get("titulo_portada") or datos_caption.get("titulo", "")
        datos_caption["_imagen_datos"] = {
            "titulo":       titulo_imagen,
            "entidad":      contexto.get("entidad", ""),
            "codigo":       contexto.get("codigo", ""),
            "monto":        contexto.get("monto", "—"),
            "monto_raw":    contexto.get("monto_raw", "0"),
            "sector":       contexto.get("sector", "Infraestructura"),
            "modalidad":    contexto.get("modalidad", ""),
            "campo3_label": "PROVINCIA",
            "campo3_valor": contexto.get("provincia", "Nacional"),
            "fecha_limite": contexto.get("fecha_limite", ""),
            "semana":       contexto.get("semana", ""),
            "total":        contexto.get("total", ""),
            "sector_top":   contexto.get("sector_top", ""),
            "monto_total":  contexto.get("monto_total", ""),
            "categoria":    contexto.get("categoria", "general"),
        }

        # ── EDUCATIVO: carrusel de imágenes ─────────────────────────────
        if tipo_actual == "educativo":
            imagenes_carrusel = generar_carrusel_educativo(datos_caption)
            # Guardar solo la portada en social_log (imagen_b64 = portada)
            post_data = {
                "tipo_contenido":  tipo_actual,
                "caption":         datos_caption.get("caption"),
                "hashtags":        datos_caption.get("hashtags"),
                "titulo_imagen":   datos_caption.get("titulo_portada") or datos_caption.get("titulo", ""),
                "imagen_b64":      imagenes_carrusel[0],   # portada
                "estado":          "pendiente_aprobacion",
                "plataforma":      "instagram",
                "codigo_proceso":  contexto.get("_codigo_proceso"),
                "created_at":      datetime.utcnow().isoformat()
            }
            result = supabase.table("social_log").insert(post_data).execute()
            post_id = result.data[0]["id"] if result.data else None

            posts_generados.append({
                "id":              post_id,
                "tipo":            tipo_actual,
                "titulo":          datos_caption.get("titulo_portada") or datos_caption.get("titulo", ""),
                "caption":         datos_caption.get("caption"),
                "hashtags":        datos_caption.get("hashtags"),
                "imagen_b64":      imagenes_carrusel[0],      # portada (compatibilidad con n8n)
                "imagenes_b64":    imagenes_carrusel,          # TODAS para referencia
                "es_carrusel":     True,
                "total_imagenes":  len(imagenes_carrusel),
                "estado":          "pendiente_aprobacion"
            })

            # Enviar imágenes restantes (2...N) directamente a Telegram desde Railway
            # La portada (imagen 0) la envía n8n con los botones de aprobación
            background_tasks.add_task(
                enviar_carrusel_telegram, imagenes_carrusel, post_id
            )
            continue  # saltar el bloque normal

        # ── LICITACIONES / ANÁLISIS: imagen única ───────────────────────
        imagen_b64 = generar_imagen_post(tipo_actual, datos_caption)

        post_data = {
            "tipo_contenido":  tipo_actual,
            "caption":         datos_caption.get("caption"),
            "hashtags":        datos_caption.get("hashtags"),
            "titulo_imagen":   datos_caption.get("titulo"),
            "imagen_b64":      imagen_b64,
            "estado":          "pendiente_aprobacion",
            "plataforma":      "instagram",
            "codigo_proceso":  contexto.get("_codigo_proceso"),
            "created_at":      datetime.utcnow().isoformat()
        }

        result = supabase.table("social_log").insert(post_data).execute()
        post_id = result.data[0]["id"] if result.data else None

        posts_generados.append({
            "id":           post_id,
            "tipo":         tipo_actual,
            "titulo":       datos_caption.get("titulo"),
            "caption":      datos_caption.get("caption"),
            "hashtags":     datos_caption.get("hashtags"),
            "imagen_b64":   imagen_b64,
            "es_carrusel":  False,
            "estado":       "pendiente_aprobacion"
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
