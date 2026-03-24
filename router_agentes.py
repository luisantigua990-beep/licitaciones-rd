"""
router_agentes.py
Router FastAPI para los agentes de growth marketing.
Agregar a main.py con: app.include_router(agentes_router)

Endpoints:
  POST /api/agentes/prospector/run   — ejecuta scraping + scoring
  GET  /api/agentes/prospector/cola  — lista prospectos pendientes de enriquecer
"""

import os
import asyncio
import httpx
from fastapi import APIRouter, BackgroundTasks, Header, HTTPException
from pydantic import BaseModel
from typing import Optional
from supabase import create_client
from scraper_rnce import run_prospector

# ── Config ───────────────────────────────────────────
SUPABASE_URL  = os.environ["SUPABASE_URL"]
SUPABASE_KEY  = os.environ["SUPABASE_KEY"]
CLAUDE_KEY    = os.environ["ANTHROPIC_API_KEY"]
AGENT_SECRET  = os.environ.get("AGENT_SECRET", "cambiar-este-secreto")  # n8n lo envía en header

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
agentes_router = APIRouter(prefix="/api/agentes", tags=["agentes"])


# ── Modelos ───────────────────────────────────────────
class ProspectorConfig(BaseModel):
    max_rnce:  int = 80
    max_maps:  int = 40
    score_min: int = 60   # solo guarda en cola si score >= este valor


# ── Scoring con Claude ────────────────────────────────
async def score_empresa(empresa: dict) -> dict:
    """
    Llama a Claude para puntuar la empresa como prospecto de LicitacionLab.
    Retorna el dict original enriquecido con score, justificacion, prioridad.
    """
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
                    "model": "claude-haiku-4-5-20251001",  # haiku para scoring masivo — más rápido y barato
                    "max_tokens": 150,
                    "messages": [{"role": "user", "content": prompt}]
                }
            )
            data = resp.json()
            texto = data["content"][0]["text"].strip()

            # Parsear JSON de la respuesta
            import json, re
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
    """
    Guarda en Supabase los prospectos con score >= score_min.
    Hace upsert por RNC (o nombre si no hay RNC) para evitar duplicados.
    """
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
            "estado":        "pendiente",  # → Agente 2 lo cambiará a 'en_cola'
        }

        try:
            # Upsert: si ya existe el RNC, actualiza score. Si no, inserta.
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


# ── Tarea en background (no bloquea el request de n8n) ──
async def ejecutar_prospector_bg(config: ProspectorConfig):
    print(f"[Prospector] Iniciando — max_rnce={config.max_rnce}, max_maps={config.max_maps}")

    # 1. Scraping
    empresas = await run_prospector(max_rnce=config.max_rnce, max_maps=config.max_maps)

    # 2. Scoring con Claude (en lotes para no saturar la API)
    LOTE = 10
    empresas_scored = []
    for i in range(0, len(empresas), LOTE):
        lote = empresas[i:i+LOTE]
        tareas = [score_empresa(e) for e in lote]
        resultado = await asyncio.gather(*tareas)
        empresas_scored.extend(resultado)
        await asyncio.sleep(1)  # rate limiting

    # 3. Guardar en Supabase
    resumen = await guardar_prospectos(empresas_scored, config.score_min)

    # 4. Notificar a Telegram (opcional — usa tu bot existente)
    print(f"[Prospector] Completado: {resumen}")
    # TODO: llamar a tu bot de Telegram aquí con el resumen


# ── Endpoints ──────────────────────────────────────────
@agentes_router.post("/prospector/run")
async def run_prospector_endpoint(
    config: ProspectorConfig,
    background_tasks: BackgroundTasks,
    x_agent_secret: Optional[str] = Header(None)
):
    """
    n8n llama a este endpoint con el header X-Agent-Secret.
    Retorna inmediatamente y ejecuta el scraping en background.
    """
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
    """
    Retorna prospectos con score >= score_min en estado 'pendiente'.
    El Agente Enriquecedor (n8n) llama esto para saber qué procesar.
    """
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
