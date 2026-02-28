"""
API de Licitaciones RD — Backend FastAPI
=========================================
- Sirve datos de procesos y artículos a la PWA
- Ejecuta el monitor automáticamente cada 10 minutos
- Análisis de pliegos con IA de Gemini en segundo plano
"""

import os
import threading
import time
import urllib3
import json
import io
from PyPDF2 import PdfReader
from bs4 import BeautifulSoup
import requests
from datetime import datetime
from contextlib import asynccontextmanager
from fastapi.staticfiles import StaticFiles

from fastapi import FastAPI, Query, HTTPException, BackgroundTasks, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from dotenv import load_dotenv
from supabase import create_client
import google.generativeai as genai

from monitor import ejecutar_monitor
from notifications import enviar_notificacion

# Silenciamos los warnings del SSL de la DGCP
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# ============================================
# CONFIGURACIÓN DE INTELIGENCIA ARTIFICIAL
# ============================================
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
modelo_gemini = None

if GEMINI_API_KEY:
    genai.configure(api_key=GEMINI_API_KEY)
    modelo_gemini = genai.GenerativeModel(
        model_name="gemini-1.5-flash",
        generation_config={"response_mime_type": "application/json"}
    )
else:
    print("⚠️ ADVERTENCIA: No se encontró GEMINI_API_KEY en las variables de entorno.")


BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# ============================================
# MONITOR AUTOMÁTICO EN SEGUNDO PLANO
# ============================================

def monitor_loop():
    """Ejecuta el monitor de la API DGCP cada 8 horas."""
    INTERVALO_HORAS = 8
    while True:
        try:
            print(f"\n⏰ Ejecutando monitor API DGCP...")
            ejecutar_monitor()
        except Exception as e:
            print(f"❌ Error en monitor API: {e}")
        time.sleep(INTERVALO_HORAS * 3600)


def scraper_loop():
    """Scraper del portal transaccional cada 3 minutos."""
    INTERVALO_MINUTOS = 3
    time.sleep(30)
    while True:
        try:
            from scraper_portal import ejecutar_scraper_portal
            ejecutar_scraper_portal()
        except Exception as e:
            print(f"❌ Error en scraper portal: {e}")
        time.sleep(INTERVALO_MINUTOS * 60)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Inicia los monitores al arrancar."""
    threading.Thread(target=monitor_loop, daemon=True).start()
    threading.Thread(target=scraper_loop, daemon=True).start()
    yield


# ============================================
# CREAR APP FASTAPI
# ============================================

app = FastAPI(
    title="API Licitaciones RD",
    version="1.0.0",
    lifespan=lifespan
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Endpoints base y estáticos
app.mount("/frontend", StaticFiles(directory=os.path.join(BASE_DIR, "frontend"), html=True), name="frontend")

@app.get("/")
def inicio():
    return FileResponse(os.path.join(BASE_DIR, "frontend", "index.html"))

@app.get("/api/stats")
def estadisticas():
    try:
        activos = supabase.table("procesos").select("id", count="exact").eq("estado_proceso", "Proceso publicado").execute()
        return {"total_procesos": activos.count or 0, "ultima_actualizacion": datetime.now().isoformat()}
    except: return {"total_procesos": 0}

@app.get("/api/procesos")
def listar_procesos(page: int = 1, limit: int = 20, solo_activos: bool = True):
    try:
        query = supabase.table("procesos").select("*", count="exact")
        if solo_activos:
            query = query.eq("estado_proceso", "Proceso publicado").gt("fecha_fin_recepcion_ofertas", datetime.now().isoformat())
        offset = (page - 1) * limit
        result = query.order("fecha_publicacion", desc=True).range(offset, offset + limit - 1).execute()
        return {"procesos": result.data, "total": result.count}
    except Exception as e: raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/procesos/{codigo_proceso}")
def detalle_proceso(codigo_proceso: str):
    proc = supabase.table("procesos").select("*").eq("codigo_proceso", codigo_proceso).execute()
    if not proc.data: raise HTTPException(status_code=404, detail="No encontrado")
    return {"proceso": proc.data[0]}


# ═══════════════════════════════════════════════════════════════
# ANÁLISIS DE PLIEGOS CON INTELIGENCIA ARTIFICIAL (GEMINI)
# ═══════════════════════════════════════════════════════════════

PROMPT_MAESTRO = """
Eres un perito experto en licitaciones de República Dominicana (Ley 47-25). 
Analiza el pliego adjunto y extrae los requisitos técnicos, financieros y legales.
Devuelve el resultado ESTRICTAMENTE usando esta estructura JSON:
{
  "alertas_fraude": [{"riesgo": "Alto/Medio", "hallazgo": "..."}],
  "requisitos_experiencia": {"obras_similares": "...", "montos_facturados": "..."},
  "requisitos_financieros": {"indice_liquidez": "...", "indice_endeudamiento": "..."},
  "garantias_exigidas": [{"tipo": "...", "monto_o_porcentaje": "..."}],
  "personal_y_equipos": {"personal_clave": [{"posicion": "...", "titulo": "...", "anos_experiencia": "..."}], "equipos_minimos": ["..."]},
  "checklist_legal": [{"documento": "...", "es_subsanable": true}]
}
"""

def descargar_y_extraer_texto_pdf(url_documentos: str) -> str:
    """Busca el pliego real omitiendo botones falsos de JavaScript"""
    # Limpiamos la URL por si viene con doble barra
    url_limpia = url_documentos.replace("gob.do//", "gob.do/")
    print(f"🕵️‍♂️ Entrando al portal para buscar documentos en: {url_limpia}")
    
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'}
    respuesta = requests.get(url_limpia, headers=headers, verify=False)
    
    if respuesta.status_code != 200:
        raise Exception(f"Error de conexión: {respuesta.status_code}")
        
    soup = BeautifulSoup(respuesta.text, 'html.parser')
    enlace_pdf = None
    
    # Buscamos en las filas de la tabla de documentos
    for fila in soup.find_all('tr'):
        texto_fila = fila.get_text().lower()
        if any(x in texto_fila for x in ["bases de la contratación", "pliego", "condiciones"]):
            # Buscamos enlaces reales (que no sean javascript:void(0))
            enlaces = fila.find_all('a', href=True)
            for a in enlaces:
                href = a['href']
                if "javascript" not in href and href != "#":
                    enlace_pdf = href
                    break
            if enlace_pdf: break
                
    if not enlace_pdf:
        # Intento de rescate: buscar cualquier link que tenga la palabra 'Download' o 'Descargar'
        for a in soup.find_all('a', href=True):
            if "descargar" in a.get_text().lower() or "download" in a['href'].lower():
                if "javascript" not in a['href']:
                    enlace_pdf = a['href']
                    break

    if not enlace_pdf:
        raise Exception("No se encontró un enlace de descarga válido en la página.")
        
    if enlace_pdf.startswith('/'):
        enlace_pdf = "https://comunidad.comprasdominicana.gob.do" + enlace_pdf
        
    print(f"📥 Descargando PDF real desde: {enlace_pdf}")
    resp_pdf = requests.get(enlace_pdf, headers=headers, verify=False)
    
    pdf_file = io.BytesIO(resp_pdf.content)
    lector_pdf = PdfReader(pdf_file)
    
    texto_completo = ""
    for i in range(min(len(lector_pdf.pages), 80)):
        texto_completo += (lector_pdf.pages[i].extract_text() or "") + "\n"
            
    return texto_completo


def ejecutar_analisis_gemini(proceso_id: str):
    """Lógica de IA en segundo plano"""
    try:
        print(f"🚀 Iniciando IA para: {proceso_id}")
        supabase.table("analisis_pliego").upsert({"proceso_id": proceso_id, "estado": "procesando"}).execute()

        # Obtenemos la URL guardada en la columna 'url'
        proc_data = supabase.table("procesos").select("url").eq("codigo_proceso", proceso_id).execute()
        if not proc_data.data or not proc_data.data[0].get("url"):
            raise Exception(f"No se encontró URL para {proceso_id}")
            
        url_portal = proc_data.data[0]["url"]
        texto_pliego = descargar_y_extraer_texto_pdf(url_portal)
        
        if len(texto_pliego.strip()) < 100:
            raise Exception("PDF sin texto suficiente para análisis.")
        
        if not modelo_gemini: raise Exception("IA no configurada.")
            
        print("🤖 Pasando el texto a Gemini...")
        respuesta = modelo_gemini.generate_content([PROMPT_MAESTRO, texto_pliego])
        datos_json = json.loads(respuesta.text)
        
        supabase.table("analisis_pliego").upsert({
            "proceso_id": proceso_id,
            "estado": "completado",
            **datos_json
        }).execute()
        
        print(f"✅ Análisis de {proceso_id} completado.")

    except Exception as e:
        print(f"❌ Error de IA en {proceso_id}: {str(e)}")
        supabase.table("analisis_pliego").upsert({"proceso_id": proceso_id, "estado": "error"}).execute()

@app.post("/api/webhook/analizar-pliego")
async def webhook_analisis_pliego(request: Request, background_tasks: BackgroundTasks):
    payload = await request.json()
    proceso_id = payload.get("record", {}).get("proceso_codigo")
    if proceso_id:
        background_tasks.add_task(ejecutar_analisis_gemini, proceso_id)
        return {"status": "success"}
    return {"status": "error"}
