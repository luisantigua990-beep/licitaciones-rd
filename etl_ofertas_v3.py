"""
ETL Ofertas v3 — LicitacionLab
================================
La API DGCP /ofertas IGNORA los filtros startdate/enddate.
Siempre retorna los 1.38M registros ordenados por fecha_creacion DESC.

NUEVA ESTRATEGIA: paginar por número de página (chunks de 50 páginas = ~50K registros).
Cada chunk dura ~60-90s — seguro para Railway sin timeout.

Endpoints:
  POST /api/admin/etl-ofertas/chunk     → procesa el siguiente bloque de 50 páginas
  GET  /api/admin/etl-ofertas/status    → progreso actual
  POST /api/admin/etl-ofertas/reset     → reinicia desde página 1

Uso CLI:
    python etl_ofertas_v3.py --status               # ver progreso
    python etl_ofertas_v3.py --chunk                # procesar siguiente bloque
    python etl_ofertas_v3.py --full                 # loop completo (Railway lo puede matar)
    python etl_ofertas_v3.py --reset                # reiniciar progreso
"""

import os
import time
import argparse
from datetime import datetime
from dotenv import load_dotenv
import requests
from supabase import create_client

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_KEY", os.getenv("SUPABASE_KEY"))
API_BASE_URL = "https://datosabiertos.dgcp.gob.do/api-dgcp/v1"

supabase  = create_client(SUPABASE_URL, SUPABASE_KEY)

PAGE_SIZE       = 1000    # registros por request a la API
PAGES_PER_CHUNK = 50      # páginas por chunk (~50K registros, ~60-90s)
DELAY           = 0.3     # segundos entre requests para no saturar la API
CONFIG_KEY      = "etl_ofertas_progreso_v3"


# ─────────────────────────────────────────────────────────────
# API DGCP
# ─────────────────────────────────────────────────────────────

def obtener_ofertas_pagina(page: int):
    """Descarga una página de la API. Retorna (registros, total_api)."""
    try:
        r = requests.get(
            f"{API_BASE_URL}/ofertas",
            params={"page": page, "limit": PAGE_SIZE},
            timeout=30,
        )
        r.raise_for_status()
        data = r.json()
        if data.get("hasError"):
            print(f"  ⚠️  API reportó hasError en pág {page}")
            return [], 0
        payload = data.get("payload", {})
        content = payload.get("content", []) if isinstance(payload, dict) else payload
        total   = data.get("totalResults", 0)
        print(f"  📥 pág {page:>5}: {len(content):>4} registros (total API: {total:,})")
        return content or [], total
    except Exception as e:
        print(f"  ❌ Error API pág {page}: {e}")
        return [], 0


# ─────────────────────────────────────────────────────────────
# SUPABASE
# ─────────────────────────────────────────────────────────────

def mapear_oferta(o: dict) -> dict:
    return {
        "id_oferta":            o.get("id_oferta"),
        "codigo_proceso":       o.get("codigo_proceso"),
        "codigo_unidad_compra": o.get("codigo_unidad_compra"),
        "unidad_compra":        o.get("unidad_compra"),
        "rpe":                  str(o.get("rpe") or ""),
        "razon_social":         (o.get("razon_social") or "").strip(),
        "nombre_oferta":        o.get("nombre_oferta"),
        "valor_oferta":         o.get("valor_oferta"),
        "estado_oferta":        o.get("estado_oferta"),
        "estado_evaluacion":    o.get("estado_evaluacion"),
        "tipo_oferta":          o.get("tipo_oferta"),
        "fecha_creacion":       o.get("fecha_creacion"),
        "fecha_entrega_oferta": o.get("fecha_entrega_oferta"),
        "fecha_evaluacion":     o.get("fecha_evaluacion"),
    }


def cargar_ofertas(ofertas_raw: list) -> int:
    if not ofertas_raw:
        return 0
    filas = [mapear_oferta(o) for o in ofertas_raw if o.get("id_oferta")]
    if not filas:
        return 0
    insertadas = 0
    for i in range(0, len(filas), 250):
        lote = filas[i:i+250]
        try:
            r = supabase.table("ofertas_procesos") \
                .upsert(lote, on_conflict="id_oferta") \
                .execute()
            insertadas += len(r.data)
        except Exception as e:
            print(f"  ❌ Error upsert lote {i}: {e}")
    return insertadas


# ─────────────────────────────────────────────────────────────
# PROGRESO en system_config
# ─────────────────────────────────────────────────────────────

def _leer_progreso() -> dict:
    try:
        r = supabase.table("system_config") \
            .select("meta") \
            .eq("clave", CONFIG_KEY) \
            .limit(1) \
            .execute()
        if r.data:
            return r.data[0]["meta"] or {}
    except Exception:
        pass
    return {}


def _guardar_progreso(progreso: dict):
    try:
        supabase.table("system_config") \
            .upsert(
                {"clave": CONFIG_KEY, "valor": "etl_ofertas_v3", "meta": progreso},
                on_conflict="clave"
            ).execute()
    except Exception as e:
        print(f"⚠️  No se pudo guardar progreso: {e}")


def _contar_bd() -> int:
    try:
        r = supabase.table("ofertas_procesos").select("id_oferta", count="exact").execute()
        return r.count or 0
    except Exception:
        return 0


# ─────────────────────────────────────────────────────────────
# LÓGICA PRINCIPAL
# ─────────────────────────────────────────────────────────────

def obtener_status_completo() -> dict:
    progreso       = _leer_progreso()
    ultima_pag     = progreso.get("ultima_pagina_procesada", 0)
    total_api      = progreso.get("total_api", 1389066)  # conocido del diagnóstico
    total_paginas  = -(-total_api // PAGE_SIZE)           # ceil division
    pct            = round(ultima_pag / max(total_paginas, 1) * 100, 1)
    completado     = ultima_pag >= total_paginas

    return {
        "ultima_pagina_procesada": ultima_pag,
        "total_paginas_estimado":  total_paginas,
        "porcentaje":              pct,
        "total_registros_bd":      _contar_bd(),
        "total_insertadas":        progreso.get("total_insertadas", 0),
        "completado":              completado,
        "ultima_ejecucion":        progreso.get("ultima_ejecucion"),
    }


def run_siguiente_chunk() -> dict:
    """
    Procesa PAGES_PER_CHUNK páginas consecutivas de la API.
    Seguro para Railway: dura ~60-90s por llamada.
    """
    progreso   = _leer_progreso()
    desde_pag  = progreso.get("ultima_pagina_procesada", 0) + 1
    hasta_pag  = desde_pag + PAGES_PER_CHUNK - 1

    print(f"\n{'='*55}")
    print(f"📦 ETL Ofertas v3 — Chunk páginas {desde_pag}–{hasta_pag}")
    print(f"{'='*55}\n")

    t0             = time.time()
    total_ext      = 0
    total_ins      = 0
    ultima_pag_ok  = desde_pag - 1
    total_api      = progreso.get("total_api", 0)

    for page in range(desde_pag, hasta_pag + 1):
        ofertas, total_retornado = obtener_ofertas_pagina(page)

        # Guardar total_api la primera vez que lo obtenemos
        if total_retornado and not total_api:
            total_api = total_retornado

        if not ofertas:
            print(f"  ⏹️  Sin datos en pág {page} — fin del dataset")
            break

        total_ext     += len(ofertas)
        total_ins     += cargar_ofertas(ofertas)
        ultima_pag_ok  = page

        # Si la página vino con menos registros que PAGE_SIZE, es la última
        if len(ofertas) < PAGE_SIZE:
            print(f"  ⏹️  Última página del dataset alcanzada (pág {page})")
            break

        time.sleep(DELAY)

    duracion = round(time.time() - t0)

    # Actualizar progreso
    progreso["ultima_pagina_procesada"] = ultima_pag_ok
    progreso["total_api"]               = total_api or progreso.get("total_api", 1389066)
    progreso["total_insertadas"]        = progreso.get("total_insertadas", 0) + total_ins
    progreso["ultima_ejecucion"]        = datetime.now().isoformat()
    _guardar_progreso(progreso)

    # Log cron
    try:
        supabase.table("cron_log").insert({
            "job":         f"etl_ofertas_v3_chunk_{desde_pag}_{ultima_pag_ok}",
            "status":      "ok",
            "detalle":     {"desde": desde_pag, "hasta": ultima_pag_ok, "extraidas": total_ext, "insertadas": total_ins},
            "duracion_ms": duracion * 1000,
        }).execute()
    except Exception:
        pass

    status = obtener_status_completo()
    print(f"\n  ✅ Chunk listo: {total_ext:,} extraídas, {total_ins:,} insertadas ({duracion}s)")
    print(f"  📊 Progreso: {status['porcentaje']}% | {status['total_registros_bd']:,} registros en BD\n")

    return {
        "status":          "ok",
        "paginas":         f"{desde_pag}–{ultima_pag_ok}",
        "extraidas":       total_ext,
        "insertadas":      total_ins,
        "duracion_s":      duracion,
        "progreso":        status,
        "siguiente_chunk": not status["completado"],
    }


def run_reset_progreso() -> dict:
    try:
        supabase.table("system_config").delete().eq("clave", CONFIG_KEY).execute()
        return {"status": "ok", "mensaje": "Progreso v3 reiniciado. Próximo chunk empieza desde página 1"}
    except Exception as e:
        return {"status": "error", "error": str(e)}


# ─────────────────────────────────────────────────────────────
# ROUTER FASTAPI
# ─────────────────────────────────────────────────────────────

def get_ofertas_router():
    from fastapi import APIRouter, Depends, Header, HTTPException
    from fastapi.responses import JSONResponse

    ADMIN_SECRET = os.getenv("ADMIN_SECRET", "")

    def verificar_admin(x_admin_key: str = Header(None)):
        if not ADMIN_SECRET or x_admin_key != ADMIN_SECRET:
            raise HTTPException(status_code=403, detail="Acceso denegado")

    router  = APIRouter(prefix="/api/admin", tags=["Admin ETL Ofertas"])
    _estado = {"corriendo": False}

    @router.post("/etl-ofertas/chunk", dependencies=[Depends(verificar_admin)])
    def procesar_chunk():
        """
        Procesa el siguiente bloque de 50 páginas (~50K registros, ~60-90s).
        Llamar repetidamente desde consola del browser:

          async function correrTodo() {
            let r;
            do {
              r = await fetch('/api/admin/etl-ofertas/chunk', {
                method: 'POST',
                headers: {'X-Admin-Key': 'TU_SECRET'}
              }).then(x => x.json());
              console.log('✅', r.paginas, '|', r.progreso?.porcentaje + '%',
                          '|', r.progreso?.total_registros_bd?.toLocaleString(), 'registros');
            } while (r.siguiente_chunk);
            console.log('🎉 ETL Ofertas v3 completo!');
          }
          correrTodo();
        """
        if _estado["corriendo"]:
            return JSONResponse(status_code=409, content={"error": "Ya hay un chunk corriendo, espera"})
        _estado["corriendo"] = True
        try:
            return run_siguiente_chunk()
        except Exception as e:
            return JSONResponse(status_code=500, content={"error": str(e)})
        finally:
            _estado["corriendo"] = False

    @router.get("/etl-ofertas/status", dependencies=[Depends(verificar_admin)])
    def status_etl():
        return {"corriendo": _estado["corriendo"], **obtener_status_completo()}

    @router.post("/etl-ofertas/reset", dependencies=[Depends(verificar_admin)])
    def reset_etl():
        return run_reset_progreso()

    return router


# ─────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="ETL Ofertas v3 — paginación directa")
    parser.add_argument("--full",   action="store_true", help="Loop completo hasta terminar")
    parser.add_argument("--chunk",  action="store_true", help="Procesar siguiente bloque de 50 páginas")
    parser.add_argument("--reset",  action="store_true", help="Reiniciar progreso")
    parser.add_argument("--status", action="store_true", help="Ver estado actual")
    args = parser.parse_args()

    if args.reset:
        print(run_reset_progreso())

    elif args.status:
        import json
        print(json.dumps(obtener_status_completo(), indent=2))

    elif args.chunk:
        import json
        resultado = run_siguiente_chunk()
        print(json.dumps(resultado, indent=2))

    elif args.full:
        print("🚀 Modo full — procesando chunk a chunk hasta completar...")
        total_ins = 0
        while True:
            resultado = run_siguiente_chunk()
            total_ins += resultado.get("insertadas", 0)
            pct = resultado.get("progreso", {}).get("porcentaje", 0)
            print(f"  📊 Progreso global: {pct}% | Total en BD: {resultado['progreso']['total_registros_bd']:,}")
            if not resultado.get("siguiente_chunk"):
                print(f"\n🎉 ETL Ofertas v3 completo! Total en BD: {resultado['progreso']['total_registros_bd']:,}")
                break
            time.sleep(2)

    else:
        import json
        print("Uso: --status | --chunk | --full | --reset")
        print("\nEstado actual:")
        print(json.dumps(obtener_status_completo(), indent=2))
