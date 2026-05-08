"""
ETL Ofertas — LicitacionLab
============================
Descarga el registro de ofertas presentadas en procesos de la DGCP.
1,386,180 registros disponibles — carga por chunks de 1 mes.

ESTRATEGIA ANTI-RAILWAY-TIMEOUT:
  En lugar de un thread daemon que corre horas y Railway mata,
  cada llamada procesa UN MES completo (~5,000–15,000 registros, ~60s).
  El frontend o un cron externo llama /etl-ofertas/chunk repetidamente
  hasta que todos los meses estén completos.

Endpoints:
  POST /api/admin/etl-ofertas/chunk          → procesa el siguiente mes pendiente
  GET  /api/admin/etl-ofertas/status         → estado general del progreso
  POST /api/admin/etl-ofertas/reset          → reinicia progreso (para re-procesar todo)

Uso CLI:
    python etl_ofertas.py                    # últimos 30 días
    python etl_ofertas.py --full             # histórico completo (loop automático)
    python etl_ofertas.py --mes 2025-03      # mes específico
"""

import os
import time
import argparse
import requests
from datetime import datetime, timedelta
from dotenv import load_dotenv
from supabase import create_client

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_KEY", os.getenv("SUPABASE_KEY"))
API_BASE_URL = "https://datosabiertos.dgcp.gob.do/api-dgcp/v1"

supabase  = create_client(SUPABASE_URL, SUPABASE_KEY)
PAGE_SIZE = 1000
DELAY     = 0.3

# Rango histórico completo disponible en la API DGCP
FECHA_INICIO_HISTORICO = "2024-01-01"


# ─────────────────────────────────────────────────────────────
# HELPERS API DGCP
# ─────────────────────────────────────────────────────────────

def obtener_ofertas_api(fecha_desde: str, fecha_hasta: str, page: int = 1):
    try:
        r = requests.get(
            f"{API_BASE_URL}/ofertas",
            params={
                "startdate": fecha_desde,
                "enddate":   fecha_hasta,
                "page":      page,
                "limit":     PAGE_SIZE,
            },
            timeout=30,
        )
        r.raise_for_status()
        data = r.json()
        if data.get("hasError"):
            return [], 0
        payload = data.get("payload", {})
        content = payload.get("content", []) if isinstance(payload, dict) else payload
        total   = data.get("totalResults", 0)
        print(f"  📥 pág {page}: {len(content)} registros (total API: {total:,})")
        return content or [], total
    except Exception as e:
        print(f"  ❌ Error API ofertas pág {page}: {e}")
        return [], 0


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
        try:
            r = supabase.table("ofertas_procesos") \
                .upsert(filas[i:i+250], on_conflict="id_oferta") \
                .execute()
            insertadas += len(r.data)
        except Exception as e:
            print(f"  ❌ Error upsert lote {i}: {e}")
    return insertadas


# ─────────────────────────────────────────────────────────────
# PROCESAMIENTO DE UN MES COMPLETO
# ─────────────────────────────────────────────────────────────

def _sumar_mes(año: int, mes: int) -> tuple:
    """Retorna (año, mes) del mes siguiente."""
    if mes == 12:
        return (año + 1, 1)
    return (año, mes + 1)


def _primer_dia(año: int, mes: int) -> datetime:
    return datetime(año, mes, 1)


def _ultimo_dia(año: int, mes: int) -> datetime:
    año_sig, mes_sig = _sumar_mes(año, mes)
    return datetime(año_sig, mes_sig, 1) - timedelta(days=1)


def procesar_mes(año: int, mes: int) -> dict:
    """
    Descarga e inserta todas las ofertas de un mes calendario.
    Retorna stats del mes procesado.
    """
    desde = _primer_dia(año, mes)
    hasta = _ultimo_dia(año, mes)
    fd    = desde.strftime("%Y-%m-%d")
    fh    = min(hasta, datetime.now()).strftime("%Y-%m-%d")

    print(f"\n📅 Procesando {año}-{mes:02d} ({fd} → {fh})")
    t0 = time.time()

    total_ext = 0
    total_ins = 0
    page      = 1

    while True:
        ofertas, _ = obtener_ofertas_api(fd, fh, page)
        if not ofertas:
            break
        total_ext += len(ofertas)
        total_ins += cargar_ofertas(ofertas)
        if len(ofertas) < PAGE_SIZE:
            break
        page += 1
        time.sleep(DELAY)

    duracion = round(time.time() - t0)
    print(f"  ✅ {año}-{mes:02d}: {total_ext:,} extraídas, {total_ins:,} insertadas ({duracion}s)")

    return {
        "mes":        f"{año}-{mes:02d}",
        "fecha_desde": fd,
        "fecha_hasta": fh,
        "extraidas":  total_ext,
        "insertadas": total_ins,
        "duracion_s": duracion,
    }


# ─────────────────────────────────────────────────────────────
# GESTIÓN DE PROGRESO EN SUPABASE (system_config)
# ─────────────────────────────────────────────────────────────

CONFIG_KEY = "etl_ofertas_progreso"


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
            .upsert({"clave": CONFIG_KEY, "valor": "etl_ofertas", "meta": progreso}, on_conflict="clave") \
            .execute()
    except Exception as e:
        print(f"⚠️  No se pudo guardar progreso: {e}")


def _generar_meses(desde: str, hasta: str) -> list:
    """Lista de (año, mes) desde fecha_desde hasta fecha_hasta inclusive."""
    ini = datetime.strptime(desde, "%Y-%m-%d")
    fin = datetime.strptime(hasta, "%Y-%m-%d")
    meses = []
    cur_año, cur_mes = ini.year, ini.month
    while datetime(cur_año, cur_mes, 1) <= fin:
        meses.append((cur_año, cur_mes))
        cur_año, cur_mes = _sumar_mes(cur_año, cur_mes)
    return meses


def obtener_siguiente_mes_pendiente() -> tuple | None:
    """
    Retorna (año, mes) del siguiente mes sin procesar.
    None si ya están todos completos.
    """
    progreso = _leer_progreso()
    meses_completados = set(progreso.get("meses_completados", []))
    hasta = datetime.now().strftime("%Y-%m-%d")
    todos = _generar_meses(FECHA_INICIO_HISTORICO, hasta)
    for año, mes in todos:
        clave = f"{año}-{mes:02d}"
        if clave not in meses_completados:
            return (año, mes)
    return None


def obtener_status_completo() -> dict:
    progreso = _leer_progreso()
    meses_completados = progreso.get("meses_completados", [])
    hasta = datetime.now().strftime("%Y-%m-%d")
    todos = _generar_meses(FECHA_INICIO_HISTORICO, hasta)
    total_meses    = len(todos)
    completados    = len(meses_completados)
    pendientes     = total_meses - completados
    pct            = round(completados / max(total_meses, 1) * 100, 1)

    # Contar registros en tabla
    try:
        r = supabase.table("ofertas_procesos").select("id_oferta", count="exact").execute()
        total_registros = r.count or 0
    except Exception:
        total_registros = 0

    return {
        "total_meses":       total_meses,
        "meses_completados": completados,
        "meses_pendientes":  pendientes,
        "porcentaje":        pct,
        "total_registros_bd": total_registros,
        "completado":        pendientes == 0,
        "ultimo_mes":        progreso.get("ultimo_mes"),
        "total_insertadas":  progreso.get("total_insertadas", 0),
    }


# ─────────────────────────────────────────────────────────────
# FUNCIÓN PRINCIPAL: PROCESAR SIGUIENTE CHUNK
# ─────────────────────────────────────────────────────────────

def run_siguiente_chunk() -> dict:
    """
    Procesa el siguiente mes pendiente y actualiza el progreso.
    Diseñado para llamarse repetidamente desde el endpoint admin.
    Cada llamada dura ~30-90 segundos — nunca genera timeout en Railway.
    """
    siguiente = obtener_siguiente_mes_pendiente()

    if siguiente is None:
        return {
            "status":   "completado",
            "mensaje":  "Todos los meses ya fueron procesados",
            **obtener_status_completo(),
        }

    año, mes = siguiente
    stats = procesar_mes(año, mes)

    # Actualizar progreso
    progreso = _leer_progreso()
    meses_completados = progreso.get("meses_completados", [])
    clave = f"{año}-{mes:02d}"
    if clave not in meses_completados:
        meses_completados.append(clave)

    progreso["meses_completados"] = meses_completados
    progreso["ultimo_mes"]        = clave
    progreso["total_insertadas"]  = progreso.get("total_insertadas", 0) + stats["insertadas"]
    progreso["ultima_ejecucion"]  = datetime.now().isoformat()
    _guardar_progreso(progreso)

    # Log en cron_log
    try:
        supabase.table("cron_log").insert({
            "job":         f"etl_ofertas_chunk_{clave}",
            "status":      "ok",
            "detalle":     stats,
            "duracion_ms": stats["duracion_s"] * 1000,
        }).execute()
    except Exception:
        pass

    status = obtener_status_completo()
    return {
        "status":          "ok",
        "mes_procesado":   clave,
        "extraidas":       stats["extraidas"],
        "insertadas":      stats["insertadas"],
        "duracion_s":      stats["duracion_s"],
        "progreso":        status,
        "siguiente_chunk": not status["completado"],
    }


def run_reset_progreso() -> dict:
    """Borra el progreso guardado para reprocesar todo desde cero."""
    try:
        supabase.table("system_config") \
            .delete() \
            .eq("clave", CONFIG_KEY) \
            .execute()
        return {"status": "ok", "mensaje": "Progreso reiniciado. Próximo chunk empezará desde 2024-01-01"}
    except Exception as e:
        return {"status": "error", "error": str(e)}


# ─────────────────────────────────────────────────────────────
# ROUTER FASTAPI
# ─────────────────────────────────────────────────────────────

def get_ofertas_router():
    """Router FastAPI para disparar desde admin."""
    import threading
    from fastapi import APIRouter, Depends, Header, HTTPException, Query
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
        Procesa el siguiente mes pendiente (~30-90s por llamada).
        Llama repetidamente hasta que 'siguiente_chunk' sea false.

        Ejemplo de uso desde consola del browser:
          async function correrTodo() {
            let r;
            do {
              r = await fetch('/api/admin/etl-ofertas/chunk', {
                method: 'POST',
                headers: {'X-Admin-Key': 'TU_SECRET'}
              }).then(x => x.json());
              console.log(r.mes_procesado, r.progreso.porcentaje + '%');
            } while (r.siguiente_chunk);
            console.log('✅ ETL Ofertas completado!');
          }
          correrTodo();
        """
        if _estado["corriendo"]:
            return JSONResponse(status_code=409, content={"error": "Ya hay un chunk corriendo"})

        _estado["corriendo"] = True
        try:
            resultado = run_siguiente_chunk()
            return resultado
        except Exception as e:
            return JSONResponse(status_code=500, content={"error": str(e)})
        finally:
            _estado["corriendo"] = False

    @router.get("/etl-ofertas/status", dependencies=[Depends(verificar_admin)])
    def status_etl_ofertas():
        """Consulta el progreso general del ETL de ofertas."""
        return {
            "corriendo": _estado["corriendo"],
            **obtener_status_completo(),
        }

    @router.post("/etl-ofertas/reset", dependencies=[Depends(verificar_admin)])
    def reset_etl_ofertas():
        """Reinicia el progreso para reprocesar todo desde cero."""
        return run_reset_progreso()

    # Mantener compatibilidad con el endpoint antiguo /etl-ofertas
    @router.post("/etl-ofertas", dependencies=[Depends(verificar_admin)])
    def disparar_etl_ofertas_legacy(
        modo: str = Query("incremental"),
    ):
        """
        Endpoint legacy — ahora usa la estrategia de chunks.
        Para modo 'full' usa /etl-ofertas/chunk repetidamente.
        """
        if modo == "full":
            return JSONResponse(content={
                "mensaje": "Modo full ahora usa chunks. Llama POST /api/admin/etl-ofertas/chunk repetidamente.",
                "instrucciones": "Ver docstring del endpoint /chunk para el loop de consola.",
                **obtener_status_completo(),
            })
        # Para incremental: procesar el mes actual
        hoy = datetime.now()
        stats = procesar_mes(hoy.year, hoy.month)
        return {"mensaje": "Mes actual procesado", **stats}

    return router


# ─────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="ETL Ofertas — LicitacionLab")
    parser.add_argument("--full",  action="store_true", help="Procesar histórico completo (loop)")
    parser.add_argument("--mes",   type=str,            help="Mes específico: 2025-03")
    parser.add_argument("--chunk", action="store_true", help="Procesar solo el siguiente mes pendiente")
    parser.add_argument("--reset", action="store_true", help="Reiniciar progreso")
    parser.add_argument("--status",action="store_true", help="Ver estado actual")
    args = parser.parse_args()

    if args.reset:
        print(run_reset_progreso())

    elif args.status:
        import json
        print(json.dumps(obtener_status_completo(), indent=2))

    elif args.mes:
        año, mes = map(int, args.mes.split("-"))
        stats = procesar_mes(año, mes)
        print(f"\n✅ {stats}")

    elif args.chunk:
        resultado = run_siguiente_chunk()
        import json
        print(json.dumps(resultado, indent=2))

    elif args.full:
        print("🚀 Modo full — procesando mes a mes hasta completar...")
        total_ins = 0
        while True:
            resultado = run_siguiente_chunk()
            total_ins += resultado.get("insertadas", 0)
            pct = resultado.get("progreso", {}).get("porcentaje", 0)
            print(f"  📊 Progreso global: {pct}% | Total insertadas: {total_ins:,}")
            if not resultado.get("siguiente_chunk"):
                print(f"\n✅ ETL Ofertas completo! Total insertadas: {total_ins:,}")
                break
            time.sleep(2)  # pausa entre meses

    else:
        # Default: mes actual
        hoy = datetime.now()
        stats = procesar_mes(hoy.year, hoy.month)
        print(f"\n✅ {stats}")
