"""
ETL Ofertas — LicitacionLab
============================
Descarga el registro de ofertas presentadas en procesos de la DGCP.
1,386,180 registros disponibles — carga incremental por fecha.

Endpoint: GET /ofertas
Campos: id_oferta, codigo_proceso, codigo_unidad_compra, unidad_compra,
        rpe, razon_social, nombre_oferta, valor_oferta, estado_oferta,
        estado_evaluacion, tipo_oferta, fecha_creacion, fecha_entrega_oferta,
        fecha_evaluacion

Uso:
    python etl_ofertas.py                    # últimos 30 días
    python etl_ofertas.py --full             # histórico completo
    python etl_ofertas.py --year 2025        # año específico
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

supabase     = create_client(SUPABASE_URL, SUPABASE_KEY)
PAGE_SIZE    = 1000
DELAY        = 0.3


def obtener_ofertas_api(fecha_desde, fecha_hasta, page=1):
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
        print(f"  📥 Ofertas pág {page}: {len(content)} registros")
        return content or [], total
    except Exception as e:
        print(f"  ❌ Error API ofertas pág {page}: {e}")
        return [], 0


def mapear_oferta(o):
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


def cargar_ofertas(ofertas_raw):
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
            print(f"  ❌ Error upsert ofertas lote {i}: {e}")
    return insertadas


def run_etl_ofertas(fecha_desde, fecha_hasta, modo="incremental"):
    t0 = time.time()
    print(f"\n{'='*55}")
    print(f"📊 ETL Ofertas | {fecha_desde} → {fecha_hasta} | {modo}")
    print(f"{'='*55}")

    total_ext = 0
    total_ins = 0

    if modo == "full":
        ini = datetime.strptime(fecha_desde, "%Y-%m-%d")
        fin = datetime.strptime(fecha_hasta, "%Y-%m-%d")
        cur = ini
        while cur < fin:
            mes_hasta = min(cur + timedelta(days=30), fin)
            d = cur.strftime("%Y-%m-%d")
            h = mes_hasta.strftime("%Y-%m-%d")
            print(f"\n📅 {d} → {h}")
            page = 1
            while True:
                ofertas, _ = obtener_ofertas_api(d, h, page)
                if not ofertas:
                    break
                total_ext += len(ofertas)
                total_ins += cargar_ofertas(ofertas)
                if len(ofertas) < PAGE_SIZE:
                    break
                page += 1
                time.sleep(DELAY)
            cur = mes_hasta + timedelta(days=1)
            time.sleep(1)
    else:
        page = 1
        while True:
            ofertas, _ = obtener_ofertas_api(fecha_desde, fecha_hasta, page)
            if not ofertas:
                break
            total_ext += len(ofertas)
            total_ins += cargar_ofertas(ofertas)
            if len(ofertas) < PAGE_SIZE:
                break
            page += 1
            time.sleep(DELAY)

    duracion = round(time.time() - t0)
    print(f"\n✅ ETL Ofertas completado en {duracion}s")
    print(f"   Extraídas:  {total_ext:,}")
    print(f"   Insertadas: {total_ins:,}")

    try:
        supabase.table("cron_log").insert({
            "job":         f"etl_ofertas_{modo}",
            "status":      "ok",
            "detalle":     {"fecha_desde": fecha_desde, "fecha_hasta": fecha_hasta,
                            "total_extraidos": total_ext, "total_insertados": total_ins},
            "duracion_ms": duracion * 1000,
        }).execute()
    except Exception:
        pass

    return total_ins


def get_ofertas_router():
    """Router FastAPI para disparar desde admin."""
    import threading
    from fastapi import APIRouter, Depends, Header, HTTPException, Query
    from fastapi.responses import JSONResponse

    ADMIN_SECRET = os.getenv("ADMIN_SECRET", "")

    def verificar_admin(x_admin_key: str = Header(None)):
        if not ADMIN_SECRET or x_admin_key != ADMIN_SECRET:
            raise HTTPException(status_code=403, detail="Acceso denegado")

    router  = APIRouter(prefix="/api/admin", tags=["Admin"])
    _estado = {"corriendo": False, "stats": None, "iniciado_en": None}

    @router.post("/etl-ofertas", dependencies=[Depends(verificar_admin)])
    def disparar_etl_ofertas(
        modo:      str = Query("incremental", description="incremental | full | year"),
        year:      int = Query(None),
        desde:     str = Query(None),
        hasta:     str = Query(None),
    ):
        if _estado["corriendo"]:
            return JSONResponse(status_code=409, content={"error": "Ya hay un proceso corriendo"})

        if modo == "full":
            fd, fh = "2024-01-01", datetime.now().strftime("%Y-%m-%d")
        elif modo == "year" and year:
            fd, fh = f"{year}-01-01", f"{year}-12-31"
        elif desde and hasta:
            fd, fh = desde, hasta
        else:
            fd = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
            fh = datetime.now().strftime("%Y-%m-%d")

        def _run():
            _estado["corriendo"]   = True
            _estado["iniciado_en"] = datetime.now().isoformat()
            try:
                n = run_etl_ofertas(fd, fh, modo=modo)
                _estado["stats"] = {"insertadas": n, "desde": fd, "hasta": fh}
            except Exception as e:
                _estado["stats"] = {"error": str(e)}
            finally:
                _estado["corriendo"] = False

        threading.Thread(target=_run, daemon=True, name="etl_ofertas").start()
        return {"mensaje": "ETL Ofertas iniciado en background", "desde": fd, "hasta": fh, "modo": modo}

    @router.get("/etl-ofertas/status", dependencies=[Depends(verificar_admin)])
    def status_etl_ofertas():
        return {"corriendo": _estado["corriendo"], "stats": _estado["stats"]}

    return router


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--full",  action="store_true")
    parser.add_argument("--year",  type=int)
    parser.add_argument("--desde", type=str)
    parser.add_argument("--hasta", type=str)
    args = parser.parse_args()

    if args.full:
        run_etl_ofertas("2024-01-01", datetime.now().strftime("%Y-%m-%d"), "full")
    elif args.year:
        run_etl_ofertas(f"{args.year}-01-01", f"{args.year}-12-31", "full")
    elif args.desde and args.hasta:
        run_etl_ofertas(args.desde, args.hasta, "custom")
    else:
        desde = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
        hasta = datetime.now().strftime("%Y-%m-%d")
        run_etl_ofertas(desde, hasta, "incremental")
