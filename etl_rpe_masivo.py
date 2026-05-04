"""
ETL RPE Masivo — LicitacionLab
================================
Descarga todo el Registro de Proveedores del Estado (RPE) de la DGCP
y hace un JOIN con empresas_estado por numero_documento = rnc.

La API /proveedores devuelve 125,241 registros paginados.
No acepta filtros — hay que descargar todo y cruzar localmente.

Estrategia:
  1. Descarga todas las páginas del RPE en memoria (125 páginas x 1000)
  2. Construye un dict {numero_documento: datos_contacto}
  3. Trae empresas_estado en batches
  4. Hace el match por RNC y actualiza en Supabase

Tiempo estimado: ~15 minutos total.

Uso:
    python etl_rpe_masivo.py
    python etl_rpe_masivo.py --dry-run   # sin guardar
    python etl_rpe_masivo.py --desde-pagina 50  # resumir desde página 50
"""

import os
import sys
import time
import argparse
import requests
from datetime import datetime
from dotenv import load_dotenv
from supabase import create_client

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_KEY", os.getenv("SUPABASE_KEY"))
API_BASE_URL = "https://datosabiertos.dgcp.gob.do/api-dgcp/v1"

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

PAGE_SIZE         = 1000
DELAY_ENTRE_PAGS  = 0.3   # segundos entre páginas RPE
BATCH_SUPABASE    = 500   # empresas por batch al actualizar


# ─────────────────────────────────────────────────────────────
# PASO 1: Descargar todo el RPE
# ─────────────────────────────────────────────────────────────

def descargar_rpe_completo(desde_pagina: int = 1) -> dict:
    """
    Descarga todas las páginas del RPE y retorna un dict:
    { rpe_secuencial: {contacto...} }

    CLAVE: El campo de cruce es `rpe` (número secuencial interno del DGCP),
    NO el numero_documento (RNC de la DGII).
    empresas_estado.rnc almacena ese número secuencial, no el RNC real.
    """
    rpe_index = {}
    page = desde_pagina
    total_descargados = 0
    paginas_saltadas  = []
    MAX_INTENTOS      = 3

    print(f"\n📥 Descargando RPE completo desde página {desde_pagina}...")

    while True:
        intentos = 0
        exito    = False

        while intentos < MAX_INTENTOS:
            try:
                r = requests.get(
                    f"{API_BASE_URL}/proveedores",
                    params={"page": page, "limit": PAGE_SIZE},
                    timeout=30,
                )
                r.raise_for_status()
                data = r.json()

                if data.get("hasError"):
                    print(f"⚠️  hasError en página {page}")
                    exito = True
                    break

                payload = data.get("payload", {})
                content = payload.get("content", []) if isinstance(payload, dict) else payload

                if not content:
                    print(f"✅ Sin más registros en página {page} — descarga completa.")
                    if paginas_saltadas:
                        print(f"⚠️  Páginas saltadas (error DGCP): {paginas_saltadas}")
                    return rpe_index

                # ✅ Indexar por rpe secuencial
                for p in content:
                    rpe_val = str(p.get("rpe") or "").strip()
                    if not rpe_val or rpe_val == "0":
                        continue
                    rpe_index[rpe_val] = {
                        "correo_comercial":   p.get("correo_comercial"),
                        "correo_contacto":    p.get("correo_contacto"),
                        "telefono_comercial": p.get("telefono_comercial"),
                        "telefono_contacto":  p.get("telefono_contacto"),
                        "nombre_contacto":    p.get("contacto"),
                        "posicion_contacto":  p.get("posicion_contacto"),
                        "provincia":          p.get("provincia"),
                        "direccion":          p.get("direccion"),
                        "estado_rpe":         p.get("estado"),
                        "enriquecido_rpe":    True,
                    }

                total_descargados += len(content)
                total_paginas = data.get("pages", "?")

                if page % 10 == 0 or page == 1:
                    print(f"   Página {page}/{total_paginas} — {total_descargados:,} descargados — {len(rpe_index):,} indexados")

                if len(content) < PAGE_SIZE:
                    print(f"✅ Última página ({page}) — descarga completa.")
                    if paginas_saltadas:
                        print(f"⚠️  Páginas saltadas (error DGCP): {paginas_saltadas}")
                    return rpe_index

                exito = True
                break

            except requests.exceptions.RequestException as e:
                intentos += 1
                if intentos < MAX_INTENTOS:
                    print(f"❌ Error página {page} (intento {intentos}/{MAX_INTENTOS}): {e} — reintentando en 5s...")
                    time.sleep(5)
                else:
                    print(f"⏭️  Página {page} saltada tras {MAX_INTENTOS} intentos — error DGCP, continuando...")
                    paginas_saltadas.append(page)
                    exito = True

        if exito:
            page += 1
            time.sleep(DELAY_ENTRE_PAGS)

    print(f"\n📊 RPE descargado: {total_descargados:,} registros totales")
    print(f"   Indexados: {len(rpe_index):,}")
    if paginas_saltadas:
        print(f"   Páginas saltadas: {paginas_saltadas}")
    return rpe_index


# ─────────────────────────────────────────────────────────────
# PASO 2: Cruzar con empresas_estado y actualizar
# ─────────────────────────────────────────────────────────────

def normalizar_rnc(rnc: str) -> str:
    """Normaliza un RNC para comparación."""
    if not rnc:
        return ""
    return str(rnc).strip().replace("-", "").lstrip("0") or str(rnc).strip()


def cruzar_y_actualizar(rpe_index: dict, dry_run: bool = False) -> dict:
    """
    Trae todas las empresas_estado, cruza con el índice del RPE
    y actualiza las que tienen match.
    """
    stats = {
        "total_empresas":  0,
        "con_match":       0,
        "sin_match":       0,
        "actualizadas":    0,
        "errores":         0,
    }

    print(f"\n🔗 Cruzando empresas_estado con RPE...")
    offset = 0

    while True:
        try:
            r = supabase.table("empresas_estado") \
                .select("id, rnc, nombre") \
                .not_.is_("rnc", "null") \
                .order("id") \
                .range(offset, offset + BATCH_SUPABASE - 1) \
                .execute()
        except Exception as e:
            print(f"❌ Error trayendo batch offset={offset}: {e}")
            break

        empresas = r.data or []
        if not empresas:
            break

        updates_con_match    = []
        updates_sin_match    = []

        for emp in empresas:
            # rnc en empresas_estado = número rpe secuencial del DGCP
            rpe_key = str(emp.get("rnc") or "").strip()
            if not rpe_key:
                continue

            stats["total_empresas"] += 1

            if rpe_key in rpe_index:
                stats["con_match"] += 1
                updates_con_match.append({
                    "id": emp["id"],
                    **rpe_index[rpe_key],
                })
            else:
                stats["sin_match"] += 1
                updates_sin_match.append(emp["id"])

        # Actualizar empresas con match en lotes de 100
        if not dry_run:
            for i in range(0, len(updates_con_match), 100):
                lote = updates_con_match[i:i+100]
                try:
                    supabase.table("empresas_estado") \
                        .upsert(lote, on_conflict="id") \
                        .execute()
                    stats["actualizadas"] += len(lote)
                except Exception as e:
                    print(f"❌ Error upsert lote: {e}")
                    stats["errores"] += len(lote)

            # Marcar sin match como intentados (enriquecido_rpe=True pero sin datos)
            for i in range(0, len(updates_sin_match), 100):
                ids_lote = updates_sin_match[i:i+100]
                try:
                    supabase.table("empresas_estado") \
                        .update({"enriquecido_rpe": True}) \
                        .in_("id", ids_lote) \
                        .execute()
                except Exception:
                    pass
        else:
            stats["actualizadas"] += len(updates_con_match)

        if offset % 2000 == 0:
            pct = round(stats["con_match"] / max(stats["total_empresas"], 1) * 100, 1)
            print(f"   [{stats['total_empresas']:,}] match={stats['con_match']:,} ({pct}%) | sin_match={stats['sin_match']:,}")

        offset += BATCH_SUPABASE

    return stats


# ─────────────────────────────────────────────────────────────
# RUNNER PRINCIPAL
# ─────────────────────────────────────────────────────────────

def run_etl_rpe(dry_run: bool = False, desde_pagina: int = 1):
    t0 = time.time()
    print(f"\n{'='*55}")
    print(f"🔄 ETL RPE Masivo — {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"   dry_run={dry_run} | desde_pagina={desde_pagina}")
    print(f"{'='*55}")

    # Paso 1: Descargar RPE completo
    rpe_index = descargar_rpe_completo(desde_pagina=desde_pagina)

    if not rpe_index:
        print("❌ No se descargó ningún registro del RPE.")
        return

    # Paso 2: Cruzar y actualizar
    stats = cruzar_y_actualizar(rpe_index, dry_run=dry_run)

    duracion = round(time.time() - t0)
    stats["duracion_segundos"] = duracion

    print(f"\n{'='*55}")
    print(f"✅ ETL RPE completado en {duracion}s ({duracion//60}min {duracion%60}s)")
    print(f"   Empresas procesadas: {stats['total_empresas']:,}")
    print(f"   Con match RPE:       {stats['con_match']:,} ({round(stats['con_match']/max(stats['total_empresas'],1)*100,1)}%)")
    print(f"   Sin match:           {stats['sin_match']:,}")
    print(f"   Actualizadas en BD:  {stats['actualizadas']:,}")
    print(f"   Errores:             {stats['errores']:,}")
    print(f"{'='*55}\n")

    # Registrar en cron_log
    try:
        supabase.table("cron_log").insert({
            "job":         "etl_rpe_masivo",
            "status":      "ok",
            "detalle":     stats,
            "duracion_ms": duracion * 1000,
        }).execute()
    except Exception:
        pass

    return stats


# ─────────────────────────────────────────────────────────────
# ENDPOINT FASTAPI
# ─────────────────────────────────────────────────────────────

def get_rpe_masivo_router():
    """
    Retorna el router FastAPI para disparar desde admin.
    Agregar en main.py:
        from etl_rpe_masivo import get_rpe_masivo_router
        app.include_router(get_rpe_masivo_router())
    """
    import threading
    from fastapi import APIRouter, Depends, Header, HTTPException, Query
    from fastapi.responses import JSONResponse

    ADMIN_SECRET = os.getenv("ADMIN_SECRET", "")

    def verificar_admin(x_admin_key: str = Header(None)):
        if not ADMIN_SECRET or x_admin_key != ADMIN_SECRET:
            raise HTTPException(status_code=403, detail="Acceso denegado")

    router = APIRouter(prefix="/api/admin", tags=["Admin"])
    _estado = {"corriendo": False, "stats": None, "iniciado_en": None}

    @router.post("/rpe-masivo", dependencies=[Depends(verificar_admin)])
    def disparar_rpe_masivo(
        dry_run:       bool = Query(False),
        desde_pagina:  int  = Query(1),
    ):
        if _estado["corriendo"]:
            return JSONResponse(status_code=409, content={
                "error": "Ya hay un proceso corriendo",
                "iniciado_en": _estado["iniciado_en"],
            })

        def _run():
            _estado["corriendo"]   = True
            _estado["iniciado_en"] = datetime.now().isoformat()
            _estado["stats"]       = None
            try:
                stats = run_etl_rpe(dry_run=dry_run, desde_pagina=desde_pagina)
                _estado["stats"] = stats
            except Exception as e:
                _estado["stats"] = {"error": str(e)}
            finally:
                _estado["corriendo"] = False

        threading.Thread(target=_run, daemon=True, name="etl_rpe_masivo").start()

        return {
            "mensaje":       "ETL RPE masivo iniciado en background",
            "dry_run":       dry_run,
            "desde_pagina":  desde_pagina,
            "total_rpe":     125241,
            "estimado_min":  15,
        }

    @router.get("/rpe-masivo/status", dependencies=[Depends(verificar_admin)])
    def status_rpe_masivo():
        return {
            "corriendo":   _estado["corriendo"],
            "iniciado_en": _estado["iniciado_en"],
            "stats":       _estado["stats"],
        }

    return router


# ─────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="ETL RPE Masivo — LicitacionLab")
    parser.add_argument("--dry-run",       action="store_true", help="Simular sin guardar")
    parser.add_argument("--desde-pagina",  type=int, default=1, help="Empezar desde esta página del RPE")
    args = parser.parse_args()

    run_etl_rpe(dry_run=args.dry_run, desde_pagina=args.desde_pagina)
