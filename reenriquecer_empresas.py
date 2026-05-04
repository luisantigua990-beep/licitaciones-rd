"""
Re-enriquecimiento de empresas_estado con datos del RPE
========================================================
Recorre todas las empresas con RNC que aún no tienen datos
de contacto válidos y consulta la API DGCP /proveedores
con validación estricta del RNC.

Uso directo (desde Railway shell o local):
    python reenriquecer_empresas.py
    python reenriquecer_empresas.py --batch 500   # solo 500 empresas
    python reenriquecer_empresas.py --dry-run     # simular sin guardar

También se puede disparar vía endpoint admin:
    POST /api/admin/reenriquecer-empresas
    Header: X-Admin-Key: <ADMIN_SECRET>
"""

import os
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

# ── Parámetros de rate limiting ────────────────────────────
DELAY_ENTRE_CONSULTAS = 0.4   # segundos entre llamadas a la API DGCP
BATCH_SIZE            = 200   # cuántas empresas traer de Supabase por vez
DELAY_ENTRE_BATCHES   = 2.0   # pausa entre batches para no saturar Supabase


def obtener_datos_rpe_validado(rnc: str) -> dict:
    """
    Consulta /proveedores con el RNC y valida que el resultado
    corresponda exactamente a ese RNC antes de retornar los datos.
    
    La API DGCP devuelve el primer resultado del índice cuando no
    encuentra el RNC — por eso la validación es crítica.
    """
    if not rnc:
        return {}

    rnc_str = str(rnc).strip()

    try:
        r = requests.get(
            f"{API_BASE_URL}/proveedores",
            params={"rnc": rnc_str, "limit": 1},
            timeout=15,
        )
        r.raise_for_status()
        data = r.json()

        if data.get("hasError"):
            return {}

        payload = data.get("payload", {})
        content = payload.get("content", payload) if isinstance(payload, dict) else payload

        if not content or not isinstance(content, list) or not content[0]:
            return {}

        p = content[0]

        # ✅ VALIDACIÓN: el RNC del resultado debe coincidir con el consultado
        rnc_resultado = str(p.get("rnc") or p.get("rpe") or "").strip()
        if rnc_resultado and rnc_resultado != rnc_str:
            return {}  # La API devolvió otro proveedor — descartar

        # Verificar que tenga al menos un dato de contacto útil
        email = p.get("correo_comercial") or p.get("correo_contacto")
        tel   = p.get("telefono_comercial") or p.get("telefono_contacto")
        if not email and not tel and not p.get("contacto"):
            return {}  # Sin datos útiles — marcar como intentado pero vacío

        return {
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

    except requests.exceptions.Timeout:
        return {"_timeout": True}
    except Exception as e:
        print(f"  ⚠️  Error RPE({rnc}): {e}")
        return {}


def run_reenriquecimiento(
    max_empresas: int = None,
    dry_run: bool = False,
    offset_inicial: int = 0,
) -> dict:
    """
    Recorre todas las empresas pendientes y actualiza sus datos de contacto.
    
    Args:
        max_empresas: Límite de empresas a procesar (None = todas)
        dry_run: Si True, consulta la API pero no guarda en Supabase
        offset_inicial: Continuar desde este offset (para resumir)
    
    Returns:
        Dict con estadísticas del proceso
    """
    t0 = time.time()
    stats = {
        "procesadas":    0,
        "enriquecidas":  0,   # tenían datos válidos en el RPE
        "sin_datos":     0,   # RNC no encontrado o sin contacto
        "rnc_no_match":  0,   # API devolvió otro proveedor
        "errores":       0,
        "omitidas":      0,   # ya tenían datos
    }

    print(f"\n{'='*55}")
    print(f"🔄 Re-enriquecimiento RPE — {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"   dry_run={dry_run} | max={max_empresas or 'todas'} | offset={offset_inicial}")
    print(f"{'='*55}\n")

    offset = offset_inicial
    total_procesadas = 0

    while True:
        # Traer batch de empresas pendientes
        try:
            query = supabase.table("empresas_estado") \
                .select("id, rnc, nombre, enriquecido_rpe") \
                .eq("enriquecido_rpe", False) \
                .not_.is_("rnc", "null") \
                .order("id") \
                .range(offset, offset + BATCH_SIZE - 1) \
                .execute()
        except Exception as e:
            print(f"❌ Error al traer batch desde Supabase: {e}")
            break

        empresas = query.data or []
        if not empresas:
            print("✅ Sin más empresas pendientes.")
            break

        print(f"📦 Batch offset={offset} — {len(empresas)} empresas")

        for emp in empresas:
            if max_empresas and total_procesadas >= max_empresas:
                print(f"🛑 Límite de {max_empresas} alcanzado.")
                break

            rnc    = emp.get("rnc", "")
            nombre = emp.get("nombre", "")
            emp_id = emp.get("id")

            datos = obtener_datos_rpe_validado(rnc)

            if datos.get("_timeout"):
                stats["errores"] += 1
                time.sleep(1.0)  # pausa extra en timeout
                continue

            if not datos:
                # Sin datos válidos — marcar como intentado para no reintentar
                stats["sin_datos"] += 1
                if not dry_run:
                    try:
                        supabase.table("empresas_estado") \
                            .update({"enriquecido_rpe": True}) \
                            .eq("id", emp_id) \
                            .execute()
                    except Exception:
                        pass
            else:
                stats["enriquecidas"] += 1
                if not dry_run:
                    try:
                        supabase.table("empresas_estado") \
                            .update(datos) \
                            .eq("id", emp_id) \
                            .execute()
                    except Exception as e:
                        print(f"  ❌ Error actualizando {nombre}: {e}")
                        stats["errores"] += 1

            stats["procesadas"] += 1
            total_procesadas    += 1

            # Log cada 100
            if total_procesadas % 100 == 0:
                pct = round(stats["enriquecidas"] / max(stats["procesadas"], 1) * 100, 1)
                elapsed = round(time.time() - t0)
                print(
                    f"   [{total_procesadas}] "
                    f"enriquecidas={stats['enriquecidas']} "
                    f"({pct}%) | "
                    f"sin_datos={stats['sin_datos']} | "
                    f"tiempo={elapsed}s"
                )

            time.sleep(DELAY_ENTRE_CONSULTAS)

        if max_empresas and total_procesadas >= max_empresas:
            break

        offset += BATCH_SIZE
        time.sleep(DELAY_ENTRE_BATCHES)

    duracion = round(time.time() - t0)
    stats["duracion_segundos"] = duracion

    print(f"\n{'='*55}")
    print(f"✅ COMPLETADO en {duracion}s ({duracion//60}min {duracion%60}s)")
    print(f"   Procesadas:   {stats['procesadas']}")
    print(f"   Enriquecidas: {stats['enriquecidas']} ({round(stats['enriquecidas']/max(stats['procesadas'],1)*100,1)}%)")
    print(f"   Sin datos:    {stats['sin_datos']}")
    print(f"   Errores:      {stats['errores']}")
    print(f"{'='*55}\n")

    # Registrar en cron_log
    try:
        supabase.table("cron_log").insert({
            "job":         "reenriquecer_empresas",
            "status":      "ok",
            "detalle":     stats,
            "duracion_ms": duracion * 1000,
        }).execute()
    except Exception:
        pass

    return stats


# ─────────────────────────────────────────────────────────────
# ENDPOINT FASTAPI — para disparar desde el admin
# ─────────────────────────────────────────────────────────────

def get_reenriquecimiento_router():
    """
    Retorna un APIRouter con el endpoint admin.
    Se importa en main.py:
        from reenriquecer_empresas import get_reenriquecimiento_router
        app.include_router(get_reenriquecimiento_router())
    """
    import threading
    from fastapi import APIRouter, Depends, Header, HTTPException, Query
    from fastapi.responses import JSONResponse

    ADMIN_SECRET = os.getenv("ADMIN_SECRET", "")

    def verificar_admin(x_admin_key: str = Header(None)):
        if not ADMIN_SECRET or x_admin_key != ADMIN_SECRET:
            raise HTTPException(status_code=403, detail="Acceso denegado")

    router = APIRouter(prefix="/api/admin", tags=["Admin"])

    # Estado del job en memoria
    _estado = {"corriendo": False, "stats": None, "iniciado_en": None}

    @router.post("/reenriquecer-empresas", dependencies=[Depends(verificar_admin)])
    def disparar_reenriquecimiento(
        max_empresas: int = Query(None, description="Límite de empresas (None = todas)"),
        dry_run:      bool = Query(False, description="Simular sin guardar"),
    ):
        """
        Dispara el re-enriquecimiento en background.
        Solo corre un proceso a la vez.
        """
        if _estado["corriendo"]:
            return JSONResponse(
                status_code=409,
                content={
                    "error": "Ya hay un proceso corriendo",
                    "iniciado_en": _estado["iniciado_en"],
                }
            )

        def _run():
            _estado["corriendo"]    = True
            _estado["iniciado_en"]  = datetime.now().isoformat()
            _estado["stats"]        = None
            try:
                stats = run_reenriquecimiento(
                    max_empresas=max_empresas,
                    dry_run=dry_run,
                )
                _estado["stats"] = stats
            except Exception as e:
                _estado["stats"] = {"error": str(e)}
            finally:
                _estado["corriendo"] = False

        hilo = threading.Thread(target=_run, daemon=True, name="reenriquecimiento")
        hilo.start()

        return {
            "mensaje":     "Re-enriquecimiento iniciado en background",
            "dry_run":     dry_run,
            "max_empresas": max_empresas or "todas",
            "pendientes":  14935,
            "estimado_min": round(14935 * 0.4 / 60),
        }

    @router.get("/reenriquecer-empresas/status", dependencies=[Depends(verificar_admin)])
    def status_reenriquecimiento():
        """Consulta el estado del proceso de re-enriquecimiento."""
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
    parser = argparse.ArgumentParser(description="Re-enriquecimiento RPE — LicitacionLab")
    parser.add_argument("--batch",   type=int,  help="Máximo de empresas a procesar")
    parser.add_argument("--dry-run", action="store_true", help="Simular sin guardar")
    parser.add_argument("--offset",  type=int,  default=0, help="Continuar desde offset")
    args = parser.parse_args()

    run_reenriquecimiento(
        max_empresas=args.batch,
        dry_run=args.dry_run,
        offset_inicial=args.offset,
    )
