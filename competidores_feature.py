"""
Router: Inteligencia Competitiva — LicitacionLab
=================================================
Endpoints para búsqueda de empresas y dashboard de historial
de contratos con el Estado dominicano.

Prefijo: /api/v1/proveedores
Integrar en main.py:
    from competidores_feature import competidores_router
    app.include_router(competidores_router)
"""

import os
from uuid import UUID
from typing import Optional
from datetime import datetime

from fastapi import APIRouter, Query, HTTPException, Depends, Header
from supabase import create_client

# ── Cliente Supabase ──────────────────────────────────────────
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_KEY", os.getenv("SUPABASE_KEY"))
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# ── Admin Key (misma que el resto de la app) ──────────────────
ADMIN_SECRET = os.getenv("ADMIN_SECRET", "")

def verificar_admin(x_admin_key: str = Header(None)):
    if not ADMIN_SECRET:
        raise HTTPException(status_code=500, detail="ADMIN_SECRET no configurado")
    if x_admin_key != ADMIN_SECRET:
        raise HTTPException(status_code=403, detail="Acceso denegado")

# ── Router ────────────────────────────────────────────────────
competidores_router = APIRouter(prefix="/api/v1/proveedores", tags=["Inteligencia Competitiva"])


# ─────────────────────────────────────────────────────────────
# GET /api/v1/proveedores/buscar?q=constructora&limit=10
# ─────────────────────────────────────────────────────────────

@competidores_router.get("/buscar")
def buscar_proveedores(
    q: str = Query(..., min_length=2, description="Nombre o RNC de la empresa"),
    limit: int = Query(10, ge=1, le=50),
):
    """
    Búsqueda tolerante a errores de ortografía usando pg_trgm + FTS.
    Acepta nombre parcial o RNC exacto.
    Devuelve lista con KPIs básicos para mostrar en resultados de búsqueda.
    """
    q_clean = q.strip()

    # Si parece RNC (solo dígitos), buscar por RNC exacto primero
    if q_clean.replace("-", "").isdigit():
        rnc_limpio = q_clean.replace("-", "")
        try:
            r = supabase.table("empresas_estado") \
                .select("id, nombre, rnc, total_contratos, monto_total, "
                        "correo_comercial, correo_contacto, provincia, estado_rpe") \
                .eq("rnc", rnc_limpio) \
                .limit(1) \
                .execute()
            if r.data:
                return {"resultados": r.data, "total": len(r.data), "modo": "rnc_exacto"}
        except Exception:
            pass  # fallback a búsqueda por nombre

    # Búsqueda por nombre usando RPC (pg_trgm + FTS)
    try:
        r = supabase.rpc("buscar_empresas_estado", {
            "p_query": q_clean.lower(),
            "p_limit": limit,
        }).execute()

        resultados = r.data or []
        return {
            "resultados": resultados,
            "total": len(resultados),
            "modo": "fuzzy",
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error en búsqueda: {str(e)}")


# ─────────────────────────────────────────────────────────────
# GET /api/v1/proveedores/{empresa_id}/dashboard
# ─────────────────────────────────────────────────────────────

@competidores_router.get("/{empresa_id}/dashboard")
def dashboard_empresa(empresa_id: str):
    """
    Dashboard completo de una empresa:
    - Perfil y datos de contacto
    - KPIs: total contratos, monto total, primer y último contrato
    - Contratos por año (bar chart)
    - Top instituciones compradoras
    - Últimos 10 contratos
    """
    # ── 1. Perfil de la empresa ───────────────────────────────
    try:
        r_emp = supabase.table("empresas_estado") \
            .select("*") \
            .eq("id", empresa_id) \
            .single() \
            .execute()
    except Exception:
        raise HTTPException(status_code=404, detail="Empresa no encontrada")

    if not r_emp.data:
        raise HTTPException(status_code=404, detail="Empresa no encontrada")

    empresa = r_emp.data

    # ── 2. Evolución por año ──────────────────────────────────
    try:
        r_anios = supabase.rpc("get_montos_por_anio", {
            "p_empresa_id": empresa_id,
            "p_ultimos_n": 5,
        }).execute()
        por_anio = r_anios.data or []
    except Exception:
        por_anio = []

    # ── 3. Top instituciones ──────────────────────────────────
    try:
        r_inst = supabase.rpc("get_top_instituciones", {
            "p_empresa_id": empresa_id,
            "p_top_n": 5,
        }).execute()
        top_instituciones = r_inst.data or []
    except Exception:
        top_instituciones = []

    # ── 4. Últimos 10 contratos ───────────────────────────────
    try:
        r_contratos = supabase.table("contratos_adjudicados") \
            .select(
                "ocid, titulo_proceso, modalidad, monto_adjudicado, "
                "divisa, fecha_adjudicacion, fecha_contrato, "
                "instituciones_compradoras(nombre)"
            ) \
            .eq("empresa_id", empresa_id) \
            .order("fecha_adjudicacion", desc=True) \
            .limit(10) \
            .execute()
        ultimos_contratos = r_contratos.data or []
    except Exception:
        ultimos_contratos = []

    # ── 5. Rubros / tipos de obra más frecuentes ─────────────
    try:
        r_rubros = supabase.table("contratos_adjudicados") \
            .select("objeto_proceso") \
            .eq("empresa_id", empresa_id) \
            .not_.is_("objeto_proceso", "null") \
            .execute()

        # Contar frecuencia de rubros
        from collections import Counter
        rubros_raw = [c["objeto_proceso"] for c in (r_rubros.data or []) if c.get("objeto_proceso")]
        rubros_contados = Counter(rubros_raw).most_common(5)
        top_rubros = [{"rubro": k, "cantidad": v} for k, v in rubros_contados]
    except Exception:
        top_rubros = []

    return {
        "empresa": empresa,
        "kpis": {
            "total_contratos": empresa.get("total_contratos", 0),
            "monto_total": float(empresa.get("monto_total") or 0),
            "primer_contrato": str(empresa.get("primer_contrato") or ""),
            "ultimo_contrato": str(empresa.get("ultimo_contrato") or ""),
            "tiene_email": bool(empresa.get("correo_comercial") or empresa.get("correo_contacto")),
        },
        "por_anio": [
            {
                "anio": r["anio"],
                "cantidad_contratos": r["cantidad_contratos"],
                "monto_total": float(r["monto_total"] or 0),
            }
            for r in por_anio
        ],
        "top_instituciones": [
            {
                "nombre": r["nombre"],
                "cantidad_contratos": r["cantidad_contratos"],
                "monto_total": float(r["monto_total"] or 0),
            }
            for r in top_instituciones
        ],
        "top_rubros": top_rubros,
        "ultimos_contratos": [
            {
                "ocid": c.get("ocid"),
                "titulo": c.get("titulo_proceso"),
                "institucion": (c.get("instituciones_compradoras") or {}).get("nombre", ""),
                "monto": float(c.get("monto_adjudicado") or 0),
                "divisa": c.get("divisa", "DOP"),
                "fecha": str(c.get("fecha_adjudicacion") or ""),
            }
            for c in ultimos_contratos
        ],
    }


# ─────────────────────────────────────────────────────────────
# GET /api/v1/proveedores/prospectos  [ADMIN]
# Empresas ideales para prospección de LicitacionLab
# ─────────────────────────────────────────────────────────────

@competidores_router.get("/prospectos", dependencies=[Depends(verificar_admin)])
def get_prospectos_licitacionlab(
    min_contratos: int = Query(3,  description="Mínimo de contratos ganados"),
    max_contratos: int = Query(30, description="Máximo de contratos — más que esto ya son grandes"),
    min_monto:     int = Query(1_000_000,   description="Monto mínimo total RD$"),
    max_monto:     int = Query(500_000_000, description="Monto máximo total RD$"),
    solo_con_email: bool = Query(True, description="Solo empresas con email disponible"),
    limit: int = Query(50, ge=1, le=200),
):
    """
    Detecta prospectos ideales para LicitacionLab:
    empresas activas que ganan contratos medianos con frecuencia
    pero que aún no usan inteligencia para licitaciones.

    Rango ideal: 3–30 contratos, RD$1M–RD$500M total.
    Estas empresas licitan activamente pero necesitan la plataforma.
    """
    try:
        query = supabase.table("empresas_estado") \
            .select(
                "id, nombre, rnc, total_contratos, monto_total, "
                "correo_comercial, correo_contacto, telefono_comercial, "
                "nombre_contacto, posicion_contacto, provincia, ultimo_contrato"
            ) \
            .gte("total_contratos", min_contratos) \
            .lte("total_contratos", max_contratos) \
            .gte("monto_total", min_monto) \
            .lte("monto_total", max_monto) \
            .order("total_contratos", desc=True) \
            .limit(limit)

        if solo_con_email:
            query = query.not_.is_("correo_comercial", "null")

        r = query.execute()

        prospectos = r.data or []

        return {
            "total": len(prospectos),
            "filtros": {
                "min_contratos": min_contratos,
                "max_contratos": max_contratos,
                "min_monto_rd": min_monto,
                "max_monto_rd": max_monto,
                "solo_con_email": solo_con_email,
            },
            "prospectos": prospectos,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error obteniendo prospectos: {str(e)}")


# ─────────────────────────────────────────────────────────────
# GET /api/v1/proveedores/inteligencia-precios  [ADMIN]
# ¿Cuánto ofertar en un tipo de obra?
# ─────────────────────────────────────────────────────────────

@competidores_router.get("/inteligencia-precios", dependencies=[Depends(verificar_admin)])
def inteligencia_precios(
    rubro:         str = Query(..., description="Tipo de obra o rubro (ej: acueducto, carretera)"),
    institucion:   Optional[str] = Query(None, description="Filtrar por institución (ej: INAPA)"),
    ultimos_anios: int = Query(2, description="Cuántos años hacia atrás analizar"),
):
    """
    Inteligencia de precios de mercado:
    dado un rubro, devuelve estadísticas de montos adjudicados
    para ayudar a calibrar ofertas.
    """
    try:
        anio_desde = datetime.now().year - ultimos_anios

        # Buscar contratos del rubro usando ILIKE
        query = supabase.table("contratos_adjudicados") \
            .select(
                "monto_adjudicado, fecha_adjudicacion, anio, "
                "titulo_proceso, instituciones_compradoras(nombre)"
            ) \
            .ilike("titulo_proceso", f"%{rubro}%") \
            .gte("anio", anio_desde) \
            .not_.is_("monto_adjudicado", "null") \
            .gt("monto_adjudicado", 0) \
            .order("monto_adjudicado", desc=False) \
            .limit(500)

        r = query.execute()
        contratos = r.data or []

        if not contratos:
            return {
                "rubro": rubro,
                "mensaje": "No se encontraron contratos para ese rubro en el período indicado.",
                "estadisticas": None,
                "contratos": [],
            }

        montos = [float(c["monto_adjudicado"]) for c in contratos if c.get("monto_adjudicado")]
        montos_sorted = sorted(montos)
        n = len(montos_sorted)

        def percentil(p):
            idx = int(n * p / 100)
            return montos_sorted[min(idx, n - 1)]

        estadisticas = {
            "total_contratos_analizados": n,
            "monto_minimo":   montos_sorted[0],
            "monto_maximo":   montos_sorted[-1],
            "monto_promedio": sum(montos) / n,
            "monto_mediana":  percentil(50),
            "percentil_25":   percentil(25),
            "percentil_75":   percentil(75),
            "recomendacion":  f"Para ser competitivo, oferta entre RD${percentil(25):,.0f} y RD${percentil(75):,.0f}",
        }

        # Muestra de contratos relevantes (top 10 más recientes)
        muestra = sorted(contratos, key=lambda c: c.get("fecha_adjudicacion") or "", reverse=True)[:10]

        return {
            "rubro": rubro,
            "anios_analizados": ultimos_anios,
            "estadisticas": estadisticas,
            "muestra_contratos": [
                {
                    "titulo": c.get("titulo_proceso", "")[:80],
                    "institucion": (c.get("instituciones_compradoras") or {}).get("nombre", ""),
                    "monto": float(c.get("monto_adjudicado") or 0),
                    "fecha": str(c.get("fecha_adjudicacion") or ""),
                }
                for c in muestra
            ],
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error calculando inteligencia de precios: {str(e)}")
