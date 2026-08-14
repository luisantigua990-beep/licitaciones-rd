"""
router_bid_manager.py — Bid Manager para LicitacionLab
=======================================================
CRUD completo para el Expediente Digital de Empresa:
- Perfil empresa (campos extendidos)
- Representantes legales
- Socios / accionistas
- Certificaciones y documentos de empresa
- Personal técnico + certificaciones del personal
- Experiencia (banco de proyectos) + personal por proyecto
- Equipos
- Estados financieros
- Capacidad financiera (líneas de crédito, solvencias)
- Referencias comerciales
- IR-2
- Formularios por institución (catálogo)

Integración en main.py (2 líneas):
    from router_bid_manager import bid_manager_router
    app.include_router(bid_manager_router)
"""

import os
from datetime import datetime
from typing import Optional, List

import re
import uuid
from fastapi import APIRouter, HTTPException, Header, Query, UploadFile, File, Form
from pydantic import BaseModel
from supabase import create_client

# ── Clientes Supabase ──────────────────────────────────────
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
SUPABASE_SERVICE_KEY = os.getenv("SUPABASE_SERVICE_KEY", SUPABASE_KEY)

_sb = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)

bid_manager_router = APIRouter(prefix="/api/bid", tags=["bid-manager"])


# ── Auth ───────────────────────────────────────────────────
def _auth(authorization: str | None) -> str:
    """Valida Bearer token y devuelve user_id."""
    if not authorization or not authorization.lower().startswith("bearer "):
        raise HTTPException(401, "Falta el token de sesión")
    token = authorization.split(" ", 1)[1].strip()
    try:
        user_resp = _sb.auth.get_user(token)
        if not user_resp or not user_resp.user:
            raise HTTPException(401, "Sesión inválida o expirada")
        return user_resp.user.id
    except HTTPException:
        raise
    except Exception:
        raise HTTPException(401, "Sesión inválida o expirada")


# ── Helper: obtener empresa_id del usuario ─────────────────
def _empresa_id(user_id: str) -> str:
    """Devuelve el ID de perfiles_empresa del usuario. Lanza 404 si no existe."""
    res = _sb.table("perfiles_empresa").select("id").eq("user_id", user_id).execute()
    if not res.data:
        raise HTTPException(404, "No tienes perfil de empresa. Completa el onboarding primero.")
    return res.data[0]["id"]


# ── Helper: CRUD genérico ──────────────────────────────────
def _crear(tabla: str, data: dict, empresa_id: str):
    data["empresa_id"] = empresa_id
    res = _sb.table(tabla).insert(data).execute()
    return res.data[0] if res.data else None

def _listar(tabla: str, empresa_id: str, filtros: dict = None):
    q = _sb.table(tabla).select("*").eq("empresa_id", empresa_id)
    if filtros:
        for k, v in filtros.items():
            if v is not None:
                q = q.eq(k, v)
    return q.order("creado_en", desc=True).execute().data or []

def _obtener(tabla: str, id: str, empresa_id: str):
    res = _sb.table(tabla).select("*").eq("id", id).eq("empresa_id", empresa_id).execute()
    if not res.data:
        raise HTTPException(404, "Registro no encontrado")
    return res.data[0]

def _actualizar(tabla: str, id: str, data: dict, empresa_id: str):
    data["actualizado_en"] = datetime.utcnow().isoformat()
    res = _sb.table(tabla).update(data).eq("id", id).eq("empresa_id", empresa_id).execute()
    if not res.data:
        raise HTTPException(404, "Registro no encontrado")
    return res.data[0]

def _eliminar(tabla: str, id: str, empresa_id: str):
    res = _sb.table(tabla).delete().eq("id", id).eq("empresa_id", empresa_id).execute()
    if not res.data:
        raise HTTPException(404, "Registro no encontrado")
    return {"ok": True}


# ══════════════════════════════════════════════════════════════
# 1. PERFIL DE EMPRESA (campos extendidos del Bid Manager)
# ══════════════════════════════════════════════════════════════

CAMPOS_BID_EMPRESA = {
    "rpe", "registro_mercantil_no", "tipo_sociedad", "capital_social",
    "moneda_capital", "objeto_social", "fecha_constitucion", "fecha_registro_rpe",
    "telefono_principal", "telefono_secundario", "clasificacion_mipyme",
    "registro_mipyme", "provee", "sitio_web", "direccion_completa",
    # Campos originales que también se pueden actualizar
    "razon_social", "rnc", "domicilio", "email_empresa",
}

@bid_manager_router.get("/empresa")
def get_empresa(authorization: str | None = Header(default=None)):
    """Obtener perfil completo de empresa con todos los campos del Bid Manager."""
    uid = _auth(authorization)
    res = _sb.table("perfiles_empresa").select("*").eq("user_id", uid).execute()
    return {"empresa": res.data[0] if res.data else None}

@bid_manager_router.put("/empresa")
async def actualizar_empresa(request: dict, authorization: str | None = Header(default=None)):
    """Actualizar campos del Bid Manager en el perfil de empresa."""
    uid = _auth(authorization)
    data = {k: v for k, v in request.items() if k in CAMPOS_BID_EMPRESA}
    if not data:
        raise HTTPException(400, "No hay campos válidos para actualizar")
    data["actualizado_en"] = datetime.utcnow().isoformat()
    
    existente = _sb.table("perfiles_empresa").select("id").eq("user_id", uid).execute()
    if existente.data:
        _sb.table("perfiles_empresa").update(data).eq("user_id", uid).execute()
    else:
        data["user_id"] = uid
        _sb.table("perfiles_empresa").insert(data).execute()
    return {"ok": True}


# ══════════════════════════════════════════════════════════════
# 2. REPRESENTANTES LEGALES
# ══════════════════════════════════════════════════════════════

@bid_manager_router.post("/representantes")
async def crear_representante(request: dict, authorization: str | None = Header(default=None)):
    uid = _auth(authorization)
    eid = _empresa_id(uid)
    return _crear("bid_representantes", request, eid)

@bid_manager_router.get("/representantes")
def listar_representantes(authorization: str | None = Header(default=None)):
    uid = _auth(authorization)
    eid = _empresa_id(uid)
    return _listar("bid_representantes", eid)

@bid_manager_router.get("/representantes/{id}")
def obtener_representante(id: str, authorization: str | None = Header(default=None)):
    uid = _auth(authorization)
    eid = _empresa_id(uid)
    return _obtener("bid_representantes", id, eid)

@bid_manager_router.put("/representantes/{id}")
async def actualizar_representante(id: str, request: dict, authorization: str | None = Header(default=None)):
    uid = _auth(authorization)
    eid = _empresa_id(uid)
    return _actualizar("bid_representantes", id, request, eid)

@bid_manager_router.delete("/representantes/{id}")
def eliminar_representante(id: str, authorization: str | None = Header(default=None)):
    uid = _auth(authorization)
    eid = _empresa_id(uid)
    return _eliminar("bid_representantes", id, eid)


# ══════════════════════════════════════════════════════════════
# 3. SOCIOS / ACCIONISTAS
# ══════════════════════════════════════════════════════════════

def _validar_participacion(empresa_id: str, nuevo_pct, excluir_id: str = None):
    """Valida que la suma de porcentajes de participación no exceda 100%."""
    try:
        pct_nuevo = float(nuevo_pct or 0)
    except (TypeError, ValueError):
        pct_nuevo = 0
    if pct_nuevo <= 0:
        return
    q = _sb.table("bid_socios").select("id, porcentaje_participacion").eq("empresa_id", empresa_id)
    rows = q.execute().data or []
    total_otros = sum(
        float(r.get("porcentaje_participacion") or 0)
        for r in rows if r["id"] != excluir_id
    )
    if total_otros + pct_nuevo > 100.0001:  # tolerancia flotante
        disponible = max(0, 100 - total_otros)
        raise HTTPException(
            400,
            f"La participación total no puede exceder 100%. "
            f"Ya hay {total_otros:.2f}% asignado; disponible: {disponible:.2f}%"
        )

@bid_manager_router.post("/socios")
async def crear_socio(request: dict, authorization: str | None = Header(default=None)):
    uid = _auth(authorization)
    eid = _empresa_id(uid)
    _validar_participacion(eid, request.get("porcentaje_participacion"))
    return _crear("bid_socios", request, eid)

@bid_manager_router.get("/socios")
def listar_socios(authorization: str | None = Header(default=None)):
    uid = _auth(authorization)
    return _listar("bid_socios", _empresa_id(uid))

@bid_manager_router.get("/socios/{id}")
def obtener_socio(id: str, authorization: str | None = Header(default=None)):
    uid = _auth(authorization)
    return _obtener("bid_socios", id, _empresa_id(uid))

@bid_manager_router.put("/socios/{id}")
async def actualizar_socio(id: str, request: dict, authorization: str | None = Header(default=None)):
    uid = _auth(authorization)
    eid = _empresa_id(uid)
    if "porcentaje_participacion" in request:
        _validar_participacion(eid, request.get("porcentaje_participacion"), excluir_id=id)
    return _actualizar("bid_socios", id, request, eid)

@bid_manager_router.delete("/socios/{id}")
def eliminar_socio(id: str, authorization: str | None = Header(default=None)):
    uid = _auth(authorization)
    return _eliminar("bid_socios", id, _empresa_id(uid))


# ══════════════════════════════════════════════════════════════
# 4. CERTIFICACIONES Y DOCUMENTOS DE EMPRESA
# ══════════════════════════════════════════════════════════════
# Tipos: DGII, TSS, RPE, REGISTRO_MERCANTIL, MIPYME,
#        ANTECEDENTES_PENALES, ESTATUTOS, ACTA_ASAMBLEA,
#        PODER_REPRESENTACION, NOMINA_SOCIETARIA,
#        CEDULA_REPRESENTANTE, OTRO

@bid_manager_router.post("/certificaciones")
async def crear_certificacion(request: dict, authorization: str | None = Header(default=None)):
    uid = _auth(authorization)
    return _crear("bid_certificaciones", request, _empresa_id(uid))

@bid_manager_router.get("/certificaciones")
def listar_certificaciones(
    tipo: str = Query(None),
    estado: str = Query(None),
    authorization: str | None = Header(default=None)
):
    uid = _auth(authorization)
    return _listar("bid_certificaciones", _empresa_id(uid), {"tipo": tipo, "estado": estado})

@bid_manager_router.get("/certificaciones/alertas")
def alertas_vencimiento(authorization: str | None = Header(default=None)):
    """Devuelve certificaciones vencidas o por vencer en los próximos 15 días."""
    uid = _auth(authorization)
    eid = _empresa_id(uid)
    # Actualizar estados antes de consultar
    _sb.rpc("fn_actualizar_estados_certificaciones", {"p_empresa_id": eid}).execute()
    res = _sb.table("bid_certificaciones") \
        .select("*") \
        .eq("empresa_id", eid) \
        .in_("estado", ["vencido", "por_vencer"]) \
        .order("fecha_vencimiento") \
        .execute()
    return res.data or []

@bid_manager_router.get("/certificaciones/{id}")
def obtener_certificacion(id: str, authorization: str | None = Header(default=None)):
    uid = _auth(authorization)
    return _obtener("bid_certificaciones", id, _empresa_id(uid))

@bid_manager_router.put("/certificaciones/{id}")
async def actualizar_certificacion(id: str, request: dict, authorization: str | None = Header(default=None)):
    uid = _auth(authorization)
    return _actualizar("bid_certificaciones", id, request, _empresa_id(uid))

@bid_manager_router.delete("/certificaciones/{id}")
def eliminar_certificacion(id: str, authorization: str | None = Header(default=None)):
    uid = _auth(authorization)
    return _eliminar("bid_certificaciones", id, _empresa_id(uid))


# ══════════════════════════════════════════════════════════════
# 5. PERSONAL TÉCNICO
# ══════════════════════════════════════════════════════════════

@bid_manager_router.post("/personal")
async def crear_personal(request: dict, authorization: str | None = Header(default=None)):
    uid = _auth(authorization)
    return _crear("bid_personal", request, _empresa_id(uid))

@bid_manager_router.get("/personal")
def listar_personal(
    cargo: str = Query(None),
    profesion: str = Query(None),
    disponible: bool = Query(None),
    authorization: str | None = Header(default=None)
):
    uid = _auth(authorization)
    filtros = {"cargo_empresa": cargo, "profesion": profesion}
    if disponible is not None:
        filtros["disponible"] = disponible
    return _listar("bid_personal", _empresa_id(uid), filtros)

@bid_manager_router.get("/personal/{id}")
def obtener_personal(id: str, authorization: str | None = Header(default=None)):
    uid = _auth(authorization)
    return _obtener("bid_personal", id, _empresa_id(uid))

@bid_manager_router.put("/personal/{id}")
async def actualizar_personal(id: str, request: dict, authorization: str | None = Header(default=None)):
    uid = _auth(authorization)
    return _actualizar("bid_personal", id, request, _empresa_id(uid))

@bid_manager_router.delete("/personal/{id}")
def eliminar_personal(id: str, authorization: str | None = Header(default=None)):
    uid = _auth(authorization)
    return _eliminar("bid_personal", id, _empresa_id(uid))


# ══════════════════════════════════════════════════════════════
# 6. CERTIFICACIONES DEL PERSONAL
# ══════════════════════════════════════════════════════════════

def _verificar_personal_pertenece(personal_id: str, empresa_id: str):
    """Verifica que el personal pertenece a la empresa del usuario."""
    res = _sb.table("bid_personal").select("id").eq("id", personal_id).eq("empresa_id", empresa_id).execute()
    if not res.data:
        raise HTTPException(404, "Personal no encontrado")

@bid_manager_router.post("/personal/{personal_id}/certificaciones")
async def crear_cert_personal(personal_id: str, request: dict, authorization: str | None = Header(default=None)):
    uid = _auth(authorization)
    eid = _empresa_id(uid)
    _verificar_personal_pertenece(personal_id, eid)
    request["personal_id"] = personal_id
    res = _sb.table("bid_personal_certificaciones").insert(request).execute()
    return res.data[0] if res.data else None

@bid_manager_router.get("/personal/{personal_id}/certificaciones")
def listar_certs_personal(personal_id: str, authorization: str | None = Header(default=None)):
    uid = _auth(authorization)
    eid = _empresa_id(uid)
    _verificar_personal_pertenece(personal_id, eid)
    return _sb.table("bid_personal_certificaciones") \
        .select("*").eq("personal_id", personal_id) \
        .order("creado_en", desc=True).execute().data or []

@bid_manager_router.delete("/personal/{personal_id}/certificaciones/{cert_id}")
def eliminar_cert_personal(personal_id: str, cert_id: str, authorization: str | None = Header(default=None)):
    uid = _auth(authorization)
    eid = _empresa_id(uid)
    _verificar_personal_pertenece(personal_id, eid)
    _sb.table("bid_personal_certificaciones").delete().eq("id", cert_id).eq("personal_id", personal_id).execute()
    return {"ok": True}


# ══════════════════════════════════════════════════════════════
# 7. EXPERIENCIA (BANCO DE PROYECTOS)
# ══════════════════════════════════════════════════════════════

@bid_manager_router.post("/experiencia")
async def crear_experiencia(request: dict, authorization: str | None = Header(default=None)):
    uid = _auth(authorization)
    return _crear("bid_experiencia", request, _empresa_id(uid))

@bid_manager_router.get("/experiencia")
def listar_experiencia(
    tipo_obra: str = Query(None),
    cliente: str = Query(None),
    estado: str = Query(None),
    authorization: str | None = Header(default=None)
):
    uid = _auth(authorization)
    return _listar("bid_experiencia", _empresa_id(uid), {
        "tipo_obra": tipo_obra, "cliente": cliente, "estado": estado
    })

@bid_manager_router.get("/experiencia/{id}")
def obtener_experiencia(id: str, authorization: str | None = Header(default=None)):
    uid = _auth(authorization)
    return _obtener("bid_experiencia", id, _empresa_id(uid))

@bid_manager_router.put("/experiencia/{id}")
async def actualizar_experiencia(id: str, request: dict, authorization: str | None = Header(default=None)):
    uid = _auth(authorization)
    return _actualizar("bid_experiencia", id, request, _empresa_id(uid))

@bid_manager_router.delete("/experiencia/{id}")
def eliminar_experiencia(id: str, authorization: str | None = Header(default=None)):
    uid = _auth(authorization)
    return _eliminar("bid_experiencia", id, _empresa_id(uid))


# ══════════════════════════════════════════════════════════════
# 8. PERSONAL ASIGNADO A PROYECTOS
# ══════════════════════════════════════════════════════════════

def _verificar_experiencia_pertenece(exp_id: str, empresa_id: str):
    res = _sb.table("bid_experiencia").select("id").eq("id", exp_id).eq("empresa_id", empresa_id).execute()
    if not res.data:
        raise HTTPException(404, "Proyecto no encontrado")

@bid_manager_router.post("/experiencia/{exp_id}/personal")
async def asignar_personal_proyecto(exp_id: str, request: dict, authorization: str | None = Header(default=None)):
    uid = _auth(authorization)
    eid = _empresa_id(uid)
    _verificar_experiencia_pertenece(exp_id, eid)
    request["experiencia_id"] = exp_id
    res = _sb.table("bid_experiencia_personal").insert(request).execute()
    return res.data[0] if res.data else None

@bid_manager_router.get("/experiencia/{exp_id}/personal")
def listar_personal_proyecto(exp_id: str, authorization: str | None = Header(default=None)):
    uid = _auth(authorization)
    eid = _empresa_id(uid)
    _verificar_experiencia_pertenece(exp_id, eid)
    return _sb.table("bid_experiencia_personal") \
        .select("*, bid_personal(nombre_completo, profesion, cargo_empresa)") \
        .eq("experiencia_id", exp_id).execute().data or []

@bid_manager_router.delete("/experiencia/{exp_id}/personal/{asig_id}")
def desasignar_personal_proyecto(exp_id: str, asig_id: str, authorization: str | None = Header(default=None)):
    uid = _auth(authorization)
    eid = _empresa_id(uid)
    _verificar_experiencia_pertenece(exp_id, eid)
    _sb.table("bid_experiencia_personal").delete().eq("id", asig_id).eq("experiencia_id", exp_id).execute()
    return {"ok": True}


# ══════════════════════════════════════════════════════════════
# 9. EQUIPOS
# ══════════════════════════════════════════════════════════════

@bid_manager_router.post("/equipos")
async def crear_equipo(request: dict, authorization: str | None = Header(default=None)):
    uid = _auth(authorization)
    return _crear("bid_equipos", request, _empresa_id(uid))

@bid_manager_router.get("/equipos")
def listar_equipos(
    propiedad: str = Query(None),
    estado: str = Query(None),
    authorization: str | None = Header(default=None)
):
    uid = _auth(authorization)
    return _listar("bid_equipos", _empresa_id(uid), {"propiedad": propiedad, "estado": estado})

@bid_manager_router.get("/equipos/{id}")
def obtener_equipo(id: str, authorization: str | None = Header(default=None)):
    uid = _auth(authorization)
    return _obtener("bid_equipos", id, _empresa_id(uid))

@bid_manager_router.put("/equipos/{id}")
async def actualizar_equipo(id: str, request: dict, authorization: str | None = Header(default=None)):
    uid = _auth(authorization)
    return _actualizar("bid_equipos", id, request, _empresa_id(uid))

@bid_manager_router.delete("/equipos/{id}")
def eliminar_equipo(id: str, authorization: str | None = Header(default=None)):
    uid = _auth(authorization)
    return _eliminar("bid_equipos", id, _empresa_id(uid))


# ══════════════════════════════════════════════════════════════
# 10. ESTADOS FINANCIEROS
# ══════════════════════════════════════════════════════════════

@bid_manager_router.post("/financieros")
async def crear_financiero(request: dict, authorization: str | None = Header(default=None)):
    uid = _auth(authorization)
    return _crear("bid_financieros", request, _empresa_id(uid))

@bid_manager_router.get("/financieros")
def listar_financieros(authorization: str | None = Header(default=None)):
    uid = _auth(authorization)
    return _listar("bid_financieros", _empresa_id(uid))

@bid_manager_router.get("/financieros/{id}")
def obtener_financiero(id: str, authorization: str | None = Header(default=None)):
    uid = _auth(authorization)
    return _obtener("bid_financieros", id, _empresa_id(uid))

@bid_manager_router.put("/financieros/{id}")
async def actualizar_financiero(id: str, request: dict, authorization: str | None = Header(default=None)):
    uid = _auth(authorization)
    return _actualizar("bid_financieros", id, request, _empresa_id(uid))

@bid_manager_router.delete("/financieros/{id}")
def eliminar_financiero(id: str, authorization: str | None = Header(default=None)):
    uid = _auth(authorization)
    return _eliminar("bid_financieros", id, _empresa_id(uid))

@bid_manager_router.get("/financieros/indicadores/resumen")
def resumen_indicadores(authorization: str | None = Header(default=None)):
    """Devuelve los indicadores financieros del último período."""
    uid = _auth(authorization)
    eid = _empresa_id(uid)
    res = _sb.table("bid_financieros") \
        .select("periodo, fecha_cierre, solvencia, liquidez_corriente, endeudamiento, capital_trabajo, margen_utilidad, roe") \
        .eq("empresa_id", eid) \
        .order("fecha_cierre", desc=True) \
        .limit(2) \
        .execute()
    return {"indicadores": res.data or []}


# ══════════════════════════════════════════════════════════════
# 11. CAPACIDAD FINANCIERA (líneas de crédito, solvencias)
# ══════════════════════════════════════════════════════════════

@bid_manager_router.post("/capacidad-financiera")
async def crear_capacidad(request: dict, authorization: str | None = Header(default=None)):
    uid = _auth(authorization)
    return _crear("bid_capacidad_financiera", request, _empresa_id(uid))

@bid_manager_router.get("/capacidad-financiera")
def listar_capacidad(
    tipo: str = Query(None),
    authorization: str | None = Header(default=None)
):
    uid = _auth(authorization)
    return _listar("bid_capacidad_financiera", _empresa_id(uid), {"tipo": tipo})

@bid_manager_router.get("/capacidad-financiera/{id}")
def obtener_capacidad(id: str, authorization: str | None = Header(default=None)):
    uid = _auth(authorization)
    return _obtener("bid_capacidad_financiera", id, _empresa_id(uid))

@bid_manager_router.put("/capacidad-financiera/{id}")
async def actualizar_capacidad(id: str, request: dict, authorization: str | None = Header(default=None)):
    uid = _auth(authorization)
    return _actualizar("bid_capacidad_financiera", id, request, _empresa_id(uid))

@bid_manager_router.delete("/capacidad-financiera/{id}")
def eliminar_capacidad(id: str, authorization: str | None = Header(default=None)):
    uid = _auth(authorization)
    return _eliminar("bid_capacidad_financiera", id, _empresa_id(uid))

@bid_manager_router.get("/capacidad-financiera/resumen/total")
def resumen_capacidad(authorization: str | None = Header(default=None)):
    """Total disponible por tipo de capacidad financiera."""
    uid = _auth(authorization)
    eid = _empresa_id(uid)
    res = _sb.table("bid_capacidad_financiera") \
        .select("tipo, monto, monto_disponible, estado") \
        .eq("empresa_id", eid) \
        .eq("estado", "vigente") \
        .execute()
    
    por_tipo = {}
    for r in (res.data or []):
        t = r["tipo"]
        m = float(r.get("monto_disponible") or r.get("monto") or 0)
        por_tipo[t] = por_tipo.get(t, 0) + m
    
    return {
        "por_tipo": por_tipo,
        "total_disponible": sum(por_tipo.values())
    }


# ══════════════════════════════════════════════════════════════
# 12. REFERENCIAS COMERCIALES
# ══════════════════════════════════════════════════════════════

@bid_manager_router.post("/referencias-comerciales")
async def crear_referencia(request: dict, authorization: str | None = Header(default=None)):
    uid = _auth(authorization)
    return _crear("bid_referencias_comerciales", request, _empresa_id(uid))

@bid_manager_router.get("/referencias-comerciales")
def listar_referencias(authorization: str | None = Header(default=None)):
    uid = _auth(authorization)
    return _listar("bid_referencias_comerciales", _empresa_id(uid))

@bid_manager_router.get("/referencias-comerciales/{id}")
def obtener_referencia(id: str, authorization: str | None = Header(default=None)):
    uid = _auth(authorization)
    return _obtener("bid_referencias_comerciales", id, _empresa_id(uid))

@bid_manager_router.put("/referencias-comerciales/{id}")
async def actualizar_referencia(id: str, request: dict, authorization: str | None = Header(default=None)):
    uid = _auth(authorization)
    return _actualizar("bid_referencias_comerciales", id, request, _empresa_id(uid))

@bid_manager_router.delete("/referencias-comerciales/{id}")
def eliminar_referencia(id: str, authorization: str | None = Header(default=None)):
    uid = _auth(authorization)
    return _eliminar("bid_referencias_comerciales", id, _empresa_id(uid))


# ══════════════════════════════════════════════════════════════
# 13. IR-2
# ══════════════════════════════════════════════════════════════

@bid_manager_router.post("/ir2")
async def crear_ir2(request: dict, authorization: str | None = Header(default=None)):
    uid = _auth(authorization)
    return _crear("bid_ir2", request, _empresa_id(uid))

@bid_manager_router.get("/ir2")
def listar_ir2(authorization: str | None = Header(default=None)):
    uid = _auth(authorization)
    return _listar("bid_ir2", _empresa_id(uid))

@bid_manager_router.get("/ir2/{id}")
def obtener_ir2(id: str, authorization: str | None = Header(default=None)):
    uid = _auth(authorization)
    return _obtener("bid_ir2", id, _empresa_id(uid))

@bid_manager_router.put("/ir2/{id}")
async def actualizar_ir2(id: str, request: dict, authorization: str | None = Header(default=None)):
    uid = _auth(authorization)
    return _actualizar("bid_ir2", id, request, _empresa_id(uid))

@bid_manager_router.delete("/ir2/{id}")
def eliminar_ir2(id: str, authorization: str | None = Header(default=None)):
    uid = _auth(authorization)
    return _eliminar("bid_ir2", id, _empresa_id(uid))


# ══════════════════════════════════════════════════════════════
# 14. FORMULARIOS POR INSTITUCIÓN (catálogo, solo lectura)
# ══════════════════════════════════════════════════════════════

@bid_manager_router.get("/formularios")
def listar_formularios(
    institucion: str = Query(None),
    tipo: str = Query(None)
):
    """Catálogo de formularios por institución. No requiere auth."""
    q = _sb.table("bid_formularios_institucion").select("*").eq("activo", True)
    if institucion:
        q = q.eq("institucion", institucion)
    if tipo:
        q = q.eq("tipo", tipo)
    return q.order("institucion").execute().data or []


# ══════════════════════════════════════════════════════════════
# 15. RESUMEN GENERAL DEL EXPEDIENTE
# ══════════════════════════════════════════════════════════════

@bid_manager_router.get("/expediente/resumen")
def resumen_expediente(authorization: str | None = Header(default=None)):
    """Dashboard: resumen del estado del expediente digital."""
    uid = _auth(authorization)
    eid = _empresa_id(uid)

    # Contar registros por módulo
    representantes = _sb.table("bid_representantes").select("id", count="exact").eq("empresa_id", eid).execute()
    socios = _sb.table("bid_socios").select("id", count="exact").eq("empresa_id", eid).execute()
    certificaciones = _sb.table("bid_certificaciones").select("id", count="exact").eq("empresa_id", eid).execute()
    cert_vencidas = _sb.table("bid_certificaciones").select("id", count="exact").eq("empresa_id", eid).in_("estado", ["vencido", "por_vencer"]).execute()
    personal = _sb.table("bid_personal").select("id", count="exact").eq("empresa_id", eid).execute()
    experiencia = _sb.table("bid_experiencia").select("id", count="exact").eq("empresa_id", eid).execute()
    equipos = _sb.table("bid_equipos").select("id", count="exact").eq("empresa_id", eid).execute()
    financieros = _sb.table("bid_financieros").select("id", count="exact").eq("empresa_id", eid).execute()
    capacidad = _sb.table("bid_capacidad_financiera").select("id", count="exact").eq("empresa_id", eid).execute()
    referencias = _sb.table("bid_referencias_comerciales").select("id", count="exact").eq("empresa_id", eid).execute()
    ir2 = _sb.table("bid_ir2").select("id", count="exact").eq("empresa_id", eid).execute()

    return {
        "empresa_id": eid,
        "modulos": {
            "representantes": representantes.count or 0,
            "socios": socios.count or 0,
            "certificaciones": certificaciones.count or 0,
            "certificaciones_alertas": cert_vencidas.count or 0,
            "personal": personal.count or 0,
            "experiencia": experiencia.count or 0,
            "equipos": equipos.count or 0,
            "estados_financieros": financieros.count or 0,
            "capacidad_financiera": capacidad.count or 0,
            "referencias_comerciales": referencias.count or 0,
            "ir2": ir2.count or 0,
        }
    }


# ══════════════════════════════════════════════════════════════
# 15. UPLOAD DE ARCHIVOS (Supabase Storage: bucket bid-manager-docs)
# ══════════════════════════════════════════════════════════════
# El bucket es PRIVADO. RLS multi-tenant vive en Storage.
# Los archivos se sirven al frontend mediante signed URLs de corta duración.
#
# Estructura de paths:  {empresa_id}/{categoria}/{archivo}
# Ejemplos:
#   f47ac10b-.../certificaciones/a1b2c3d4_dgii-2026.pdf
#   f47ac10b-.../personal/e5f6g7h8_cedula-lonny.jpg
# ══════════════════════════════════════════════════════════════

STORAGE_BUCKET = "bid-manager-docs"
STORAGE_MAX_BYTES = 25 * 1024 * 1024  # 25 MB (igual que el limit del bucket)

VALID_CATEGORIAS = {
    "certificaciones", "personal", "equipos", "experiencia",
    "financieros", "ir2", "capacidad", "referencias", "otro"
}


def _sanitize_filename(nombre: str) -> str:
    """
    Devuelve un nombre de archivo seguro:
    - reemplaza caracteres raros por _
    - trunca a 80 chars
    - antepone un prefijo aleatorio para evitar colisiones dentro de la misma carpeta
    - preserva la extensión original en minúsculas
    """
    if not nombre:
        nombre = "archivo"
    partes = nombre.rsplit(".", 1)
    base = partes[0] if len(partes) > 1 else nombre
    ext = ("." + partes[1].lower()) if len(partes) > 1 else ""
    base_limpio = re.sub(r"[^A-Za-z0-9_\-]", "_", base).strip("_") or "archivo"
    base_limpio = base_limpio[:80]
    prefijo = uuid.uuid4().hex[:8]
    return f"{prefijo}_{base_limpio}{ext}"


def _validar_path(path: str, empresa_id: str) -> None:
    """Bloquea path traversal y accesos cross-tenant."""
    if not path:
        raise HTTPException(400, "Path requerido")
    if ".." in path or path.startswith("/") or "\\" in path:
        raise HTTPException(400, "Path inválido")
    prefijo_empresa = f"{empresa_id}/"
    if not path.startswith(prefijo_empresa):
        raise HTTPException(403, "No autorizado para acceder a este archivo")


@bid_manager_router.post("/upload")
async def upload_archivo(
    categoria: str = Form(...),
    file: UploadFile = File(...),
    authorization: str | None = Header(default=None)
):
    """
    Sube un archivo al bucket privado bid-manager-docs.

    Body (multipart/form-data):
      - categoria: str  (una de VALID_CATEGORIAS)
      - file: archivo binario

    Respuesta:
      {"path": "...", "name": "original.pdf", "size": 12345, "content_type": "application/pdf"}

    El frontend debe guardar el `path` devuelto en el campo _url correspondiente
    del registro (bid_certificaciones.archivo_url, bid_personal.cedula_url, etc).
    Los paths NO son URLs directas; para ver el archivo pedir /api/bid/signed-url.
    """
    uid = _auth(authorization)
    eid = _empresa_id(uid)

    if categoria not in VALID_CATEGORIAS:
        raise HTTPException(
            400,
            f"Categoría inválida. Debe ser una de: {', '.join(sorted(VALID_CATEGORIAS))}"
        )

    contenido = await file.read()
    if not contenido:
        raise HTTPException(400, "Archivo vacío")
    if len(contenido) > STORAGE_MAX_BYTES:
        max_mb = STORAGE_MAX_BYTES // 1024 // 1024
        raise HTTPException(413, f"El archivo excede el máximo permitido de {max_mb} MB")

    safe_name = _sanitize_filename(file.filename or "archivo")
    path = f"{eid}/{categoria}/{safe_name}"
    content_type = file.content_type or "application/octet-stream"

    try:
        _sb.storage.from_(STORAGE_BUCKET).upload(
            path=path,
            file=contenido,
            file_options={"content-type": content_type, "upsert": "false"},
        )
    except Exception as e:
        # Si es un duplicado (raro por el prefijo UUID), reintentar con nuevo prefijo
        msg = str(e)
        if "Duplicate" in msg or "already exists" in msg.lower():
            safe_name = _sanitize_filename(file.filename or "archivo")
            path = f"{eid}/{categoria}/{safe_name}"
            try:
                _sb.storage.from_(STORAGE_BUCKET).upload(
                    path=path,
                    file=contenido,
                    file_options={"content-type": content_type, "upsert": "false"},
                )
            except Exception as e2:
                raise HTTPException(500, f"Error subiendo archivo: {str(e2)}")
        else:
            raise HTTPException(500, f"Error subiendo archivo: {msg}")

    return {
        "path": path,
        "name": file.filename,
        "size": len(contenido),
        "content_type": content_type,
    }


@bid_manager_router.get("/signed-url")
def obtener_signed_url(
    path: str = Query(..., description="Path del archivo en el bucket (empresa_id/categoria/archivo)"),
    expires_in: int = Query(3600, ge=60, le=86400, description="Segundos de validez (60 a 86400)"),
    authorization: str | None = Header(default=None),
):
    """
    Genera una signed URL temporal para ver/descargar el archivo.
    Solo funciona si el path pertenece a la empresa del usuario.
    Por defecto 1 hora de validez.
    """
    uid = _auth(authorization)
    eid = _empresa_id(uid)
    _validar_path(path, eid)

    try:
        res = _sb.storage.from_(STORAGE_BUCKET).create_signed_url(path, expires_in)
        signed = (res or {}).get("signedURL") or (res or {}).get("signed_url")
        if not signed:
            raise HTTPException(500, "No se pudo generar la URL firmada")
        return {"url": signed, "expires_in": expires_in}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(500, f"Error generando URL firmada: {str(e)}")


@bid_manager_router.delete("/upload")
def eliminar_archivo(
    path: str = Query(..., description="Path del archivo a eliminar"),
    authorization: str | None = Header(default=None),
):
    """
    Elimina un archivo del bucket.
    Solo funciona si el path pertenece a la empresa del usuario.
    """
    uid = _auth(authorization)
    eid = _empresa_id(uid)
    _validar_path(path, eid)

    try:
        _sb.storage.from_(STORAGE_BUCKET).remove([path])
    except Exception as e:
        raise HTTPException(500, f"Error eliminando archivo: {str(e)}")

    return {"ok": True, "path": path}


# ══════════════════════════════════════════════════════════════
# 16. EXTRACCIÓN CON IA — Estados Financieros / IR-2 (Gemini)
# ══════════════════════════════════════════════════════════════
# Sube un PDF de estados financieros o IR-2 y Gemini extrae los
# valores numéricos para pre-llenar el formulario. El usuario
# revisa y guarda; nada se persiste automáticamente.
# ══════════════════════════════════════════════════════════════

import base64
import json as _json
import urllib.request as _urlreq

GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
GEMINI_MODEL = os.getenv("GEMINI_MODEL", "gemini-2.5-flash")

_PROMPT_FINANCIEROS = """Eres un contador experto en estados financieros dominicanos.
Analiza el PDF adjunto (estados financieros auditados o declaración IR-2 de DGII).
Extrae los valores en DOP (pesos dominicanos). Si un valor no aparece, usa null.
Responde SOLO con JSON válido, sin markdown, sin explicación, con esta estructura exacta:
{
  "tipo_detectado": "estados_financieros" | "ir2",
  "periodo": "2025",
  "fecha_cierre": "2025-12-31",
  "activos_corrientes": 0,
  "activos_no_corrientes": 0,
  "activos_totales": 0,
  "pasivos_corrientes": 0,
  "pasivos_no_corrientes": 0,
  "pasivos_totales": 0,
  "patrimonio_neto": 0,
  "ingresos": 0,
  "costo_ventas": 0,
  "utilidad_bruta": 0,
  "gastos_operativos": 0,
  "utilidad_operativa": 0,
  "utilidad_neta": 0,
  "ingresos_brutos": 0,
  "costos_y_gastos": 0,
  "renta_neta_imponible": 0,
  "impuesto_liquidado": 0,
  "confianza": "alta" | "media" | "baja",
  "notas": "observaciones breves si algo es ambiguo"
}
Reglas:
- Números sin separadores de miles ni símbolos de moneda (ej: 1500000.50)
- Si es IR-2: llena también los campos de balance (total activos → activos_totales, etc.)
- Si hay varios períodos, usa el MÁS RECIENTE
- "confianza" baja si el PDF es escaneado borroso o faltan páginas"""


def _gemini_extraer_financieros(contenido: bytes) -> dict:
    """Llama a Gemini con el PDF y devuelve el dict extraído. Lanza HTTPException en error."""
    if not GEMINI_API_KEY:
        raise HTTPException(503, "Extracción IA no configurada (falta GEMINI_API_KEY)")

    payload = {
        "contents": [{
            "parts": [
                {"inline_data": {"mime_type": "application/pdf",
                                 "data": base64.b64encode(contenido).decode()}},
                {"text": _PROMPT_FINANCIEROS},
            ]
        }],
        "generationConfig": {
            "temperature": 0.1,
            "response_mime_type": "application/json",
        },
    }
    url = (f"https://generativelanguage.googleapis.com/v1beta/models/"
           f"{GEMINI_MODEL}:generateContent?key={GEMINI_API_KEY}")
    try:
        req = _urlreq.Request(
            url,
            data=_json.dumps(payload).encode(),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with _urlreq.urlopen(req, timeout=120) as resp:
            body = _json.loads(resp.read().decode())
    except Exception as e:
        raise HTTPException(502, f"Error llamando a Gemini: {str(e)}")

    try:
        texto = body["candidates"][0]["content"]["parts"][0]["text"].strip()
        if texto.startswith("```"):
            texto = texto.strip("`")
            if texto.lower().startswith("json"):
                texto = texto[4:]
        return _json.loads(texto)
    except Exception as e:
        raise HTTPException(502, f"Gemini devolvió una respuesta no parseable: {str(e)}")


@bid_manager_router.post("/financieros/extraer")
async def extraer_financieros_ia(
    file: UploadFile = File(...),
    authorization: str | None = Header(default=None),
):
    """
    Extrae con Gemini los valores de un PDF de estados financieros o IR-2.
    NO guarda nada: devuelve el JSON extraído para pre-llenar el formulario.
    """
    uid = _auth(authorization)
    _empresa_id(uid)  # valida que tenga perfil

    contenido = await file.read()
    if not contenido:
        raise HTTPException(400, "Archivo vacío")
    if len(contenido) > STORAGE_MAX_BYTES:
        raise HTTPException(413, "El archivo excede 25 MB")

    mime = file.content_type or "application/pdf"
    if "pdf" not in mime and not (file.filename or "").lower().endswith(".pdf"):
        raise HTTPException(400, "Solo se aceptan PDFs para extracción IA")

    datos = _gemini_extraer_financieros(contenido)
    return {"extraido": datos, "archivo": file.filename}


def _num(v):
    """Convierte a número o None."""
    if v is None:
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


@bid_manager_router.post("/financieros/extraer-lote")
async def extraer_lote_ia(
    files: List[UploadFile] = File(...),
    authorization: str | None = Header(default=None),
):
    """
    Analiza VARIOS PDFs (estados financieros y/o IR-2 de distintos años):
    para cada uno extrae con Gemini, lo sube al storage, detecta el tipo y
    CREA el registro correspondiente (bid_financieros o bid_ir2).
    Los indicadores se calculan solos (columnas GENERATED).
    Devuelve un resumen por archivo para que el usuario revise/edite.
    """
    uid = _auth(authorization)
    eid = _empresa_id(uid)

    if not files:
        raise HTTPException(400, "No se recibieron archivos")
    if len(files) > 6:
        raise HTTPException(400, "Máximo 6 archivos por lote")

    resultados = []
    for f in files:
        item = {"archivo": f.filename, "ok": False}
        try:
            contenido = await f.read()
            if not contenido:
                raise ValueError("Archivo vacío")
            if len(contenido) > STORAGE_MAX_BYTES:
                raise ValueError("Excede 25 MB")
            if "pdf" not in (f.content_type or "") and not (f.filename or "").lower().endswith(".pdf"):
                raise ValueError("Solo PDFs")

            datos = _gemini_extraer_financieros(contenido)
            tipo_det = (datos.get("tipo_detectado") or "estados_financieros").lower()
            es_ir2 = "ir2" in tipo_det or "ir-2" in tipo_det

            # Subir el PDF al storage
            categoria = "ir2" if es_ir2 else "financieros"
            safe_name = _sanitize_filename(f.filename or "archivo.pdf")
            path = f"{eid}/{categoria}/{safe_name}"
            _sb.storage.from_(STORAGE_BUCKET).upload(
                path=path,
                file=contenido,
                file_options={"content-type": "application/pdf", "upsert": "false"},
            )

            periodo = str(datos.get("periodo") or "")
            fecha_cierre = datos.get("fecha_cierre")

            if es_ir2:
                registro = {
                    "empresa_id": eid,
                    "periodo_fiscal": periodo or None,
                    "tipo": "anual",
                    "fecha_cierre_fiscal": fecha_cierre,
                    "ingresos_brutos": _num(datos.get("ingresos_brutos")) or _num(datos.get("ingresos")),
                    "costos_y_gastos": _num(datos.get("costos_y_gastos")),
                    "renta_neta_imponible": _num(datos.get("renta_neta_imponible")),
                    "impuesto_liquidado": _num(datos.get("impuesto_liquidado")),
                    "total_activos": _num(datos.get("activos_totales")),
                    "total_pasivos": _num(datos.get("pasivos_totales")),
                    "patrimonio": _num(datos.get("patrimonio_neto")),
                    "pdf_url": path,
                }
                registro = {k: v for k, v in registro.items() if v is not None}
                _sb.table("bid_ir2").insert(registro).execute()
                item["tipo"] = "IR-2"
            else:
                registro = {
                    "empresa_id": eid,
                    "periodo": periodo or None,
                    "tipo": "anual",
                    "fecha_cierre": fecha_cierre,
                    "activos_corrientes": _num(datos.get("activos_corrientes")),
                    "activos_no_corrientes": _num(datos.get("activos_no_corrientes")),
                    "activos_totales": _num(datos.get("activos_totales")),
                    "pasivos_corrientes": _num(datos.get("pasivos_corrientes")),
                    "pasivos_no_corrientes": _num(datos.get("pasivos_no_corrientes")),
                    "pasivos_totales": _num(datos.get("pasivos_totales")),
                    "patrimonio_neto": _num(datos.get("patrimonio_neto")),
                    "ingresos": _num(datos.get("ingresos")) or _num(datos.get("ingresos_brutos")),
                    "costo_ventas": _num(datos.get("costo_ventas")),
                    "utilidad_bruta": _num(datos.get("utilidad_bruta")),
                    "gastos_operativos": _num(datos.get("gastos_operativos")),
                    "utilidad_operativa": _num(datos.get("utilidad_operativa")),
                    "utilidad_neta": _num(datos.get("utilidad_neta")),
                    "pdf_url": path,
                }
                registro = {k: v for k, v in registro.items() if v is not None}
                _sb.table("bid_financieros").insert(registro).execute()
                item["tipo"] = "Estados financieros"

            item["ok"] = True
            item["periodo"] = periodo
            item["confianza"] = datos.get("confianza")
            item["notas"] = datos.get("notas")
        except HTTPException as he:
            item["error"] = he.detail
        except Exception as e:
            item["error"] = str(e)
        resultados.append(item)

    return {"resultados": resultados}
