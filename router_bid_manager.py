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
    eid = _empresa_id(uid)
    periodo = request.get("periodo")
    if periodo:
        existe = _sb.table("bid_financieros").select("id") \
            .eq("empresa_id", eid).eq("periodo", str(periodo)).execute().data
        if existe:
            raise HTTPException(
                409,
                f"Ya existe un estado financiero del período {periodo}. "
                f"Edita el existente en vez de crear otro."
            )
    return _crear("bid_financieros", request, eid)

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
    eid = _empresa_id(uid)
    periodo = request.get("periodo_fiscal")
    if periodo:
        existe = _sb.table("bid_ir2").select("id") \
            .eq("empresa_id", eid).eq("periodo_fiscal", str(periodo)).execute().data
        if existe:
            raise HTTPException(
                409,
                f"Ya existe una declaración IR-2 del período {periodo}. "
                f"Edita la existente en vez de crear otra."
            )
    return _crear("bid_ir2", request, eid)

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
                existente = _sb.table("bid_ir2").select("id") \
                    .eq("empresa_id", eid).eq("periodo_fiscal", periodo).execute().data \
                    if periodo else []
                if existente:
                    registro.pop("empresa_id", None)
                    registro["actualizado_en"] = datetime.utcnow().isoformat()
                    _sb.table("bid_ir2").update(registro) \
                        .eq("id", existente[0]["id"]).execute()
                    item["accion"] = "actualizado"
                else:
                    _sb.table("bid_ir2").insert(registro).execute()
                    item["accion"] = "creado"
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
                existente = _sb.table("bid_financieros").select("id") \
                    .eq("empresa_id", eid).eq("periodo", periodo).execute().data \
                    if periodo else []
                if existente:
                    registro.pop("empresa_id", None)
                    registro["actualizado_en"] = datetime.utcnow().isoformat()
                    _sb.table("bid_financieros").update(registro) \
                        .eq("id", existente[0]["id"]).execute()
                    item["accion"] = "actualizado"
                else:
                    _sb.table("bid_financieros").insert(registro).execute()
                    item["accion"] = "creado"
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


# ══════════════════════════════════════════════════════════════
# 17. CRON — ALERTAS DE VENCIMIENTO DE DOCUMENTOS (push + email)
# ══════════════════════════════════════════════════════════════
# Llamar diariamente (n8n / Railway cron / cron-job.org):
#   POST /api/bid/cron/alertas-vencimiento
#   Header: X-Admin-Key: <ADMIN_SECRET>
#
# Por cada certificación con fecha de vencimiento:
#   - por_vencer (<=7 días) y sin alerta previa → email + push "próximo a vencer"
#   - vencido y sin alerta previa               → email + push "documento vencido"
# Los flags evitan duplicados; se resetean al renovar la fecha en el PUT.
# ══════════════════════════════════════════════════════════════

ADMIN_SECRET_BID = os.getenv("ADMIN_SECRET", "")


def _email_alerta_html(razon_social: str, docs: list, vencidos: bool) -> str:
    titulo = "Documentos VENCIDOS" if vencidos else "Documentos próximos a vencer"
    color = "#dc2626" if vencidos else "#d97706"
    filas = "".join(
        f"<tr><td style='padding:8px 12px;border-bottom:1px solid #eee'>{d['nombre_display']}</td>"
        f"<td style='padding:8px 12px;border-bottom:1px solid #eee'>{d.get('fecha_vencimiento','')}</td></tr>"
        for d in docs
    )
    accion = ("Renuévalos cuanto antes: sin ellos no podrás presentar ofertas."
              if vencidos else
              "Renuévalos a tiempo para no quedar fuera de ningún proceso.")
    return f"""
    <div style="font-family:Arial,sans-serif;max-width:560px;margin:0 auto">
      <h2 style="color:{color}">{titulo}</h2>
      <p>Hola {razon_social or ''}, tu Expediente Digital en LicitacionLab tiene documentos que requieren atención:</p>
      <table style="width:100%;border-collapse:collapse;font-size:14px">
        <tr style="background:#f8f9fa">
          <th style="text-align:left;padding:8px 12px">Documento</th>
          <th style="text-align:left;padding:8px 12px">Vence</th>
        </tr>
        {filas}
      </table>
      <p style="margin-top:16px">{accion}</p>
      <p><a href="https://app.licitacionlab.com/#bid"
            style="display:inline-block;background:#16a34a;color:#fff;padding:10px 20px;
                   border-radius:6px;text-decoration:none">Ver mi Expediente Digital</a></p>
      <p style="color:#9ca3af;font-size:12px;margin-top:24px">LicitacionLab · Monitoreo de compras públicas RD</p>
    </div>
    """


@bid_manager_router.post("/cron/alertas-vencimiento")
def cron_alertas_vencimiento(x_admin_key: str = Header(None)):
    """Envía push + email por documentos vencidos o por vencer. Idempotente por flags."""
    if not ADMIN_SECRET_BID or x_admin_key != ADMIN_SECRET_BID:
        raise HTTPException(403, "No autorizado")

    # Imports lazy para evitar import circular con main.py
    try:
        from main import enviar_push_y_limpiar, _resend_send
    except Exception as e:
        raise HTTPException(500, f"No se pudieron importar helpers de main: {e}")

    hoy = datetime.utcnow().date().isoformat()

    # Refrescar estados de todas las certificaciones con fecha (bulk, sin filtro de empresa)
    try:
        _sb.rpc("fn_actualizar_estados_certificaciones_global", {}).execute()
    except Exception:
        pass  # si no existe la versión global, los estados por-trigger siguen siendo razonables

    # Candidatas: por_vencer sin alerta, o vencidas sin alerta
    certs = _sb.table("bid_certificaciones") \
        .select("id, empresa_id, nombre_display, fecha_vencimiento, estado, "
                "alerta_por_vencer_enviada, alerta_vencido_enviada") \
        .not_.is_("fecha_vencimiento", "null") \
        .in_("estado", ["por_vencer", "vencido"]) \
        .execute().data or []

    pendientes = [
        c for c in certs
        if (c["estado"] == "por_vencer" and not c.get("alerta_por_vencer_enviada"))
        or (c["estado"] == "vencido" and not c.get("alerta_vencido_enviada"))
    ]
    if not pendientes:
        return {"ok": True, "enviadas": 0, "detalle": "Sin alertas pendientes"}

    # Agrupar por empresa
    por_empresa: dict = {}
    for c in pendientes:
        por_empresa.setdefault(c["empresa_id"], []).append(c)

    resumen = {"empresas": 0, "emails": 0, "push": 0, "errores": []}

    for eid, docs in por_empresa.items():
        try:
            emp = _sb.table("perfiles_empresa") \
                .select("user_id, razon_social, email_empresa") \
                .eq("id", eid).execute().data
            if not emp:
                continue
            emp = emp[0]
            user_id = emp.get("user_id")
            razon = emp.get("razon_social") or "tu empresa"

            vencidos = [d for d in docs if d["estado"] == "vencido"]
            por_vencer = [d for d in docs if d["estado"] == "por_vencer"]

            # Email destino: email_empresa, o el email del auth user como fallback
            email = emp.get("email_empresa")
            if not email and user_id:
                try:
                    u = _sb.auth.admin.get_user_by_id(user_id)
                    email = u.user.email if u and u.user else None
                except Exception:
                    email = None

            # ── EMAIL ──
            if email:
                if vencidos:
                    if _resend_send(
                        f"⛔ {len(vencidos)} documento(s) VENCIDO(S) en tu expediente",
                        _email_alerta_html(razon, vencidos, vencidos=True), email
                    ):
                        resumen["emails"] += 1
                if por_vencer:
                    if _resend_send(
                        f"⏰ {len(por_vencer)} documento(s) por vencer en tu expediente",
                        _email_alerta_html(razon, por_vencer, vencidos=False), email
                    ):
                        resumen["emails"] += 1

            # ── PUSH ──
            if user_id:
                subs = _sb.table("user_subscriptions") \
                    .select("endpoint, auth, p256dh") \
                    .eq("user_id", user_id).eq("active", True) \
                    .execute().data or []
                for sub in subs:
                    if vencidos:
                        nombres = ", ".join(d["nombre_display"] for d in vencidos[:3])
                        if enviar_push_y_limpiar(
                            sub, "Documento vencido",
                            f"{nombres} — renuévalo para poder ofertar", "/#bid"
                        ):
                            resumen["push"] += 1
                    if por_vencer:
                        nombres = ", ".join(d["nombre_display"] for d in por_vencer[:3])
                        if enviar_push_y_limpiar(
                            sub, "Documento por vencer",
                            f"{nombres} — vence pronto, renuévalo a tiempo", "/#bid"
                        ):
                            resumen["push"] += 1

            # ── Marcar flags ──
            for d in vencidos:
                _sb.table("bid_certificaciones").update(
                    {"alerta_vencido_enviada": True}).eq("id", d["id"]).execute()
            for d in por_vencer:
                _sb.table("bid_certificaciones").update(
                    {"alerta_por_vencer_enviada": True}).eq("id", d["id"]).execute()

            resumen["empresas"] += 1
        except Exception as e:
            resumen["errores"].append({"empresa_id": eid, "error": str(e)})

    return {"ok": True, "fecha": hoy, "enviadas": len(pendientes), **resumen}


# ══════════════════════════════════════════════════════════════
# 18. MATCHING PLIEGO vs EXPEDIENTE (financiero + tecnico)
# ══════════════════════════════════════════════════════════════
# Flujo:
#   1. POST /matching/analizar  → sube el pliego, Gemini extrae los
#      requisitos (financieros, lineas de credito, personal, equipos,
#      experiencia, metodologia/puntaje, lotes), se guarda y se evalua
#      contra todo el expediente.
#   2. GET /matching            → historial de analisis.
#   3. POST /matching/{id}/reevaluar → recalcula con datos actuales.
#   4. DELETE /matching/{id}
#
# El esquema de "requisitos" es un objeto de secciones. Se mantiene
# retrocompatibilidad: si llega una lista (analisis viejos, solo
# financieros), se envuelve como {"financieros": [...]}.
# ══════════════════════════════════════════════════════════════

_PROMPT_MATCHING = """Eres un experto en licitaciones publicas dominicanas (Ley 47-25, Decreto 52-26, pliegos estandar SNCC.P.006).
Analiza el PDF adjunto (pliego de condiciones) y extrae los requisitos exigidos al oferente para el Sobre A.
Responde SOLO con JSON valido, sin markdown, con esta estructura exacta:
{
  "referencia": "INAPA-CCC-LPN-2026-0028",
  "nombre_proceso": "titulo corto del objeto",
  "institucion": "INAPA",
  "presupuesto_base": 3702545294.85,
  "anios_estados_requeridos": 2,
  "naturaleza_proceso": "acueducto_alcantarillado",
  "metodologia": "combinada",
  "puntaje_total": 70,
  "puntaje_minimo": 49,
  "lotes": [
    {"numero": 1, "nombre": "Lote 1", "presupuesto": 120000000.00}
  ],
  "requisitos": {
    "financieros": [
      {
        "nombre": "Indice de solvencia",
        "tipo": "solvencia",
        "formula": "Activo Total / Pasivo Total",
        "operador": ">=",
        "limite": 1.50,
        "pct_monto": null,
        "incluye_lineas_credito": false,
        "base_calculo": "ultimo",
        "subsanable": null,
        "texto_original": "Indice de solvencia = ACTIVO TOTAL / PASIVO TOTAL. Limite establecido: Igual o mayor a 1.50",
        "notas": ""
      }
    ],
    "lineas_credito": [
      {
        "tipo": "bancaria",
        "pct_grande": 0.25,
        "pct_mipyme": 0.15,
        "base": "lote",
        "subsanable": null,
        "texto_original": "Linea financiera por el veinticinco (25%) del valor estimado del lote...",
        "notas": "Certificacion de linea financiera"
      },
      {
        "tipo": "comercial",
        "pct_grande": 0.25,
        "pct_mipyme": 0.15,
        "base": "lote",
        "subsanable": null,
        "texto_original": "Linea comercial por el veinticinco (25%)...",
        "notas": "Certificaciones de ferreterias o entidades comerciales"
      }
    ],
    "personal": [
      {
        "cargo": "Director de Obra",
        "titulaciones": ["ingeniero civil", "arquitecto", "ingeniero electromecanico"],
        "anios_min": 5,
        "requiere_maestria": true,
        "area_maestria": "hidraulica, sanitaria o afines",
        "requiere_colegiatura": true,
        "certificaciones_funcion_min": 2,
        "cantidad": 1,
        "por_lote": false,
        "obligatorio": true,
        "subsanable": null,
        "texto_original": "Director de Obra: Titulo de Ingeniero Civil, Arquitecto o Ingeniero Electromecanico...",
        "puntaje": {"puntos_max": 11, "tramos_anios": [{"min": 10, "puntos": 4}, {"min": 5, "puntos": 2}], "puntos_maestria": 3, "tramos_certificaciones": [{"min": 6, "puntos": 4}, {"min": 3, "puntos": 2}, {"min": 2, "puntos": 1}]}
      }
    ],
    "equipos": [
      {
        "tipo": "camion volteo",
        "cantidad": 4,
        "capacidad_min": "12 m3",
        "anio_minimo": null,
        "admite_alquilado": true,
        "subsanable": false,
        "texto_original": "Camiones volteo (de 12 m3 minimo). 4",
        "notas": ""
      }
    ],
    "experiencia": {
      "criterio_similitud": "monto_acumulado",
      "naturaleza": ["vial_asfalto"],
      "cantidad_obras_min": 3,
      "monto_min_por_obra": null,
      "monto_min_acumulado": 258823485.45,
      "acepta_en_curso_pct": 60,
      "acepta_proyectos_propios": true,
      "requiere_acta": true,
      "pct_portafolio": null,
      "ventana_anios": null,
      "volumenes": [
        {"categoria": "movimiento_tierra", "unidad": "m3", "cantidad": 4653.0, "lote": 1}
      ],
      "subsanable": null,
      "texto_original": "frase literal del pliego que define la experiencia requerida",
      "puntaje": {"puntos_max": 20, "tramos_certificaciones": [{"min": 6, "puntos": 20}, {"min": 3, "puntos": 15}, {"min": 2, "puntos": 10}]},
      "notas": ""
    },
    "puntaje_blandos": [
      {"nombre": "Enfoque, metodologia y plan de trabajo", "puntos_max": 20},
      {"nombre": "Cronograma de ejecucion y flujo de caja", "puntos_max": 20}
    ],
    "otros": [
      {"nombre": "Planta de asfalto en el Gran Santo Domingo", "texto_original": "...", "subsanable": null}
    ]
  }
}
Reglas de extraccion:
- FINANCIEROS: los pliegos piden 2 a 4 de: solvencia (Activo/Pasivo), liquidez corriente (AC/PC), endeudamiento (Pasivo Total/Patrimonio Neto, a veces Pasivo/Activo: copia la formula TAL CUAL), capital de trabajo (AC-PC). Extrae SOLO los que pide, con limite y operador EXACTOS: "Mayor 1.20" es ">" 1.20; "Igual o mayor a 1.50" es ">=" 1.50; "Menor 1.50" es "<"; "Igual o Menor 1.40" es "<=".
- "base_calculo": "ultimo" si dice "sobre el ultimo balance" (los otros para tendencias); "promedio" si dice "sobre la media de estos balances" o similar. Default "ultimo".
- "tipo" financiero: solvencia, liquidez, capital_trabajo, endeudamiento, patrimonio, ingresos, otro.
- LINEAS DE CREDITO: si el pliego exige certificacion de linea financiera/bancaria o linea comercial/de suplidores por un % del presupuesto o del lote (patron tipico: 25% o 30% para grandes, 15% para MIPYMES segun circular DGCP44-PNP-2023-0011), extraelas en "lineas_credito" con tipo "bancaria" o "comercial", los porcentajes como fraccion (0.25) y "base": "lote" o "contratacion". NO las metas en financieros.
- PERSONAL: un objeto por cargo pedido. "titulaciones" es LISTA con todas las alternativas que acepta el pliego ("Ingeniero Civil o Arquitecto" = ["ingeniero civil","arquitecto"]). "anios_min" son los anos desde el titulo. Si pide maestria, "requiere_maestria": true y "area_maestria" con el area textual. "certificaciones_funcion_min": cuantas certificaciones de obras en esa funcion pide. "por_lote": true si exige uno DISTINTO por lote. Si pide "Residente I" y "Residente II" con el mismo perfil, puedes unificarlos en un requisito con "cantidad": 2. Si el requisito trae puntaje (metodologia combinada), llena "puntaje" con los tramos reales del pliego; si no, "puntaje": null.
- EQUIPOS: un objeto por tipo. "tipo" en minusculas y singular, usando estos nombres canonicos cuando apliquen: camion volteo, retroexcavadora, retropala, excavadora, motoniveladora, bulldozer, cargador frontal, minicargador, grua telescopica, camion grua, camion cama baja, camion cisterna, camion distribucion asfalto, pavimentadora, barredora, rodillo, compactador manual, compresor, planta electrica, torre de luz, bomba de achique, ligadora, vibrador hormigon, soldadora, camioneta, equipo topografia, maquina perforacion pozos, diferencial. Sinonimos: gradar/gredar/greda = motoniveladora; maquito = compactador manual; bobcat = minicargador; camion de agua = camion cisterna. "capacidad_min" con numero y unidad tal cual el pliego ("12 m3", "170 HP", "2000 galones", "12 ton", "40 KW"); null si no especifica.
- EXPERIENCIA (de la EMPRESA como contratista, no del personal): "criterio_similitud" es uno de: naturaleza (solo pide obras similares por tipo), monto (monto minimo POR obra), monto_acumulado (la SUMA de las obras debe alcanzar un monto, tipico "alcance o supere el presupuesto base"), volumen_partidas (pide volumenes ejecutados por categoria, ej. m3 de movimiento de tierra por lote), cantidad_intervenciones (ej. 50 puntos de bacheo), porcentaje_portafolio (ej. minimo 30% de la experiencia a nivel hospitalario, usar pct_portafolio: 30). "acepta_en_curso_pct": % minimo de ejecucion si acepta obras en curso (ej. 60 CAASD, 40 MIVHED); null si solo concluidas. "requiere_acta": true si exige carta de recepcion final/definitiva/finiquito/recibido conforme.
- "naturaleza_proceso" y "naturaleza" usan esta taxonomia: hidraulica_sanitaria, vial_asfalto, edificacion, edificacion_hospitalaria, edificacion_educativa, acueducto_alcantarillado, electromecanica, otra.
- METODOLOGIA: "cumple_no_cumple" o "combinada" (cuando hay puntaje tecnico con umbral, tipico 70 puntos con minimo 49 o 56). Extrae "puntaje_total" y "puntaje_minimo" si existen. Los criterios puntuados que NO dependen del expediente (enfoque, plan de trabajo, metodologia, cronograma, planes de seguridad/riesgo/ambiental) van en "puntaje_blandos" con sus puntos maximos.
- "subsanable": true/false si el pliego lo marca para ese requisito (los pliegos estandar lo indican requisito por requisito); null si no lo dice.
- "texto_original": SIEMPRE la frase literal del pliego de donde salio el requisito (maximo 300 caracteres). Es obligatorio para poder auditar.
- Requisitos que no encajan en ninguna seccion (ubicacion de plantas, certificaciones especiales) van en "otros".
- Si el proceso NO tiene lotes, "lotes": []. Si una seccion no aparece en el pliego, devuelvela vacia ([] o null). NUNCA inventes requisitos.
- Numeros sin separadores de miles."""


# ── Helpers de normalizacion y parseo ─────────────────────────

def _norm(txt) -> str:
    """minusculas, sin acentos, sin signos; colapsa espacios."""
    if not txt:
        return ""
    s = str(txt).lower()
    for a, b in (("á", "a"), ("é", "e"), ("í", "i"), ("ó", "o"), ("ú", "u"),
                 ("ü", "u"), ("ñ", "n")):
        s = s.replace(a, b)
    s = "".join(c if (c.isalnum() or c == " ") else " " for c in s)
    return " ".join(s.split())


def _stem(tok: str) -> str:
    """Recorte burdo de plurales/derivaciones: primeros 6 chars alfanumericos."""
    return tok[:6]


def _tokens(txt) -> set:
    return {_stem(t) for t in _norm(txt).split() if len(t) > 2}


# Alias reales sacados de pliegos RD (CAASD, INAPA, DIE, CEIZTUR, CPADP, SNS)
_EQUIPO_ALIAS = {
    "gradar": "motoniveladora", "gredar": "motoniveladora", "greda": "motoniveladora",
    "niveladora": "motoniveladora", "motoniveladora": "motoniveladora",
    "maquito": "compactador manual", "maquitos": "compactador manual",
    "bobcat": "minicargador", "mini cargador": "minicargador",
    "minicargador frontal": "minicargador", "minicargador": "minicargador",
    "camion de agua": "camion cisterna", "camion cisterna de agua": "camion cisterna",
    "cisterna": "camion cisterna",
    "torre portatil de iluminacion": "torre de luz", "torres de luz": "torre de luz",
    "torre de iluminacion": "torre de luz",
    "retro pala": "retropala", "retro-pala": "retropala",
    "camiones volteo": "camion volteo", "camion de volteo": "camion volteo",
    "volqueta": "camion volteo",
    "tractor bulldozer": "bulldozer", "tractor d6": "bulldozer", "tractor d 6": "bulldozer",
    "pala mecanica": "excavadora",
    "cargadora frontal": "cargador frontal",
    "ligadora de hormigon": "ligadora",
    "vibrador electrico": "vibrador hormigon", "vibrador para hormigon": "vibrador hormigon",
    "generador electrico": "planta electrica", "generador": "planta electrica",
    "rodillo liso vibratorio": "rodillo", "rodillo compactador": "rodillo",
    "rodillo vibrador": "rodillo", "rodillo de mano": "rodillo",
    "soldadora de arco": "soldadora",
    "grua": "grua telescopica",
}


def _canon_equipo(txt) -> str:
    """Lleva un nombre de equipo a su tipo canonico usando alias + normalizacion."""
    n = _norm(txt)
    if not n:
        return ""
    if n in _EQUIPO_ALIAS:
        return _EQUIPO_ALIAS[n]
    # busca el alias mas largo contenido en el texto
    mejor = ""
    for alias, canon in _EQUIPO_ALIAS.items():
        if alias in n and len(alias) > len(mejor):
            mejor = alias
            n_canon = canon
    if mejor:
        return n_canon
    return n


_UNIDADES = {
    "m3": "m3", "m³": "m3", "mts3": "m3", "metros cubicos": "m3",
    "m2": "m2", "m²": "m2", "metros cuadrados": "m2",
    "hp": "hp", "kw": "kw", "kva": "kw",
    "ton": "ton", "tons": "ton", "toneladas": "ton", "tonelada": "ton", "tn": "ton",
    "gal": "gal", "galones": "gal", "gl": "gal",
    "amp": "amp", "amperios": "amp",
    "pl": "pl", "pies": "pl",
    "pulg": "pulg", "pulgadas": "pulg",
    "m": "m", "mts": "m", "metros": "m",
    "ml": "ml",
}


def _parse_capacidad(txt):
    """'12 m3' → (12.0, 'm3'). ('1.5-2 fundas', modelos, etc.) → (None, None):
    lo que no parsea va a revision manual, nunca se adivina."""
    if txt is None:
        return None, None
    n = _norm(str(txt).replace(",", ""))  # comas de miles fuera antes de normalizar
    if not n:
        return None, None
    num = ""
    resto = ""
    for i, c in enumerate(n):
        if c.isdigit() or c in ".,":
            num += c
        elif num:
            resto = n[i:].strip()
            break
    if not num:
        return None, None
    try:
        valor = float(num.replace(",", ""))
    except ValueError:
        return None, None
    unidad_tok = resto.split()[0] if resto else ""
    unidad = _UNIDADES.get(unidad_tok)
    if unidad is None and resto:
        for k, v in _UNIDADES.items():
            if resto.startswith(k):
                unidad = v
                break
    return (valor, unidad) if unidad else (None, None)


def _anios_desde(fecha_str):
    """Anos completos desde una fecha ISO (fecha_graduacion) hasta hoy."""
    if not fecha_str:
        return None
    try:
        f = datetime.fromisoformat(str(fecha_str)[:10])
    except ValueError:
        return None
    hoy = datetime.utcnow()
    return hoy.year - f.year - (1 if (hoy.month, hoy.day) < (f.month, f.day) else 0)


def _num_or_none(v):
    try:
        return float(v) if v is not None else None
    except (TypeError, ValueError):
        return None


def _cmp(valor, operador, limite):
    op = (operador or ">=").strip()
    if op in (">", "mayor"):
        return valor > limite
    if op in ("<", "menor"):
        return valor < limite
    if op in ("<=", "≤", "menor_igual"):
        return valor <= limite
    return valor >= limite  # default >=


# Taxonomia de naturaleza → palabras clave para matchear contra el expediente
_NATURALEZA_KEYWORDS = {
    "hidraulica_sanitaria": ["hidraulica", "sanitaria", "sanitario", "canada", "cañada",
                             "saneamiento", "pluvial", "drenaje", "alcantarillado"],
    "vial_asfalto": ["asfalto", "asfaltico", "vial", "carretera", "bacheo", "pavimento",
                     "calle", "camino", "conten", "cuneta"],
    "edificacion": ["edificacion", "edificio", "vivienda", "construccion", "remodelacion",
                    "readecuacion", "rehabilitacion"],
    "edificacion_hospitalaria": ["hospital", "hospitalaria", "salud", "clinica",
                                 "centro de salud", "medica"],
    "edificacion_educativa": ["escuela", "educativo", "politecnico", "aula", "liceo",
                              "centro educativo", "deportiva"],
    "acueducto_alcantarillado": ["acueducto", "agua potable", "alcantarillado", "pozo",
                                 "tuberia", "redes de agua", "linea de impulsion"],
    "electromecanica": ["electromecanica", "electrica", "subestacion", "bombeo",
                        "planta de tratamiento"],
}


def _obra_matchea_naturaleza(obra: dict, naturalezas: list) -> bool:
    if not naturalezas:
        return True
    texto = " ".join([
        _norm(obra.get("tipo_obra")), _norm(obra.get("nombre_proyecto")),
        _norm(obra.get("descripcion")), _norm(obra.get("alcance_detallado")),
        " ".join(_norm(x) for x in (obra.get("categorias_obra") or [])),
        " ".join(_norm(x) for x in (obra.get("especialidad") or [])),
    ])
    for nat in naturalezas:
        kws = _NATURALEZA_KEYWORDS.get(nat, [nat.replace("_", " ")])
        if any(_norm(kw) in texto for kw in kws):
            return True
    return False


# ── Evaluador: financieros ────────────────────────────────────

def _evaluar_financieros(eid: str, reqs: list, monto) -> dict:
    fin_rows = _sb.table("bid_financieros") \
        .select("periodo, fecha_cierre, activos_totales, pasivos_totales, "
                "activos_corrientes, pasivos_corrientes, patrimonio_neto, ingresos, "
                "solvencia, liquidez_corriente, capital_trabajo, endeudamiento") \
        .eq("empresa_id", eid) \
        .not_.is_("activos_totales", "null") \
        .order("fecha_cierre", desc=True) \
        .limit(2).execute().data or []
    fin = fin_rows[0] if fin_rows else None

    cap_rows = _sb.table("bid_capacidad_financiera") \
        .select("tipo, monto, monto_disponible, estado, fecha_vencimiento") \
        .eq("empresa_id", eid).execute().data or []
    hoy = datetime.utcnow().date().isoformat()
    vigentes = [r for r in cap_rows
                if (r.get("estado") or "vigente").lower() not in ("vencido", "cancelado", "inactivo")
                and (not r.get("fecha_vencimiento") or str(r["fecha_vencimiento"]) >= hoy)]
    lineas_total = sum(_num_or_none(r.get("monto_disponible")) or _num_or_none(r.get("monto")) or 0
                       for r in vigentes)

    def _indicador(row, tipo, req):
        if tipo == "solvencia":
            return _num_or_none(row.get("solvencia")), None
        if tipo == "liquidez":
            return _num_or_none(row.get("liquidez_corriente")), None
        if tipo == "capital_trabajo":
            base = _num_or_none(row.get("capital_trabajo")) or 0
            if req.get("incluye_lineas_credito"):
                return base + lineas_total, f"CT RD$ {base:,.2f} + lineas RD$ {lineas_total:,.2f}"
            return base, None
        if tipo == "endeudamiento":
            formula_txt = _norm(req.get("formula"))
            if "activo" in formula_txt:
                pt, at = _num_or_none(row.get("pasivos_totales")), _num_or_none(row.get("activos_totales"))
                return ((pt / at) if (pt is not None and at) else None), "Pasivo Total / Activo Total"
            return _num_or_none(row.get("endeudamiento")), "Pasivo Total / Patrimonio"
        if tipo == "patrimonio":
            return _num_or_none(row.get("patrimonio_neto")), None
        if tipo == "ingresos":
            return _num_or_none(row.get("ingresos")), None
        return None, None

    detalle, faltantes = [], []
    cumple_todos = True
    evaluables = 0

    for req in (reqs or []):
        tipo = (req.get("tipo") or "otro").lower()
        operador = req.get("operador") or ">="
        limite = _num_or_none(req.get("limite"))
        pct = _num_or_none(req.get("pct_monto"))
        base_calc = (req.get("base_calculo") or "ultimo").lower()

        limite_efectivo = limite
        limite_desc = f"{operador} {limite}" if limite is not None else ""
        if pct is not None and monto:
            limite_efectivo = pct * monto
            limite_desc = f"{operador} {pct*100:.0f}% del monto (RD$ {limite_efectivo:,.2f})"

        valor, valor_desc, periodo_desc = None, None, None
        if fin:
            if base_calc == "promedio" and len(fin_rows) >= 2:
                vals = []
                for row in fin_rows[:2]:
                    v, d = _indicador(row, tipo, req)
                    if v is not None:
                        vals.append(v)
                        valor_desc = d
                if vals:
                    valor = sum(vals) / len(vals)
                    periodo_desc = "promedio " + " y ".join(str(r.get("periodo")) for r in fin_rows[:2])
            else:
                valor, valor_desc = _indicador(fin, tipo, req)
                periodo_desc = str(fin.get("periodo"))
                if base_calc == "promedio" and len(fin_rows) < 2:
                    periodo_desc += " (pliego pide promedio; solo hay 1 periodo cargado)"

        cumple, faltante = None, None
        if valor is not None and limite_efectivo is not None:
            evaluables += 1
            cumple = _cmp(valor, operador, limite_efectivo)
            if cumple is False:
                cumple_todos = False
                if operador in (">", ">=", "mayor", "≥", "mayor_igual"):
                    faltante = limite_efectivo - valor
                nom = req.get("nombre") or tipo
                faltantes.append({
                    "seccion": "financieros",
                    "texto": f"{nom}: tienes {valor:,.2f}, el pliego pide {limite_desc}",
                    "subsanable": req.get("subsanable"),
                })

        detalle.append({
            "nombre": req.get("nombre"), "tipo": tipo, "formula": req.get("formula"),
            "base_calculo": base_calc, "periodo_desc": periodo_desc,
            "limite_desc": limite_desc or (req.get("notas") or "revisar pliego"),
            "valor": valor, "valor_desc": valor_desc,
            "cumple": cumple, "faltante": faltante,
            "subsanable": req.get("subsanable"),
            "texto_original": req.get("texto_original"),
            "notas": req.get("notas"),
        })

    return {
        "detalle": detalle,
        "cumple": (cumple_todos if evaluables else None),
        "faltantes": faltantes,
        "periodo_usado": fin.get("periodo") if fin else None,
        "sin_estados": fin is None,
        "lineas_credito_total": lineas_total,
    }


# ── Evaluador: lineas de credito (% del presupuesto/lote) ─────

def _evaluar_lineas_credito(eid: str, reqs: list, presupuesto, lotes: list) -> dict:
    if not reqs:
        return {"detalle": [], "cumple": None, "faltantes": [], "escenarios_lote": []}

    perfil = _sb.table("perfiles_empresa").select("clasificacion_mipyme") \
        .eq("id", eid).limit(1).execute().data or []
    es_mipyme = bool(perfil) and str(perfil[0].get("clasificacion_mipyme") or "").strip().upper() in ("SI", "SÍ", "S", "TRUE", "1")

    cap_rows = _sb.table("bid_capacidad_financiera") \
        .select("tipo, institucion_financiera, monto, monto_disponible, estado, fecha_vencimiento") \
        .eq("empresa_id", eid).execute().data or []
    hoy = datetime.utcnow().date().isoformat()

    def _suma(tipo_linea):
        total = 0.0
        for r in cap_rows:
            t = _norm(r.get("tipo"))
            estado = (r.get("estado") or "vigente").lower()
            if estado in ("vencido", "cancelado", "inactivo"):
                continue
            if r.get("fecha_vencimiento") and str(r["fecha_vencimiento"]) < hoy:
                continue
            es_com = any(k in t for k in ("comercial", "suplidor", "ferreteria"))
            if tipo_linea == "comercial" and not es_com:
                continue
            if tipo_linea == "bancaria" and es_com:
                continue
            total += _num_or_none(r.get("monto_disponible")) or _num_or_none(r.get("monto")) or 0
        return total

    escenarios = []
    detalle, faltantes = [], []
    cumple_global = True
    evaluado = False

    bases = []
    if lotes:
        for l in (lotes or []):
            p = _num_or_none(l.get("presupuesto"))
            bases.append((f"Lote {l.get('numero')}", p))
    if not bases:
        bases = [("Proceso completo", _num_or_none(presupuesto))]

    for etiqueta, base_monto in bases:
        esc = {"lote": etiqueta, "requisitos": [], "cumple": None}
        esc_cumple = True
        esc_eval = False
        for req in reqs:
            tipo_linea = (req.get("tipo") or "bancaria").lower()
            pct = _num_or_none(req.get("pct_mipyme") if es_mipyme else req.get("pct_grande"))
            disponible = _suma(tipo_linea)
            requerido = (pct * base_monto) if (pct is not None and base_monto) else None
            cumple = None
            if requerido is not None:
                esc_eval = True
                evaluado = True
                cumple = disponible >= requerido
                if not cumple:
                    esc_cumple = False
                    cumple_global = False
                    faltantes.append({
                        "seccion": "lineas_credito",
                        "texto": (f"Linea {tipo_linea} ({etiqueta}): necesitas RD$ {requerido:,.2f} "
                                  f"({pct*100:.0f}% {'MIPYME' if es_mipyme else ''}), "
                                  f"tienes RD$ {disponible:,.2f} vigente"),
                        "subsanable": req.get("subsanable"),
                    })
            esc["requisitos"].append({
                "tipo": tipo_linea, "pct_aplicado": pct, "es_mipyme": es_mipyme,
                "requerido": requerido, "disponible": disponible, "cumple": cumple,
                "texto_original": req.get("texto_original"),
            })
        esc["cumple"] = esc_cumple if esc_eval else None
        escenarios.append(esc)

    detalle = escenarios[0]["requisitos"] if len(escenarios) == 1 else []
    return {
        "detalle": detalle,
        "escenarios_lote": escenarios if len(escenarios) > 1 else [],
        "cumple": (cumple_global if evaluado else None),
        "faltantes": faltantes,
        "es_mipyme": es_mipyme,
    }


# ── Evaluador: personal (asignacion unica greedy) ─────────────

def _persona_elegible(p: dict, req: dict) -> tuple:
    """(elegible: bool, razones_no: list[str])"""
    razones = []
    titulaciones = req.get("titulaciones") or []
    if titulaciones:
        texto_perfil = " ".join([
            _norm(p.get("profesion")), _norm(p.get("formacion_academica")),
            " ".join(_norm(x) for x in (p.get("especialidades") or [])),
        ])
        toks_perfil = {_stem(t) for t in texto_perfil.split() if len(t) > 2}
        ok_titulo = False
        for t in titulaciones:
            toks_req = {_stem(x) for x in _norm(t).split() if len(x) > 2}
            if toks_req and toks_req.issubset(toks_perfil):
                ok_titulo = True
                break
        if not ok_titulo:
            razones.append("titulacion no coincide")

    anios_min = _num_or_none(req.get("anios_min"))
    anios_p = _anios_desde(p.get("fecha_graduacion"))
    if anios_p is None:
        anios_p = _num_or_none(p.get("experiencia_general_anios"))
    if anios_min is not None:
        if anios_p is None or anios_p < anios_min:
            razones.append(f"anos insuficientes ({anios_p if anios_p is not None else 'sin dato'} < {anios_min:g})")

    if req.get("requiere_maestria"):
        if not p.get("tiene_maestria"):
            razones.append("sin maestria")
        else:
            area = req.get("area_maestria")
            if area and "afin" not in _norm(area):
                toks_area = _tokens(area)
                toks_m = _tokens(p.get("maestria_descripcion"))
                # basta con que UN token del area aparezca (las areas vienen como
                # "hidraulica, sanitaria o afines")
                if toks_area and toks_m and not (toks_area & toks_m):
                    razones.append(f"maestria en otra area (pide {area})")

    if req.get("requiere_colegiatura") and not (p.get("codia") or "").strip():
        razones.append("sin CODIA")

    return (len(razones) == 0), razones, (anios_p or 0)


def _evaluar_personal(eid: str, reqs: list, lotes: list) -> dict:
    if not reqs:
        return {"detalle": [], "cumple": None, "faltantes": [], "asignaciones": []}

    personas = _sb.table("bid_personal") \
        .select("id, nombre_completo, profesion, formacion_academica, especialidades, "
                "experiencia_general_anios, experiencia_especifica_anios, codia, "
                "tiene_maestria, maestria_descripcion, fecha_graduacion, activo, disponible") \
        .eq("empresa_id", eid).execute().data or []
    personas = [p for p in personas if p.get("activo") is not False and p.get("disponible") is not False]

    # Expandir requisitos: cantidad N = N puestos; por_lote = un puesto por lote
    puestos = []
    n_lotes = max(1, len(lotes or []))
    for req in reqs:
        cantidad = int(_num_or_none(req.get("cantidad")) or 1)
        multiplicador = n_lotes if (req.get("por_lote") and n_lotes > 1) else 1
        for i in range(cantidad * multiplicador):
            puestos.append({"req": req, "n": i + 1})

    # Greedy: puestos mas exigentes primero (maestria > anos > colegiatura)
    def _exigencia(p):
        r = p["req"]
        return (1 if r.get("requiere_maestria") else 0,
                _num_or_none(r.get("anios_min")) or 0,
                1 if r.get("requiere_colegiatura") else 0)
    puestos.sort(key=_exigencia, reverse=True)

    usadas = set()
    asignaciones, faltantes, detalle_reqs = [], [], {}
    cumple_todos = True

    for puesto in puestos:
        req = puesto["req"]
        cargo = req.get("cargo") or "puesto"
        candidatos = []
        for p in personas:
            if p["id"] in usadas:
                continue
            ok, razones, anios = _persona_elegible(p, req)
            if ok:
                candidatos.append((anios, p))
        if candidatos:
            # el candidato con MENOS anos que igual cumple, para guardar a los
            # mas fuertes para puestos mas duros que vengan despues
            candidatos.sort(key=lambda c: c[0])
            anios_c, elegido = candidatos[0]
            usadas.add(elegido["id"])
            asignaciones.append({
                "cargo": cargo, "puesto_n": puesto["n"],
                "personal_id": elegido["id"], "nombre": elegido.get("nombre_completo"),
                "anios": anios_c,
            })
            detalle_reqs.setdefault(cargo, {"req": req, "cubiertos": 0, "total": 0})
            detalle_reqs[cargo]["cubiertos"] += 1
        else:
            cumple_todos = False
            partes = []
            if req.get("titulaciones"):
                partes.append(" o ".join(req["titulaciones"]))
            if req.get("anios_min"):
                partes.append(f"{req['anios_min']:g}+ anos desde el titulo")
            if req.get("requiere_maestria"):
                partes.append(f"maestria{' en ' + req['area_maestria'] if req.get('area_maestria') else ''}")
            if req.get("requiere_colegiatura"):
                partes.append("CODIA vigente")
            faltantes.append({
                "seccion": "personal",
                "texto": f"Falta 1 {cargo}: " + ", ".join(partes) if partes else f"Falta 1 {cargo}",
                "subsanable": req.get("subsanable"),
            })
        detalle_reqs.setdefault(cargo, {"req": req, "cubiertos": 0, "total": 0})
        detalle_reqs[cargo]["total"] += 1

    detalle = []
    checklist_docs = []
    for cargo, info in detalle_reqs.items():
        req = info["req"]
        detalle.append({
            "cargo": cargo,
            "puestos": info["total"], "cubiertos": info["cubiertos"],
            "cumple": info["cubiertos"] >= info["total"],
            "titulaciones": req.get("titulaciones"),
            "anios_min": req.get("anios_min"),
            "requiere_maestria": req.get("requiere_maestria"),
            "area_maestria": req.get("area_maestria"),
            "subsanable": req.get("subsanable"),
            "texto_original": req.get("texto_original"),
        })
        certs_min = _num_or_none(req.get("certificaciones_funcion_min"))
        if certs_min:
            checklist_docs.append(
                f"{cargo}: preparar {certs_min:g}+ certificaciones de obras desempenando esa funcion "
                f"(el pliego las exige como soporte)")

    return {
        "detalle": detalle,
        "asignaciones": asignaciones,
        "cumple": cumple_todos if puestos else None,
        "faltantes": faltantes,
        "checklist_documentos": checklist_docs,
    }


# ── Evaluador: equipos ────────────────────────────────────────

def _evaluar_equipos(eid: str, reqs: list) -> dict:
    if not reqs:
        return {"detalle": [], "cumple": None, "faltantes": []}

    equipos = _sb.table("bid_equipos") \
        .select("id, tipo, descripcion, cantidad, capacidad, anio, propiedad, activo") \
        .eq("empresa_id", eid).execute().data or []
    equipos = [e for e in equipos if e.get("activo") is not False]

    detalle, faltantes = [], []
    cumple_todos = True
    evaluado = False

    for req in reqs:
        canon_req = _canon_equipo(req.get("tipo"))
        cant_req = int(_num_or_none(req.get("cantidad")) or 1)
        cap_req, unidad_req = _parse_capacidad(req.get("capacidad_min"))
        anio_min = _num_or_none(req.get("anio_minimo"))
        admite_alq = req.get("admite_alquilado")
        admite_alq = True if admite_alq is None else bool(admite_alq)

        disponibles = 0
        revision = []
        for e in equipos:
            canon_e = _canon_equipo(e.get("tipo") or e.get("descripcion"))
            if not canon_e or canon_e != canon_req:
                # segundo intento: tokens del tipo requerido dentro de la descripcion
                toks_req = _tokens(canon_req)
                toks_e = _tokens((e.get("tipo") or "") + " " + (e.get("descripcion") or ""))
                if not (toks_req and toks_req.issubset(toks_e)):
                    continue
            if not admite_alq and _norm(e.get("propiedad")) not in ("propio", "propia"):
                continue
            if anio_min is not None and e.get("anio") and float(e["anio"]) < anio_min:
                continue
            if cap_req is not None:
                cap_e, unidad_e = _parse_capacidad(e.get("capacidad"))
                if cap_e is None or unidad_e != unidad_req:
                    revision.append(f"{e.get('descripcion') or e.get('tipo')}: capacidad "
                                    f"'{e.get('capacidad') or 'sin dato'}' no comparable con "
                                    f"'{req.get('capacidad_min')}' — verificar manual")
                    continue
                if cap_e < cap_req:
                    continue
            disponibles += int(_num_or_none(e.get("cantidad")) or 1)

        evaluado = True
        cumple = disponibles >= cant_req
        if not cumple:
            cumple_todos = False
            extra = f" ({req.get('capacidad_min')})" if req.get("capacidad_min") else ""
            propio_txt = "" if admite_alq else " PROPIOS (no admite alquilados)"
            faltantes.append({
                "seccion": "equipos",
                "texto": (f"Faltan {cant_req - disponibles} {req.get('tipo')}{extra}{propio_txt}: "
                          f"tienes {disponibles}, piden {cant_req}"),
                "subsanable": req.get("subsanable"),
            })

        detalle.append({
            "tipo": req.get("tipo"), "tipo_canonico": canon_req,
            "requerido": cant_req, "disponible": disponibles,
            "capacidad_min": req.get("capacidad_min"),
            "admite_alquilado": admite_alq, "cumple": cumple,
            "revision_manual": revision or None,
            "subsanable": req.get("subsanable"),
            "texto_original": req.get("texto_original"),
        })

    return {
        "detalle": detalle,
        "cumple": cumple_todos if evaluado else None,
        "faltantes": faltantes,
    }


# ── Evaluador: experiencia de la empresa ──────────────────────

def _evaluar_experiencia(eid: str, req: dict, presupuesto, lotes: list) -> dict:
    if not req:
        return {"detalle": None, "cumple": None, "faltantes": []}

    obras = _sb.table("bid_experiencia") \
        .select("id, nombre_proyecto, cliente, tipo_obra, descripcion, alcance_detallado, "
                "monto_contrato, monto_ejecutado, moneda, fecha_inicio, fecha_fin, estado, "
                "porcentaje_avance, categorias_obra, especialidad, volumenes_partidas, "
                "acta_recepcion_url, recepcion_definitiva_url, finiquito_url, "
                "certificacion_url, cubicaciones_url") \
        .eq("empresa_id", eid).execute().data or []

    naturalezas = req.get("naturaleza") or []
    en_curso_pct = _num_or_none(req.get("acepta_en_curso_pct"))
    requiere_acta = bool(req.get("requiere_acta"))
    ventana = _num_or_none(req.get("ventana_anios"))
    monto_min_obra = _num_or_none(req.get("monto_min_por_obra"))
    monto_min_acum = _num_or_none(req.get("monto_min_acumulado"))
    cant_min = _num_or_none(req.get("cantidad_obras_min"))
    pct_portafolio = _num_or_none(req.get("pct_portafolio"))
    criterio = (req.get("criterio_similitud") or "naturaleza").lower()

    hoy = datetime.utcnow()
    faltantes, notas = [], []
    calificadas = []

    for o in obras:
        estado = _norm(o.get("estado"))
        avance = _num_or_none(o.get("porcentaje_avance"))
        terminada = estado in ("terminado", "terminada", "concluido", "concluida",
                               "finalizado", "finalizada", "completado", "completada") \
            or (avance is not None and avance >= 100)
        razones = []

        if not terminada:
            if en_curso_pct is not None and avance is not None and avance >= en_curso_pct:
                if not o.get("cubicaciones_url"):
                    razones.append(f"en curso ({avance:g}%) pero sin cubicacion cargada para validarla")
            else:
                razones.append("no concluida" + (f" (avance {avance:g}% < {en_curso_pct:g}%)"
                                                 if (en_curso_pct is not None and avance is not None) else
                                                 " y el pliego no acepta obras en curso" if en_curso_pct is None else ""))

        if not _obra_matchea_naturaleza(o, naturalezas):
            razones.append("naturaleza distinta a la del proceso")

        if ventana is not None and o.get("fecha_fin"):
            try:
                ff = datetime.fromisoformat(str(o["fecha_fin"])[:10])
                if (hoy - ff).days > ventana * 365.25:
                    razones.append(f"fuera de la ventana de {ventana:g} anos")
            except ValueError:
                pass

        monto_o = _num_or_none(o.get("monto_ejecutado")) or _num_or_none(o.get("monto_contrato")) or 0
        if terminada is False and avance and en_curso_pct is not None:
            monto_o = monto_o * (avance / 100.0)  # magnitud proporcional a lo cubicado (patron MIVHED)
        if monto_min_obra is not None and monto_o < monto_min_obra:
            razones.append(f"monto RD$ {monto_o:,.2f} < minimo por obra RD$ {monto_min_obra:,.2f}")

        if requiere_acta:
            tiene_acta = any(o.get(k) for k in ("acta_recepcion_url", "recepcion_definitiva_url",
                                                "finiquito_url", "certificacion_url"))
            if not terminada:
                tiene_acta = tiene_acta or bool(o.get("cubicaciones_url"))
            if not tiene_acta:
                razones.append("sin acta de recepcion/finiquito cargada (el pliego la exige)")

        if not razones:
            calificadas.append({"obra": o, "monto": monto_o})
        else:
            notas.append({"obra": o.get("nombre_proyecto"), "descartada_por": razones})

    n_ok = len(calificadas)
    monto_acum = sum(c["monto"] for c in calificadas)

    checks = []
    cumple = True

    if cant_min is not None:
        ok = n_ok >= cant_min
        checks.append({"criterio": f"minimo {cant_min:g} obras similares",
                       "valor": n_ok, "cumple": ok})
        if not ok:
            cumple = False
            faltantes.append({
                "seccion": "experiencia",
                "texto": f"Faltan {int(cant_min - n_ok)} obras similares que califiquen: tienes {n_ok}, piden {cant_min:g}",
                "subsanable": req.get("subsanable"),
            })

    if monto_min_acum is not None:
        ok = monto_acum >= monto_min_acum
        checks.append({"criterio": f"monto acumulado >= RD$ {monto_min_acum:,.2f}",
                       "valor": monto_acum, "cumple": ok})
        if not ok:
            cumple = False
            faltantes.append({
                "seccion": "experiencia",
                "texto": (f"Monto acumulado insuficiente: tus obras calificadas suman RD$ {monto_acum:,.2f}, "
                          f"el pliego exige RD$ {monto_min_acum:,.2f} (faltan RD$ {monto_min_acum - monto_acum:,.2f})"),
                "subsanable": req.get("subsanable"),
            })

    if pct_portafolio is not None and obras:
        pct_real = 100.0 * n_ok / len(obras)
        ok = pct_real >= pct_portafolio
        checks.append({"criterio": f">= {pct_portafolio:g}% del portafolio en la naturaleza pedida",
                       "valor": round(pct_real, 1), "cumple": ok})
        if not ok:
            cumple = False
            faltantes.append({
                "seccion": "experiencia",
                "texto": (f"Solo {pct_real:.0f}% de tu experiencia es de la naturaleza pedida; "
                          f"el pliego exige minimo {pct_portafolio:g}%"),
                "subsanable": req.get("subsanable"),
            })

    escenarios_vol = []
    for v in (req.get("volumenes") or []):
        cat = _norm(v.get("categoria")).replace(" ", "_")
        unidad = v.get("unidad")
        cant_v = _num_or_none(v.get("cantidad"))
        if cant_v is None:
            continue
        total = 0.0
        for c in calificadas:
            vp = c["obra"].get("volumenes_partidas") or {}
            for k, val in (vp.items() if isinstance(vp, dict) else []):
                if _norm(k).replace(" ", "_").startswith(cat[:10]):
                    total += _num_or_none(val) or 0
        ok = total >= cant_v
        etiqueta = f"Lote {v['lote']}" if v.get("lote") else "Proceso"
        escenarios_vol.append({"lote": etiqueta, "categoria": v.get("categoria"),
                               "requerido": cant_v, "unidad": unidad,
                               "ejecutado": total, "cumple": ok})
        if not ok:
            # volumenes por lote son escenarios: solo tumban el cumple global si
            # el proceso no tiene lotes (si hay lotes, el usuario elige donde entrar)
            if not v.get("lote"):
                cumple = False
            faltantes.append({
                "seccion": "experiencia",
                "texto": (f"{etiqueta}: volumen de {v.get('categoria')} insuficiente "
                          f"({total:,.2f} de {cant_v:,.2f} {unidad or ''}) — "
                          f"carga los volumenes por partida de tus obras si los tienes"),
                "subsanable": req.get("subsanable"),
            })

    if criterio == "cantidad_intervenciones":
        notas.append({"nota": "El pliego mide la experiencia por cantidad de intervenciones "
                              "(ej. puntos de bacheo); revisar manualmente contra el texto original."})
        cumple = None if not checks else cumple

    evaluado = bool(checks or escenarios_vol)
    return {
        "criterio_similitud": criterio,
        "naturaleza": naturalezas,
        "obras_calificadas": n_ok,
        "monto_acumulado": monto_acum,
        "checks": checks,
        "escenarios_volumen": escenarios_vol or None,
        "obras_descartadas": notas or None,
        "cumple": (cumple if evaluado else None),
        "faltantes": faltantes,
        "texto_original": req.get("texto_original"),
    }


# ── Puntaje estimado (metodologia combinada) ──────────────────

def _estimar_puntaje(requisitos: dict, sec_personal: dict, sec_exp: dict) -> dict:
    blandos = requisitos.get("puntaje_blandos") or []
    pts_blandos = sum(_num_or_none(b.get("puntos_max")) or 0 for b in blandos)

    pts_duros, detalle_duros = 0.0, []

    # Personal: tramos de anos + maestria + certificaciones por cargo
    asignados = {a["cargo"]: a for a in (sec_personal.get("asignaciones") or [])}
    for req in (requisitos.get("personal") or []):
        pj = req.get("puntaje")
        if not pj:
            continue
        cargo = req.get("cargo")
        asig = asignados.get(cargo)
        obtenidos = 0.0
        maximo = _num_or_none(pj.get("puntos_max")) or 0
        if asig:
            anios = _num_or_none(asig.get("anios")) or 0
            for tramo in sorted(pj.get("tramos_anios") or [], key=lambda t: -(t.get("min") or 0)):
                if anios >= (_num_or_none(tramo.get("min")) or 0):
                    obtenidos += _num_or_none(tramo.get("puntos")) or 0
                    break
            if req.get("requiere_maestria") or pj.get("puntos_maestria"):
                # si fue asignado con requiere_maestria, ya la tiene
                obtenidos += _num_or_none(pj.get("puntos_maestria")) or 0
            # certificaciones por funcion: siguiendo la regla acordada, si cumple
            # anos se asume que cumple las certificaciones → tramo mas alto
            tramos_c = pj.get("tramos_certificaciones") or []
            if tramos_c:
                obtenidos += max(_num_or_none(t.get("puntos")) or 0 for t in tramos_c)
        pts_duros += min(obtenidos, maximo) if maximo else obtenidos
        detalle_duros.append({"criterio": f"Personal: {cargo}", "puntos_max": maximo,
                              "estimado": min(obtenidos, maximo) if maximo else obtenidos,
                              "asignado": bool(asig)})

    # Experiencia empresa: tramos por cantidad de certificaciones/obras
    exp_req = requisitos.get("experiencia") or {}
    pj = exp_req.get("puntaje")
    if pj:
        n_ok = sec_exp.get("obras_calificadas") or 0
        maximo = _num_or_none(pj.get("puntos_max")) or 0
        obtenidos = 0.0
        for tramo in sorted(pj.get("tramos_certificaciones") or [], key=lambda t: -(t.get("min") or 0)):
            if n_ok >= (_num_or_none(tramo.get("min")) or 0):
                obtenidos = _num_or_none(tramo.get("puntos")) or 0
                break
        pts_duros += min(obtenidos, maximo) if maximo else obtenidos
        detalle_duros.append({"criterio": "Experiencia como contratista", "puntos_max": maximo,
                              "estimado": obtenidos, "obras_calificadas": n_ok})

    total_max = _num_or_none(requisitos.get("puntaje_total"))
    minimo = _num_or_none(requisitos.get("puntaje_minimo"))
    estimado = pts_blandos + pts_duros

    return {
        "puntaje_total": total_max,
        "puntaje_minimo": minimo,
        "estimado": round(estimado, 2),
        "asumido_blandos": round(pts_blandos, 2),
        "verificado_duros": round(pts_duros, 2),
        "detalle_blandos": blandos,
        "detalle_duros": detalle_duros,
        "cumple_umbral": (estimado >= minimo) if minimo is not None else None,
        "nota": ("Los puntos 'blandos' (enfoque, planes, cronograma) se asumen completos: "
                 "dependen de la calidad de redaccion de tu oferta, no del expediente."),
    }


# ── Orquestador ───────────────────────────────────────────────

def _evaluar_matching(eid: str, requisitos, monto: float | None,
                      presupuesto: float | None = None) -> dict:
    """Evalua todos los requisitos del pliego contra el expediente.
    Retrocompatible: si 'requisitos' es una lista (analisis viejos, solo
    financieros), se envuelve como {"financieros": [...]}."""
    if isinstance(requisitos, list):
        requisitos = {"financieros": requisitos}
    requisitos = requisitos or {}

    lotes = requisitos.get("lotes") or []
    base_pct = presupuesto if presupuesto is not None else monto

    sec_fin = _evaluar_financieros(eid, requisitos.get("financieros") or [], monto)
    sec_lin = _evaluar_lineas_credito(eid, requisitos.get("lineas_credito") or [], base_pct, lotes)
    sec_per = _evaluar_personal(eid, requisitos.get("personal") or [], lotes)
    sec_eq = _evaluar_equipos(eid, requisitos.get("equipos") or [])
    sec_exp = _evaluar_experiencia(eid, requisitos.get("experiencia") or None, base_pct, lotes)

    faltantes = (sec_fin.get("faltantes", []) + sec_lin.get("faltantes", []) +
                 sec_per.get("faltantes", []) + sec_eq.get("faltantes", []) +
                 sec_exp.get("faltantes", []))

    secciones = {
        "financieros": sec_fin, "lineas_credito": sec_lin,
        "personal": sec_per, "equipos": sec_eq, "experiencia": sec_exp,
    }
    estados = [s.get("cumple") for s in secciones.values()]
    evaluadas = [e for e in estados if e is not None]
    cumple_global = (all(evaluadas) if evaluadas else None)

    metodologia = (requisitos.get("metodologia") or "cumple_no_cumple").lower()
    puntaje = None
    if metodologia == "combinada":
        puntaje = _estimar_puntaje(requisitos, sec_per, sec_exp)
        if puntaje.get("cumple_umbral") is False:
            cumple_global = False
            faltantes.append({
                "seccion": "puntaje",
                "texto": (f"Puntaje estimado {puntaje['estimado']:g}/{puntaje.get('puntaje_total') or '?'} "
                          f"por debajo del minimo de {puntaje['puntaje_minimo']:g} puntos, "
                          f"aun asumiendo los criterios de redaccion completos"),
                "subsanable": False,
            })

    otros = requisitos.get("otros") or []

    return {
        "evaluado_en": datetime.utcnow().isoformat(),
        "version_evaluador": 2,
        "metodologia": metodologia,
        "naturaleza_proceso": requisitos.get("naturaleza_proceso"),
        "lotes": lotes or None,
        "monto_usado": monto,
        "cumple_global": cumple_global,
        "puntaje": puntaje,
        "faltantes": faltantes,
        "secciones": secciones,
        "otros_requisitos": otros or None,
        "checklist_documentos": sec_per.get("checklist_documentos") or None,
        # Campos legado para que el frontend actual no se rompa hasta actualizarlo:
        "cumple_financiero": sec_fin.get("cumple"),
        "detalle": sec_fin.get("detalle"),
        "periodo_usado": sec_fin.get("periodo_usado"),
        "lineas_credito_total": sec_fin.get("lineas_credito_total"),
        "sin_estados": sec_fin.get("sin_estados"),
    }


@bid_manager_router.post("/matching/analizar")
async def matching_analizar(
    file: UploadFile = File(...),
    monto_ofertado: str = Form(None),
    authorization: str | None = Header(default=None),
):
    """Sube el pliego, extrae requisitos (financieros + tecnicos) con IA,
    guarda y evalua contra el expediente completo."""
    uid = _auth(authorization)
    eid = _empresa_id(uid)

    contenido = await file.read()
    if not contenido:
        raise HTTPException(400, "Archivo vacío")
    if len(contenido) > STORAGE_MAX_BYTES:
        raise HTTPException(413, "El pliego excede 25 MB")

    # Extraer con Gemini
    if not GEMINI_API_KEY:
        raise HTTPException(503, "Extracción IA no configurada (falta GEMINI_API_KEY)")
    payload = {
        "contents": [{"parts": [
            {"inline_data": {"mime_type": "application/pdf",
                             "data": base64.b64encode(contenido).decode()}},
            {"text": _PROMPT_MATCHING},
        ]}],
        "generationConfig": {"temperature": 0.1, "response_mime_type": "application/json"},
    }
    url = (f"https://generativelanguage.googleapis.com/v1beta/models/"
           f"{GEMINI_MODEL}:generateContent?key={GEMINI_API_KEY}")
    try:
        req = _urlreq.Request(url, data=_json.dumps(payload).encode(),
                              headers={"Content-Type": "application/json"}, method="POST")
        with _urlreq.urlopen(req, timeout=180) as resp:
            body = _json.loads(resp.read().decode())
        texto = body["candidates"][0]["content"]["parts"][0]["text"].strip()
        if texto.startswith("```"):
            texto = texto.strip("`")
            if texto.lower().startswith("json"):
                texto = texto[4:]
        extraido = _json.loads(texto)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(502, f"Error extrayendo requisitos del pliego: {str(e)}")

    # Subir pliego al storage
    safe_name = _sanitize_filename(file.filename or "pliego.pdf")
    path = f"{eid}/matching/{safe_name}"
    try:
        _sb.storage.from_(STORAGE_BUCKET).upload(
            path=path, file=contenido,
            file_options={"content-type": "application/pdf", "upsert": "false"})
    except Exception:
        path = None  # no bloquear el análisis por fallo de storage

    presupuesto = None
    try:
        presupuesto = float(extraido.get("presupuesto_base")) if extraido.get("presupuesto_base") else None
    except (TypeError, ValueError):
        pass

    monto = None
    try:
        monto = float(monto_ofertado) if monto_ofertado else None
    except (TypeError, ValueError):
        pass
    if monto is None:
        monto = presupuesto

    # El prompt v2 devuelve requisitos como objeto de secciones; metodologia,
    # puntajes y lotes vienen al nivel raiz y se anidan dentro de requisitos
    # para que reevaluar los tenga disponibles.
    requisitos = extraido.get("requisitos") or {}
    if isinstance(requisitos, list):
        requisitos = {"financieros": requisitos}
    for campo in ("metodologia", "puntaje_total", "puntaje_minimo",
                  "naturaleza_proceso", "lotes"):
        if extraido.get(campo) is not None and campo not in requisitos:
            requisitos[campo] = extraido[campo]

    resultado = _evaluar_matching(eid, requisitos, monto, presupuesto)

    registro = {
        "empresa_id": eid,
        "referencia": extraido.get("referencia"),
        "nombre_proceso": extraido.get("nombre_proceso"),
        "institucion": extraido.get("institucion"),
        "presupuesto_base": presupuesto,
        "monto_ofertado": monto,
        "anios_estados_requeridos": extraido.get("anios_estados_requeridos"),
        "requisitos": requisitos,
        "resultado": resultado,
        "pliego_url": path,
    }
    res = _sb.table("bid_matching_pliegos").insert(registro).execute()
    return res.data[0] if res.data else registro


@bid_manager_router.get("/matching")
def matching_listar(authorization: str | None = Header(default=None)):
    uid = _auth(authorization)
    eid = _empresa_id(uid)
    return _sb.table("bid_matching_pliegos").select("*") \
        .eq("empresa_id", eid).order("creado_en", desc=True).execute().data or []


@bid_manager_router.post("/matching/{id}/reevaluar")
async def matching_reevaluar(id: str, request: dict = None,
                             authorization: str | None = Header(default=None)):
    """Recalcula el matching con los datos ACTUALES del expediente.
    Body opcional: {"monto_ofertado": 123456.78}"""
    uid = _auth(authorization)
    eid = _empresa_id(uid)
    reg = _obtener("bid_matching_pliegos", id, eid)

    monto = None
    if request and request.get("monto_ofertado") is not None:
        try:
            monto = float(request["monto_ofertado"])
        except (TypeError, ValueError):
            monto = None
    if monto is None:
        monto = float(reg.get("monto_ofertado") or reg.get("presupuesto_base") or 0) or None

    presupuesto = None
    try:
        presupuesto = float(reg.get("presupuesto_base")) if reg.get("presupuesto_base") else None
    except (TypeError, ValueError):
        pass

    resultado = _evaluar_matching(eid, reg.get("requisitos") or {}, monto, presupuesto)
    upd = {"resultado": resultado, "actualizado_en": datetime.utcnow().isoformat()}
    if monto is not None:
        upd["monto_ofertado"] = monto
    res = _sb.table("bid_matching_pliegos").update(upd) \
        .eq("id", id).eq("empresa_id", eid).execute()
    return res.data[0] if res.data else {**reg, **upd}


@bid_manager_router.delete("/matching/{id}")
def matching_eliminar(id: str, authorization: str | None = Header(default=None)):
    uid = _auth(authorization)
    return _eliminar("bid_matching_pliegos", id, _empresa_id(uid))
