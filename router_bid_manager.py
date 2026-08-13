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

from fastapi import APIRouter, HTTPException, Header, Query
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

@bid_manager_router.post("/socios")
async def crear_socio(request: dict, authorization: str | None = Header(default=None)):
    uid = _auth(authorization)
    return _crear("bid_socios", request, _empresa_id(uid))

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
    return _actualizar("bid_socios", id, request, _empresa_id(uid))

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
