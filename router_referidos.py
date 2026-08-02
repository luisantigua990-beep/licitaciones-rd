"""
router_referidos.py — Programa de Referidos y Fundadores para LicitacionLab
============================================================================
Integración en main.py (2 líneas, junto a pagos_router):
    from router_referidos import referidos_router
    app.include_router(referidos_router)

Reglas de negocio:
- Todo usuario (free o pago) recibe un referral_code al registrarse (trigger en Supabase).
- Referidor free: gana 15% recurrente. Solo cobra como crédito; retiro efectivo desde US$60.
- Referidor de pago: gana 20% recurrente. Crédito o transferencia desde US$40.
- Al pasar de free a pago, TODOS sus referidos (viejos y nuevos) generan 20%.
  (La tasa se evalúa en el momento de cada pago del referido, así que esto es automático.)
- El referido recibe 50% off su primer mes (se aplica en router_pagos.crear_pago).

Programa Fundadores (30 cupos):
- Mensual  → 1 proceso gratis (bienes y servicios)
- Semestral → 3 procesos B&S o 1 plan de construcción
- Anual    → 5 procesos B&S o 1 Sobre A de construcción (tamaño razonable)
"""

import os
from datetime import datetime, timezone

from fastapi import APIRouter, HTTPException, Header
from pydantic import BaseModel
from supabase import create_client

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
SUPABASE_SERVICE_KEY = os.getenv("SUPABASE_SERVICE_KEY", SUPABASE_KEY)
APP_URL = os.getenv("APP_URL", "https://app.licitacionlab.com")

_sb = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)

referidos_router = APIRouter(prefix="/api/referidos", tags=["referidos"])


def _config():
    """Lee la config de comisiones desde la DB (auditable, no hardcodeada)."""
    q = _sb.table("referral_config").select("*").eq("id", 1).execute()
    if q.data:
        return q.data[0]
    return {
        "bono_bienvenida": 10.0, "tasa_pago": 15, "tasa_free": 10,
        "descuento_referido_pct": 50, "umbral_retiro_pago": 40.0, "umbral_retiro_free": 60.0,
    }


def _audit(user_id: str, accion: str, detalle: dict | None = None):
    """Registra acciones sensibles para auditoría antifraude."""
    try:
        _sb.table("referral_audit").insert({
            "user_id": user_id,
            "accion": accion,
            "detalle": detalle or {},
        }).execute()
    except Exception:
        pass


# ── Auth helper (mismo patrón que router_pagos) ──────────────────
def _user_id_desde_token(authorization: str | None) -> str:
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


def _es_suscriptor_activo(user_id: str) -> bool:
    q = _sb.table("suscripciones").select("id, fecha_vencimiento") \
        .eq("user_id", user_id).eq("activa", True).execute()
    if not q.data:
        return False
    venc = datetime.fromisoformat(q.data[0]["fecha_vencimiento"])
    return venc > datetime.now(timezone.utc)


def _email_parcial(email: str) -> str:
    """j***@gmail.com — privacidad del referido."""
    try:
        local, dominio = email.split("@", 1)
        return f"{local[0]}***@{dominio}"
    except Exception:
        return "***"


# ══════════════════════════════════════════════════════════════
# 1) MI CÓDIGO — código y link de referido del usuario
# ══════════════════════════════════════════════════════════════
@referidos_router.get("/mi-codigo")
def mi_codigo(authorization: str | None = Header(default=None)):
    user_id = _user_id_desde_token(authorization)
    q = _sb.table("user_profiles").select("referral_code").eq("id", user_id).execute()
    if not q.data:
        raise HTTPException(404, "Perfil no encontrado")
    code = q.data[0]["referral_code"]
    return {
        "code": code,
        "link": f"{APP_URL}/?ref={code}",
        "mensaje_whatsapp": (
            f"Mira, estoy usando LicitacionLab para monitorear las licitaciones "
            f"del Estado en tiempo real. Regístrate con mi link y te preparan un "
            f"proceso de licitación GRATIS en tu primer mes: {APP_URL}/?ref={code}"
        ),
    }


# ══════════════════════════════════════════════════════════════
# 2) MIS REFERIDOS — lista con status y comisiones
# ══════════════════════════════════════════════════════════════
@referidos_router.get("/mis-referidos")
def mis_referidos(authorization: str | None = Header(default=None)):
    user_id = _user_id_desde_token(authorization)

    refs = _sb.table("referrals").select("*") \
        .eq("referrer_id", user_id).order("created_at", desc=True).execute().data or []

    resultado = []
    for r in refs:
        # Email parcial del referido
        email = "***"
        try:
            u = _sb.auth.admin.get_user_by_id(r["referred_user_id"])
            if u and u.user and u.user.email:
                email = _email_parcial(u.user.email)
        except Exception:
            pass

        # Comisión total generada por este referido
        coms = _sb.table("referral_commissions").select("amount") \
            .eq("referral_id", r["id"]).execute().data or []
        total_com = sum(float(c["amount"]) for c in coms)

        resultado.append({
            "email": email,
            "status": r["status"],
            "fecha_registro": r["created_at"],
            "comision_generada": round(total_com, 2),
        })

    activos = sum(1 for r in refs if r["status"] == "active")
    return {
        "referidos": resultado,
        "total_referidos": len(refs),
        "activos": activos,
    }


# ══════════════════════════════════════════════════════════════
# 3) BALANCE — comisiones pendientes, tasa y umbrales
# ══════════════════════════════════════════════════════════════
@referidos_router.get("/balance")
def balance(authorization: str | None = Header(default=None)):
    user_id = _user_id_desde_token(authorization)
    es_pro = _es_suscriptor_activo(user_id)
    cfg = _config()

    coms = _sb.table("referral_commissions").select("amount, status") \
        .eq("referrer_id", user_id).execute().data or []

    pendiente = sum(float(c["amount"]) for c in coms if c["status"] == "pending")
    cobrado = sum(float(c["amount"]) for c in coms if c["status"] in ("paid", "credited"))
    total = pendiente + cobrado

    umbral = float(cfg["umbral_retiro_pago"]) if es_pro else float(cfg["umbral_retiro_free"])

    return {
        "balance_pendiente": round(pendiente, 2),
        "total_ganado": round(total, 2),
        "total_cobrado": round(cobrado, 2),
        "tasa_actual": cfg["tasa_pago"] if es_pro else cfg["tasa_free"],
        "bono_bienvenida": float(cfg["bono_bienvenida"]),
        "es_suscriptor": es_pro,
        "umbral_efectivo": umbral,
        "puede_retirar_efectivo": pendiente >= umbral,
        "puede_usar_credito": pendiente > 0,
    }


# ══════════════════════════════════════════════════════════════
# 4) SOLICITAR COBRO — crédito o transferencia
# ══════════════════════════════════════════════════════════════
class SolicitudCobro(BaseModel):
    tipo: str  # "credito" | "transferencia"
    datos_bancarios: str | None = None  # banco + cuenta, solo para transferencia


@referidos_router.post("/solicitar-cobro")
def solicitar_cobro(body: SolicitudCobro, authorization: str | None = Header(default=None)):
    user_id = _user_id_desde_token(authorization)
    es_pro = _es_suscriptor_activo(user_id)

    if body.tipo not in ("credito", "transferencia"):
        raise HTTPException(400, "Tipo debe ser 'credito' o 'transferencia'")

    coms_pend = _sb.table("referral_commissions").select("id, amount") \
        .eq("referrer_id", user_id).eq("status", "pending").execute().data or []
    pendiente = sum(float(c["amount"]) for c in coms_pend)

    if pendiente <= 0:
        raise HTTPException(400, "No tienes balance pendiente")

    umbral = UMBRAL_EFECTIVO_PRO if es_pro else UMBRAL_EFECTIVO_FREE

    if body.tipo == "transferencia":
        if pendiente < umbral:
            raise HTTPException(
                400,
                f"Necesitas al menos US${umbral:.0f} para retirar en efectivo. "
                f"Tu balance actual: US${pendiente:.2f}",
            )
        # Usar datos bancarios del body, o los guardados en el perfil
        datos = body.datos_bancarios
        if not datos:
            perfil = _sb.table("user_profiles").select(
                "bank_name, bank_account_number, bank_account_holder"
            ).eq("id", user_id).execute().data
            if perfil and perfil[0].get("bank_account_number"):
                p = perfil[0]
                datos = f"{p['bank_name']} - {p['bank_account_number']} - {p['bank_account_holder']}"
        if not datos:
            raise HTTPException(400, "Registra tu cuenta bancaria antes de solicitar transferencia")
        body.datos_bancarios = datos

    # Marcar comisiones según el tipo
    nuevo_status = "credited" if body.tipo == "credito" else "paid"
    ids = [c["id"] for c in coms_pend]
    for cid in ids:
        _sb.table("referral_commissions").update({
            "status": nuevo_status,
            "paid_at": datetime.now(timezone.utc).isoformat(),
        }).eq("id", cid).execute()

    # Registrar la solicitud en cron_log para que Lonny la vea/procese
    detalle = f"COBRO REFERIDOS [{body.tipo.upper()}] user={user_id[:8]} monto=US${pendiente:.2f}"
    if body.datos_bancarios:
        detalle += f" | banco: {body.datos_bancarios[:120]}"
    try:
        _sb.table("cron_log").insert({
            "job": "cobro_referidos",
            "detalle": detalle,
            "status": "pendiente",
            "ejecutado_at": datetime.now(timezone.utc).isoformat(),
        }).execute()
    except Exception:
        pass  # el log no debe tumbar la operación

    _audit(user_id, "cobro_solicitado", {
        "tipo": body.tipo, "monto": round(pendiente, 2),
        "num_comisiones": len(ids),
    })

    return {
        "ok": True,
        "tipo": body.tipo,
        "monto": round(pendiente, 2),
        "mensaje": (
            "Tu crédito quedó registrado y se aplicará a tu próxima suscripción."
            if body.tipo == "credito"
            else "Solicitud recibida. Procesamos transferencias una vez al mes."
        ),
    }


# ══════════════════════════════════════════════════════════════
# 9) CUENTA BANCARIA — para depósito de comisiones
# ══════════════════════════════════════════════════════════════
class CuentaBancaria(BaseModel):
    bank_name: str
    account_number: str
    account_holder: str
    account_type: str | None = None  # ahorros / corriente
    document: str | None = None  # cédula/RNC del titular


@referidos_router.get("/cuenta-bancaria")
def obtener_cuenta_bancaria(authorization: str | None = Header(default=None)):
    user_id = _user_id_desde_token(authorization)
    q = _sb.table("user_profiles").select(
        "bank_name, bank_account_number, bank_account_holder, bank_account_type, bank_document"
    ).eq("id", user_id).execute()
    if not q.data:
        return {"tiene_cuenta": False}
    p = q.data[0]
    tiene = bool(p.get("bank_account_number"))
    return {
        "tiene_cuenta": tiene,
        "bank_name": p.get("bank_name") or "",
        "account_number": p.get("bank_account_number") or "",
        "account_holder": p.get("bank_account_holder") or "",
        "account_type": p.get("bank_account_type") or "",
        "document": p.get("bank_document") or "",
    }


@referidos_router.post("/cuenta-bancaria")
def guardar_cuenta_bancaria(body: CuentaBancaria, authorization: str | None = Header(default=None)):
    user_id = _user_id_desde_token(authorization)
    if not body.bank_name.strip() or not body.account_number.strip() or not body.account_holder.strip():
        raise HTTPException(400, "Banco, número de cuenta y titular son obligatorios")
    _sb.table("user_profiles").update({
        "bank_name": body.bank_name.strip(),
        "bank_account_number": body.account_number.strip(),
        "bank_account_holder": body.account_holder.strip(),
        "bank_account_type": (body.account_type or "").strip() or None,
        "bank_document": (body.document or "").strip() or None,
    }).eq("id", user_id).execute()
    return {"ok": True, "mensaje": "Cuenta bancaria guardada. La usaremos para depositar tus comisiones."}


# ══════════════════════════════════════════════════════════════
# 5) FUNDADORES — estado público de cupos
# ══════════════════════════════════════════════════════════════
@referidos_router.get("/fundadores/estado")
def fundadores_estado():
    q = _sb.table("founder_config").select("*").eq("id", 1).execute()
    if not q.data:
        return {"activo": False}
    cfg = q.data[0]
    disponibles = max(0, cfg["max_slots"] - cfg["used_slots"])
    return {
        "activo": cfg["active"] and disponibles > 0,
        "cupos_total": cfg["max_slots"],
        "cupos_usados": cfg["used_slots"],
        "cupos_disponibles": disponibles,
    }


# ══════════════════════════════════════════════════════════════
# 6) MI ESTADO FUNDADOR — beneficios del usuario
# ══════════════════════════════════════════════════════════════
@referidos_router.get("/fundadores/mi-estado")
def mi_estado_fundador(authorization: str | None = Header(default=None)):
    user_id = _user_id_desde_token(authorization)
    q = _sb.table("user_profiles") \
        .select("is_founder, founder_type, free_processes_total, free_processes_used, founder_enrolled_at") \
        .eq("id", user_id).execute()
    if not q.data or not q.data[0]["is_founder"]:
        return {"is_founder": False}
    p = q.data[0]
    return {
        "is_founder": True,
        "founder_type": p["founder_type"],
        "procesos_gratis_total": p["free_processes_total"],
        "procesos_gratis_usados": p["free_processes_used"],
        "procesos_gratis_restantes": max(0, p["free_processes_total"] - p["free_processes_used"]),
        "inscrito_en": p["founder_enrolled_at"],
    }


# ══════════════════════════════════════════════════════════════
# 7) REGISTRAR REFERIDO — llamado post-signup si había ?ref=
# ══════════════════════════════════════════════════════════════
class RegistroReferido(BaseModel):
    ref_code: str


@referidos_router.post("/registrar")
def registrar_referido(body: RegistroReferido, authorization: str | None = Header(default=None)):
    user_id = _user_id_desde_token(authorization)
    ref_code = (body.ref_code or "").strip().upper()
    if not ref_code:
        return {"ok": False, "msg": "Código vacío"}

    # Buscar referidor por código
    referrer = _sb.table("user_profiles").select("id") \
        .eq("referral_code", ref_code).execute()
    if not referrer.data:
        return {"ok": False, "msg": "Código no válido"}

    referrer_id = referrer.data[0]["id"]

    # GUARD 1: no auto-referido
    if referrer_id == user_id:
        _audit(user_id, "auto_referido_bloqueado", {"ref_code": ref_code})
        return {"ok": False, "msg": "No puedes referirte a ti mismo"}

    # GUARD 2: ¿ya tiene referidor?
    existing = _sb.table("referrals").select("id") \
        .eq("referred_user_id", user_id).execute()
    if existing.data:
        return {"ok": False, "msg": "Ya tienes un referidor asignado"}

    # GUARD 3: el referido NO puede ser una cuenta que ya pagó antes
    # (evita que usuarios existentes se vinculen a un referidor solo para generar bono)
    pagos_previos = _sb.table("pagos").select("id") \
        .eq("user_id", user_id).eq("estado", "COMPLETED").execute().data
    if pagos_previos:
        _audit(user_id, "referido_rechazado_ya_pago", {"ref_code": ref_code})
        return {"ok": False, "msg": "Solo cuentas nuevas pueden vincularse a un referidor"}

    _sb.table("referrals").insert({
        "referrer_id": referrer_id,
        "referred_user_id": user_id,
        "referral_code": ref_code,
        "status": "pending",
    }).execute()

    _sb.table("user_profiles").update({
        "referred_by": referrer_id,
        "source": "referral",
    }).eq("id", user_id).execute()

    _audit(user_id, "referido_registrado", {"referrer_id": referrer_id, "ref_code": ref_code})
    return {"ok": True, "msg": "Referido registrado. Te prepararemos un proceso de bienes/servicios GRATIS en tu primer mes."}


# ══════════════════════════════════════════════════════════════
# 11) ANÁLISIS GRATIS — gancho de conversión para freemium
# ══════════════════════════════════════════════════════════════
class AnalisisGratis(BaseModel):
    codigo_proceso: str


@referidos_router.get("/analisis-gratis/estado")
def estado_analisis_gratis(authorization: str | None = Header(default=None)):
    """Indica si el usuario free todavía tiene su análisis de cortesía disponible."""
    user_id = _user_id_desde_token(authorization)
    # Los suscriptores tienen análisis ilimitado, no aplica
    if _es_suscriptor_activo(user_id):
        return {"aplica": False, "es_suscriptor": True}
    q = _sb.table("user_profiles").select("free_analysis_used, free_analysis_proceso") \
        .eq("id", user_id).execute()
    usado = bool(q.data and q.data[0].get("free_analysis_used"))
    return {
        "aplica": True,
        "es_suscriptor": False,
        "disponible": not usado,
        "usado": usado,
        "proceso_analizado": q.data[0].get("free_analysis_proceso") if q.data else None,
    }


@referidos_router.post("/analisis-gratis/reclamar")
def reclamar_analisis_gratis(body: AnalisisGratis, authorization: str | None = Header(default=None)):
    """Marca el análisis gratis como usado y autoriza UNO. El análisis real y el
    envío de correo lo dispara main.py; aquí solo controlamos el límite de forma
    atómica del lado servidor (el frontend no puede saltarse esto)."""
    user_id = _user_id_desde_token(authorization)
    codigo = (body.codigo_proceso or "").strip()
    if not codigo:
        raise HTTPException(400, "Falta el código del proceso")

    # Suscriptores no consumen el gratis (tienen ilimitado)
    if _es_suscriptor_activo(user_id):
        return {"ok": True, "es_suscriptor": True, "autorizado": True}

    # Verificar que no lo haya usado ya
    q = _sb.table("user_profiles").select("free_analysis_used").eq("id", user_id).execute()
    if q.data and q.data[0].get("free_analysis_used"):
        raise HTTPException(403, "Ya usaste tu análisis gratis. Suscríbete para análisis ilimitados.")

    # Marcar como usado ANTES de disparar (previene doble uso por doble clic)
    _sb.table("user_profiles").update({
        "free_analysis_used": True,
        "free_analysis_proceso": codigo,
        "free_analysis_at": datetime.now(timezone.utc).isoformat(),
    }).eq("id", user_id).execute()

    _audit(user_id, "analisis_gratis_reclamado", {"proceso": codigo})
    return {"ok": True, "autorizado": True, "es_suscriptor": False}


# ══════════════════════════════════════════════════════════════
# 8) ADMIN — dashboard de fundadores para Lonny (protegido)
# ══════════════════════════════════════════════════════════════
ADMIN_SECRET = os.getenv("ADMIN_SECRET", "")


@referidos_router.get("/admin/fundadores")
def admin_fundadores(x_admin_key: str = Header(default=None)):
    """Lista de fundadores con procesos pendientes de preparar."""
    if not ADMIN_SECRET or x_admin_key != ADMIN_SECRET:
        raise HTTPException(403, "Acceso denegado")

    fundadores = _sb.table("user_profiles").select("*") \
        .eq("is_founder", True).order("founder_enrolled_at").execute().data or []

    resultado = []
    for f in fundadores:
        email = "?"
        try:
            u = _sb.auth.admin.get_user_by_id(f["id"])
            if u and u.user:
                email = u.user.email
        except Exception:
            pass
        resultado.append({
            "email": email,
            "founder_type": f["founder_type"],
            "procesos_total": f["free_processes_total"],
            "procesos_usados": f["free_processes_used"],
            "pendientes": max(0, f["free_processes_total"] - f["free_processes_used"]),
            "inscrito": f["founder_enrolled_at"],
        })

    return {"fundadores": resultado, "total": len(resultado)}


@referidos_router.post("/admin/marcar-proceso")
def admin_marcar_proceso(payload: dict, x_admin_key: str = Header(default=None)):
    """Marca un proceso gratis como usado. Body: {"email": "..."}"""
    if not ADMIN_SECRET or x_admin_key != ADMIN_SECRET:
        raise HTTPException(403, "Acceso denegado")

    email = payload.get("email", "").strip().lower()
    if not email:
        raise HTTPException(400, "Falta el email")

    # Buscar user por email
    user_id = None
    try:
        users = _sb.auth.admin.list_users()
        for u in users:
            if u.email and u.email.lower() == email:
                user_id = u.id
                break
    except Exception:
        pass

    if not user_id:
        raise HTTPException(404, "Usuario no encontrado")

    q = _sb.table("user_profiles").select("free_processes_total, free_processes_used") \
        .eq("id", user_id).execute()
    if not q.data:
        raise HTTPException(404, "Perfil no encontrado")

    p = q.data[0]
    if p["free_processes_used"] >= p["free_processes_total"]:
        raise HTTPException(400, "Este usuario ya usó todos sus procesos gratis")

    _sb.table("user_profiles").update({
        "free_processes_used": p["free_processes_used"] + 1,
    }).eq("id", user_id).execute()

    return {
        "ok": True,
        "procesos_usados": p["free_processes_used"] + 1,
        "restantes": p["free_processes_total"] - p["free_processes_used"] - 1,
    }


@referidos_router.get("/admin/comisiones-pendientes")
def admin_comisiones_pendientes(x_admin_key: str = Header(default=None)):
    """Resumen de comisiones por pagar, agrupado por referidor."""
    if not ADMIN_SECRET or x_admin_key != ADMIN_SECRET:
        raise HTTPException(403, "Acceso denegado")

    coms = _sb.table("referral_commissions").select("referrer_id, amount, status") \
        .eq("status", "pending").execute().data or []

    por_usuario: dict[str, float] = {}
    for c in coms:
        por_usuario[c["referrer_id"]] = por_usuario.get(c["referrer_id"], 0) + float(c["amount"])

    resultado = []
    for uid, monto in sorted(por_usuario.items(), key=lambda x: -x[1]):
        email = "?"
        try:
            u = _sb.auth.admin.get_user_by_id(uid)
            if u and u.user:
                email = u.user.email
        except Exception:
            pass
        resultado.append({"email": email, "balance_pendiente": round(monto, 2)})

    return {"comisiones": resultado, "total_pendiente": round(sum(por_usuario.values()), 2)}
