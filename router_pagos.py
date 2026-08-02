"""
router_pagos.py — Pagos con Pagadito Connect para LicitacionLab
================================================================
Sigue el mismo patrón que router_agentes: router autosuficiente.

Integración en main.py (2 líneas, junto a agentes_router):
    from router_pagos import pagos_router
    app.include_router(pagos_router)

Variables de entorno nuevas en Railway:
    PAGADITO_UID=<uid de 32 caracteres>
    PAGADITO_WSK=<wsk de 32 caracteres>
    PAGADITO_SANDBOX=true        # cambiar a false en producción
    CRON_SECRET=<cualquier string secreto>
    RESEND_API_KEY=<ya la tienes configurada para otros emails>
    RESEND_FROM=LicitacionLab <notificaciones@licitacionlab.com>

Seguridad: /crear y /verificar requieren el access_token de Supabase
(el mismo session.access_token que ya tiene la PWA) en el header
Authorization: Bearer <token>. El backend valida el JWT contra Supabase,
así NADIE puede crear pagos ni consultar pagos de otro usuario.
"""

import os
import time
import requests
from datetime import datetime, timedelta, timezone

from fastapi import APIRouter, HTTPException, Request, Header
from fastapi.responses import RedirectResponse
from pydantic import BaseModel
from supabase import create_client

from pagadito import (
    PagaditoClient, PagaditoError,
    ESTADOS_FINALES_OK, ESTADOS_FINALES_FALLO, ESTADOS_EN_PROCESO,
)

# ── Clientes propios (mismo patrón que main.py) ──────────────────
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
SUPABASE_SERVICE_KEY = os.getenv("SUPABASE_SERVICE_KEY", SUPABASE_KEY)
RESEND_API_KEY = os.getenv("RESEND_API_KEY", "")
FROM_EMAIL = os.getenv("RESEND_FROM", "LicitacionLab <notificaciones@licitacionlab.com>")

_sb_admin = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)  # escribe pagos/suscripciones (bypassa RLS)
_pg = PagaditoClient()

pagos_router = APIRouter(prefix="/api/pagos", tags=["pagos"])


# ── Auth: validar el JWT de Supabase que envía la PWA ─────────────
def _user_id_desde_token(authorization: str | None) -> str:
    """Valida 'Authorization: Bearer <access_token>' contra Supabase y devuelve el user_id."""
    if not authorization or not authorization.lower().startswith("bearer "):
        raise HTTPException(401, "Falta el token de sesión")
    token = authorization.split(" ", 1)[1].strip()
    try:
        user_resp = _sb_admin.auth.get_user(token)
        if not user_resp or not user_resp.user:
            raise HTTPException(401, "Sesión inválida o expirada")
        return user_resp.user.id
    except HTTPException:
        raise
    except Exception:
        raise HTTPException(401, "Sesión inválida o expirada")


class CrearPagoRequest(BaseModel):
    plan_id: int


# ══════════════════════════════════════════════════════════════
# 0) PLANES — para que la pricing page los lea del backend
# ══════════════════════════════════════════════════════════════
@pagos_router.get("/planes")
def listar_planes():
    res = _sb_admin.table("planes").select("id,nombre,precio,moneda,duracion_meses") \
        .eq("activo", True).order("duracion_meses").execute()
    return {"planes": res.data or []}


# ══════════════════════════════════════════════════════════════
# 1) CREAR PAGO — registra la transacción y devuelve URL de Pagadito
# ══════════════════════════════════════════════════════════════
@pagos_router.post("/crear")
def crear_pago(body: CrearPagoRequest, authorization: str | None = Header(default=None)):
    user_id = _user_id_desde_token(authorization)

    plan_q = _sb_admin.table("planes").select("*").eq("id", body.plan_id).eq("activo", True).execute()
    if not plan_q.data:
        raise HTTPException(404, "Plan no encontrado")
    plan = plan_q.data[0]

    # ─── Descuento primer mes para usuarios referidos (solo plan mensual) ───
    monto_final = float(plan["precio"])
    descripcion_extra = ""
    if plan["duracion_meses"] == 1:
        es_referido = _sb_admin.table("referrals").select("id") \
            .eq("referred_user_id", user_id).execute().data
        if es_referido:
            pagos_previos = _sb_admin.table("pagos").select("id") \
                .eq("user_id", user_id).eq("estado", "COMPLETED").execute().data
            if not pagos_previos:
                cfg_q = _sb_admin.table("referral_config").select("descuento_referido_pct").eq("id", 1).execute()
                desc_pct = cfg_q.data[0]["descuento_referido_pct"] if cfg_q.data else 50
                monto_final = round(monto_final * (100 - desc_pct) / 100, 2)
                descripcion_extra = f" · {desc_pct}% OFF referido"

    ern = f"LL-{user_id[:8]}-{int(time.time() * 1000)}"

    pago_ins = _sb_admin.table("pagos").insert({
        "user_id": user_id,
        "plan_id": plan["id"],
        "ern": ern,
        "monto": monto_final,
        "moneda": plan["moneda"],
        "estado": "PENDIENTE",
    }).execute()
    pago_id = pago_ins.data[0]["id"]

    try:
        resp = _pg.exec_trans(
            ern=ern,
            amount=monto_final,
            currency=plan["moneda"],
            details=[{
                "quantity": 1,
                "description": f"LicitacionLab — Plan {plan['nombre']} ({plan['duracion_meses']} {'mes' if plan['duracion_meses']==1 else 'meses'}){descripcion_extra}",
                "price": monto_final,
            }],
        )
    except PagaditoError as e:
        _sb_admin.table("pagos").update({"estado": "ERROR", "detalle_error": str(e)}).eq("id", pago_id).execute()
        print(f"❌ Pagadito exec-trans falló: {e}")
        raise HTTPException(502, f"Error de Pagadito: {e.code}")

    token = resp["data"]["token"]
    url_pago = resp["data"]["url"]
    _sb_admin.table("pagos").update({"token_pagadito": token}).eq("id", pago_id).execute()

    print(f"💳 Pago creado: {ern} | user {user_id[:8]} | plan {plan['nombre']} | ${monto_final}{descripcion_extra}")
    return {"url_pago": url_pago, "pago_id": pago_id, "ern": ern, "monto": monto_final, "descuento_aplicado": bool(descripcion_extra)}


# ══════════════════════════════════════════════════════════════
# 2) RETORNO — Pagadito redirige aquí al usuario después del pago
#    Configurar en panel Pagadito (Configuración Técnica → URL de retorno):
#    https://app.licitacionlab.com/api/pagos/retorno?token={value}&ern={ern_value}
# ══════════════════════════════════════════════════════════════
@pagos_router.get("/retorno")
def retorno_pago(request: Request):
    qp = request.query_params
    token = qp.get("token") or qp.get("value")
    ern = qp.get("ern") or qp.get("ern_value") or qp.get("comprobante")

    if not token and ern:
        # Plan B: recuperar el token guardado al crear el pago
        q = _sb_admin.table("pagos").select("token_pagadito").eq("ern", ern).execute()
        if q.data and q.data[0]["token_pagadito"]:
            token = q.data[0]["token_pagadito"]

    if not token:
        return RedirectResponse("/?pago=error")

    resultado = _verificar_y_activar(token)
    return RedirectResponse(f"/?pago={resultado}")


# ══════════════════════════════════════════════════════════════
# 3) VERIFICAR — el frontend consulta el estado de su pago
# ══════════════════════════════════════════════════════════════
@pagos_router.get("/verificar/{pago_id}")
def verificar_pago(pago_id: int, authorization: str | None = Header(default=None)):
    user_id = _user_id_desde_token(authorization)
    q = _sb_admin.table("pagos").select("*").eq("id", pago_id).eq("user_id", user_id).execute()
    if not q.data:
        raise HTTPException(404, "Pago no encontrado")
    pago = q.data[0]
    if pago["estado"] == "COMPLETED":
        return {"estado": "exitoso"}
    if not pago["token_pagadito"]:
        return {"estado": "error"}
    return {"estado": _verificar_y_activar(pago["token_pagadito"])}


# ══════════════════════════════════════════════════════════════
# 4) MI SUSCRIPCIÓN — estado actual del usuario
# ══════════════════════════════════════════════════════════════
@pagos_router.get("/mi-suscripcion")
def mi_suscripcion(authorization: str | None = Header(default=None)):
    user_id = _user_id_desde_token(authorization)
    q = _sb_admin.table("suscripciones").select("*, planes(nombre)") \
        .eq("user_id", user_id).eq("activa", True) \
        .order("fecha_vencimiento", desc=True).limit(1).execute()
    if not q.data:
        return {"activa": False}
    sub = q.data[0]
    vence = datetime.fromisoformat(sub["fecha_vencimiento"])
    vigente = vence > datetime.now(timezone.utc)
    return {
        "activa": vigente,
        "plan": (sub.get("planes") or {}).get("nombre"),
        "fecha_vencimiento": sub["fecha_vencimiento"],
        "dias_restantes": max(0, (vence - datetime.now(timezone.utc)).days),
    }


# ══════════════════════════════════════════════════════════════
# Lógica central: get-status + activación de suscripción
# ══════════════════════════════════════════════════════════════
def _verificar_y_activar(token: str) -> str:
    """Devuelve: 'exitoso' | 'fallido' | 'pendiente' | 'error'"""
    try:
        resp = _pg.get_status(token)
    except PagaditoError as e:
        print(f"⚠️ get-status falló para token {token[:12]}...: {e}")
        return "error"

    estado = resp["data"]["status"]
    referencia = resp["data"].get("reference")
    fecha_trans = resp["data"].get("date_trans")

    q = _sb_admin.table("pagos").select("*").eq("token_pagadito", token).execute()
    if not q.data:
        return "error"
    pago = q.data[0]

    # Idempotencia: un pago COMPLETED no se activa dos veces
    if pago["estado"] == "COMPLETED":
        return "exitoso"

    _sb_admin.table("pagos").update({
        "estado": estado,
        "referencia_pg": referencia,
        "fecha_transaccion": fecha_trans,
    }).eq("id", pago["id"]).execute()
    pago["estado"] = estado
    pago["referencia_pg"] = referencia
    pago["fecha_transaccion"] = fecha_trans

    if estado in ESTADOS_FINALES_OK:
        _activar_suscripcion(pago)
        print(f"✅ Pago COMPLETED {pago['ern']} ref {referencia} — suscripción activada")
        return "exitoso"
    if estado in ESTADOS_FINALES_FALLO:
        print(f"❌ Pago {estado}: {pago['ern']}")
        return "fallido"
    print(f"⏳ Pago en proceso ({estado}): {pago['ern']}")
    return "pendiente"


def _enviar_confirmacion_pago(pago: dict, plan: dict):
    """
    Envía el email de confirmación de compra exigido por Pagadito (observación
    de certificación: debe incluir el Número de Aprobación / referencia).
    No lanza excepción si falla — un email caído no debe tumbar la activación.
    """
    if not RESEND_API_KEY:
        print("   ⚠️ RESEND_API_KEY no configurada — email de confirmación omitido")
        return

    try:
        user_resp = _sb_admin.auth.admin.get_user_by_id(pago["user_id"])
        to_email = user_resp.user.email
    except Exception as e:
        print(f"   ⚠️ No se pudo obtener email del usuario {pago['user_id'][:8]}: {e}")
        return

    if not to_email:
        return

    fecha = pago.get("fecha_transaccion") or datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    html = f"""
    <div style="font-family: Arial, sans-serif; max-width: 480px; margin: 0 auto; color: #1a2420;">
      <h2 style="color: #1a2420;">¡Pago confirmado!</h2>
      <p>Gracias por tu compra en <strong>LicitacionLab</strong>. Aquí el detalle de tu transacción:</p>
      <table style="width: 100%; border-collapse: collapse; margin: 16px 0;">
        <tr><td style="padding: 6px 0; color: #555;">Plan</td><td style="padding: 6px 0; text-align: right;"><strong>{plan['nombre']}</strong></td></tr>
        <tr><td style="padding: 6px 0; color: #555;">Monto</td><td style="padding: 6px 0; text-align: right;"><strong>{pago['moneda']} {pago['monto']}</strong></td></tr>
        <tr><td style="padding: 6px 0; color: #555;">Fecha</td><td style="padding: 6px 0; text-align: right;">{fecha}</td></tr>
        <tr><td style="padding: 6px 0; color: #555;">Referencia (ERN)</td><td style="padding: 6px 0; text-align: right;">{pago['ern']}</td></tr>
        <tr><td style="padding: 6px 0; color: #555;">Número de Aprobación Pagadito</td><td style="padding: 6px 0; text-align: right;"><strong>{pago.get('referencia_pg', '')}</strong></td></tr>
      </table>
      <p style="font-size: 13px; color: #888;">Conserva este correo como comprobante de tu pago.</p>
    </div>
    """

    try:
        resp = requests.post(
            "https://api.resend.com/emails",
            headers={"Authorization": f"Bearer {RESEND_API_KEY}", "Content-Type": "application/json"},
            json={"from": FROM_EMAIL, "to": [to_email], "subject": "Confirmación de pago — LicitacionLab", "html": html},
            timeout=20,
        )
        if resp.status_code in (200, 201):
            print(f"   ✉️ Email de confirmación enviado a {to_email}")
        else:
            print(f"   ⚠️ Resend error {resp.status_code}: {resp.text[:150]}")
    except Exception as e:
        print(f"   ⚠️ Resend error: {e}")


def _activar_suscripcion(pago: dict):
    plan = _sb_admin.table("planes").select("*").eq("id", pago["plan_id"]).execute().data[0]
    ahora = datetime.now(timezone.utc)

    sub_q = _sb_admin.table("suscripciones").select("*") \
        .eq("user_id", pago["user_id"]).eq("activa", True).execute()

    inicio = ahora
    if sub_q.data:
        venc_actual = datetime.fromisoformat(sub_q.data[0]["fecha_vencimiento"])
        if venc_actual > ahora:
            inicio = venc_actual  # renovación anticipada: EXTIENDE, no pisa
        _sb_admin.table("suscripciones").update({"activa": False}).eq("id", sub_q.data[0]["id"]).execute()

    vencimiento = inicio + timedelta(days=30 * plan["duracion_meses"])

    _sb_admin.table("suscripciones").insert({
        "user_id": pago["user_id"],
        "plan_id": plan["id"],
        "pago_id": pago["id"],
        "fecha_inicio": inicio.isoformat(),
        "fecha_vencimiento": vencimiento.isoformat(),
        "activa": True,
    }).execute()

    # ─── Programa de Referidos: comisión al referidor ───
    try:
        _procesar_comision_referido(pago)
    except Exception as e:
        print(f"⚠️ Error procesando comisión de referido: {e}")

    # ─── Programa Fundadores: inscribir si hay cupos ───
    try:
        _procesar_fundador(pago, plan)
    except Exception as e:
        print(f"⚠️ Error procesando fundador: {e}")

    # ─── Proceso gratis por referido (solo si NO quedó como fundador) ───
    try:
        _procesar_proceso_referido(pago)
    except Exception as e:
        print(f"⚠️ Error procesando proceso de referido: {e}")

    _enviar_confirmacion_pago(pago, plan)


def _procesar_proceso_referido(pago: dict):
    """Da 1 proceso de bienes/servicios gratis al referido en su primer pago.
    REGLA: si ya es fundador, NO se suma (solo cuenta el beneficio de fundador).
    Solo aplica una vez, en el primer pago del referido."""
    user_id = pago["user_id"]

    # ¿Este usuario fue referido?
    ref = _sb_admin.table("referrals").select("id, proceso_gratis_otorgado") \
        .eq("referred_user_id", user_id).execute().data
    if not ref:
        return
    referral = ref[0]

    # Solo una vez
    if referral.get("proceso_gratis_otorgado"):
        return

    # Traer perfil para ver si ya es fundador
    perfil = _sb_admin.table("user_profiles").select(
        "is_founder, free_processes_total, free_processes_used"
    ).eq("id", user_id).execute().data
    if not perfil:
        return
    p = perfil[0]

    # REGLA: si ya es fundador, no se suma. Solo marcamos el referral como procesado.
    if p.get("is_founder"):
        _sb_admin.table("referrals").update({"proceso_gratis_otorgado": True}) \
            .eq("id", referral["id"]).execute()
        print(f"ℹ️ Referido {user_id[:8]} ya es fundador — proceso por referido NO se suma")
        return

    # No es fundador → otorgar 1 proceso gratis de bienes/servicios
    cfg = _sb_admin.table("referral_config").select("proceso_gratis_referido").eq("id", 1).execute().data
    n = cfg[0]["proceso_gratis_referido"] if cfg else 1

    _sb_admin.table("user_profiles").update({
        "founder_type": "referido_bs",  # marca que es beneficio de referido (bienes/servicios)
        "free_processes_total": (p.get("free_processes_total") or 0) + n,
    }).eq("id", user_id).execute()

    _sb_admin.table("referrals").update({"proceso_gratis_otorgado": True}) \
        .eq("id", referral["id"]).execute()
    print(f"🎁 Referido {user_id[:8]} recibe {n} proceso(s) gratis de bienes/servicios")


def _procesar_comision_referido(pago: dict):
    """Genera comisión al referidor cuando su referido paga.
    Modelo híbrido: bono de bienvenida (1 sola vez, al primer pago) + recurrente.
    Tasas y bono leídos desde referral_config (auditable, no hardcodeado).
    Seguridad: valida no-auto-referido, idempotencia por pago_id, bono 1 sola vez."""
    ref = _sb_admin.table("referrals").select("*") \
        .eq("referred_user_id", pago["user_id"]).execute().data
    if not ref:
        return

    referral = ref[0]
    referrer_id = referral["referrer_id"]

    # GUARD 1: nunca auto-referido (defensa en profundidad; la DB también lo bloquea)
    if referrer_id == pago["user_id"]:
        print(f"⚠️ Auto-referido bloqueado: {referrer_id[:8]}")
        return

    # GUARD 2: idempotencia — si este pago ya generó comisión RECURRENTE, no duplicar
    ya = _sb_admin.table("referral_commissions").select("id") \
        .eq("pago_id", pago["id"]).eq("tipo", "recurrente").execute().data
    if ya:
        print(f"⚠️ Pago {pago['id']} ya tiene comisión recurrente, se omite")
        return

    # Config de comisiones desde la DB
    cfg_q = _sb_admin.table("referral_config").select("*").eq("id", 1).execute()
    cfg = cfg_q.data[0] if cfg_q.data else {
        "bono_bienvenida": 10.0, "tasa_pago": 15, "tasa_free": 10
    }

    es_primer_pago = referral["status"] == "pending"

    # Activar el referral la primera vez que el referido paga
    if es_primer_pago:
        _sb_admin.table("referrals").update({
            "status": "active",
            "activated_at": datetime.now(timezone.utc).isoformat(),
        }).eq("id", referral["id"]).execute()

    # ¿El referidor es suscriptor activo? → define la tasa recurrente
    sub = _sb_admin.table("suscripciones").select("id, fecha_vencimiento") \
        .eq("user_id", referrer_id).eq("activa", True).execute()
    es_pro = False
    if sub.data:
        venc = datetime.fromisoformat(sub.data[0]["fecha_vencimiento"])
        es_pro = venc > datetime.now(timezone.utc)
    tasa = cfg["tasa_pago"] if es_pro else cfg["tasa_free"]

    periodo = datetime.now(timezone.utc).strftime("%Y-%m")

    # ─── BONO DE BIENVENIDA: solo en el primer pago del referido y 1 sola vez ───
    if es_primer_pago and not referral.get("bono_pagado"):
        bono = round(float(cfg["bono_bienvenida"]), 2)
        if bono > 0:
            _sb_admin.table("referral_commissions").insert({
                "referral_id": referral["id"],
                "referrer_id": referrer_id,
                "amount": bono,
                "rate": 0,
                "period": periodo,
                "pago_id": pago["id"],
                "tipo": "bono",
                "status": "pending",
            }).execute()
            _sb_admin.table("referrals").update({"bono_pagado": True}) \
                .eq("id", referral["id"]).execute()
            print(f"🎁 Bono bienvenida US${bono} → referidor {referrer_id[:8]}")

    # ─── COMISIÓN RECURRENTE ───
    comision = round(float(pago["monto"]) * tasa / 100, 2)
    try:
        _sb_admin.table("referral_commissions").insert({
            "referral_id": referral["id"],
            "referrer_id": referrer_id,
            "amount": comision,
            "rate": tasa,
            "period": periodo,
            "pago_id": pago["id"],
            "tipo": "recurrente",
            "status": "pending",
        }).execute()
        print(f"💰 Comisión referido: US${comision} ({tasa}%) → referidor {referrer_id[:8]}")
    except Exception as e:
        # Si el UNIQUE(pago_id) rebota (bono ya usó ese pago_id), registrar recurrente sin romper
        print(f"⚠️ Comisión recurrente no insertada (posible constraint pago_id): {e}")


def _procesar_fundador(pago: dict, plan: dict):
    """Inscribe al usuario en el Programa Fundadores si hay cupos.
    Beneficios: Mensual=1 proceso B&S | Semestral=3 B&S o 1 plan constr. | Anual=5 B&S o 1 Sobre A."""
    perfil = _sb_admin.table("user_profiles").select("is_founder") \
        .eq("id", pago["user_id"]).execute().data
    if perfil and perfil[0].get("is_founder"):
        return  # ya es fundador, no consume otro cupo

    config_q = _sb_admin.table("founder_config").select("*").eq("id", 1).execute()
    if not config_q.data:
        return
    config = config_q.data[0]
    if not config["active"] or config["used_slots"] >= config["max_slots"]:
        return  # programa cerrado o cupos agotados

    duracion = plan["duracion_meses"]
    if duracion == 1:
        free_total = 1   # 1 proceso B&S (construcción: nada)
    elif duracion == 6:
        free_total = 3   # 3 procesos B&S o 1 plan de construcción
    elif duracion == 12:
        free_total = 5   # 5 procesos B&S o 1 Sobre A de construcción
    else:
        return

    _sb_admin.table("user_profiles").update({
        "is_founder": True,
        "founder_type": "por_definir",  # Lonny lo clasifica al contactar al cliente
        "free_processes_total": free_total,
        "free_processes_used": 0,
        "founder_enrolled_at": datetime.now(timezone.utc).isoformat(),
    }).eq("id", pago["user_id"]).execute()

    _sb_admin.table("founder_config").update({
        "used_slots": config["used_slots"] + 1,
    }).eq("id", 1).execute()
    print(f"🏆 Fundador #{config['used_slots'] + 1}/30 inscrito: user {pago['user_id'][:8]} | plan {plan['nombre']} | {free_total} procesos gratis")


# ══════════════════════════════════════════════════════════════
# 5) CRON — re-verifica pagos en VERIFYING/REGISTERED/PENDING
#    Conéctalo a n8n cada 30-60 min con header X-Cron-Secret
# ══════════════════════════════════════════════════════════════
@pagos_router.post("/cron/reverificar")
def cron_reverificar(request: Request):
    cron_secret = os.getenv("CRON_SECRET", "")
    if not cron_secret:
        # Sin CRON_SECRET configurado en Railway el endpoint queda cerrado
        # (evita que un header vacío pase la comparación "" == "")
        raise HTTPException(500, "CRON_SECRET no configurado")
    if request.headers.get("X-Cron-Secret") != cron_secret:
        raise HTTPException(403, "No autorizado")

    pendientes = _sb_admin.table("pagos").select("token_pagadito") \
        .in_("estado", list(ESTADOS_EN_PROCESO)) \
        .not_.is_("token_pagadito", "null").execute()

    resultados = {"revisados": 0, "completados": 0}
    for p in (pendientes.data or []):
        resultados["revisados"] += 1
        if _verificar_y_activar(p["token_pagadito"]) == "exitoso":
            resultados["completados"] += 1
    return resultados
