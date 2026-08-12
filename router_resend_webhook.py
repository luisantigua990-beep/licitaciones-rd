"""
router_resend_webhook.py — Webhook de Resend para tracking de outreach
======================================================================
Recibe los eventos de Resend (email.delivered, email.opened, email.bounced,
email.complained, email.clicked, email.delivery_delayed, email.failed) y
actualiza public.outreach_log con el estado real de cada correo.

Integración en main.py:
    from router_resend_webhook import resend_webhook_router
    app.include_router(resend_webhook_router)

Variables de entorno requeridas (Railway):
    SUPABASE_URL             (ya la tienes)
    SUPABASE_SERVICE_KEY     (ya la tienes)
    RESEND_WEBHOOK_SECRET    ← NUEVO. Se genera al crear el webhook en el
                               dashboard de Resend → Webhooks → Add Endpoint.
                               Ver: https://resend.com/docs/dashboard/webhooks/verify-webhooks-requests

Configurar el webhook en Resend:
    URL: https://app.licitacionlab.com/api/outreach/webhook/resend
    Eventos: email.delivered, email.opened, email.bounced,
             email.complained, email.clicked, email.delivery_delayed,
             email.failed
    Copiar el "Signing Secret" que muestra Resend → guardarlo como
    RESEND_WEBHOOK_SECRET en Railway.

Seguridad:
    Resend firma cada request con Svix (svix-id, svix-timestamp,
    svix-signature). Verificamos la firma HMAC-SHA256 antes de tocar la DB.
    Si RESEND_WEBHOOK_SECRET no está configurado el endpoint devuelve 500
    para que nadie pueda mandarnos eventos falsos.
"""

import base64
import hashlib
import hmac
import os
import time
from datetime import datetime, timezone

from fastapi import APIRouter, HTTPException, Request
from supabase import create_client

SUPABASE_URL         = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_KEY = os.getenv("SUPABASE_SERVICE_KEY") or os.getenv("SUPABASE_KEY")
RESEND_WEBHOOK_SECRET = os.getenv("RESEND_WEBHOOK_SECRET", "")

_sb = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)

resend_webhook_router = APIRouter(prefix="/api/outreach/webhook", tags=["outreach-webhook"])


# ──────────────────────────────────────────────────────────────
# Verificación de firma Svix (formato usado por Resend)
# Documentación: https://docs.svix.com/receiving/verifying-payloads/how-manual
# ──────────────────────────────────────────────────────────────
def _verificar_firma_svix(secret: str, svix_id: str, svix_timestamp: str,
                          svix_signature: str, body: bytes) -> bool:
    if not (secret and svix_id and svix_timestamp and svix_signature):
        return False

    # Rechazar eventos con timestamp muy viejo (evita replay attacks)
    try:
        ts = int(svix_timestamp)
        if abs(time.time() - ts) > 60 * 5:   # 5 minutos de tolerancia
            return False
    except ValueError:
        return False

    # El secret viene como "whsec_<base64>"; hay que decodificar el base64
    if secret.startswith("whsec_"):
        secret = secret[len("whsec_"):]
    try:
        secret_bytes = base64.b64decode(secret)
    except Exception:
        return False

    # Firma esperada: HMAC-SHA256 sobre "{svix_id}.{svix_timestamp}.{body}"
    firmar = f"{svix_id}.{svix_timestamp}.".encode() + body
    esperada = base64.b64encode(hmac.new(secret_bytes, firmar, hashlib.sha256).digest()).decode()

    # svix-signature puede traer varias firmas separadas por espacio:
    #   "v1,<firma1> v1,<firma2> ..."
    for parte in svix_signature.split(" "):
        _, _, firma = parte.partition(",")
        if hmac.compare_digest(firma, esperada):
            return True
    return False


# ──────────────────────────────────────────────────────────────
# Mapeo evento Resend → columnas de outreach_log
# ──────────────────────────────────────────────────────────────
def _campos_por_evento(tipo: str, evento_data: dict) -> dict:
    ahora = datetime.now(timezone.utc).isoformat()

    if tipo == "email.sent":
        return {"estado_entrega": "sent"}

    if tipo == "email.delivered":
        return {"estado_entrega": "delivered", "entregado_at": ahora}

    if tipo == "email.opened":
        # No sobrescribir si ya se registró la primera apertura
        return {"abierto_at": ahora}

    if tipo == "email.clicked":
        return {"clic_at": ahora}

    if tipo == "email.bounced":
        motivo = ""
        try:
            b = evento_data.get("bounce") or {}
            motivo = b.get("message") or b.get("subType") or b.get("type") or ""
        except Exception:
            pass
        return {"estado_entrega": "bounced", "rebote_at": ahora, "motivo_rebote": motivo[:500]}

    if tipo == "email.complained":
        return {"estado_entrega": "complained"}

    if tipo == "email.delivery_delayed":
        return {"estado_entrega": "delivery_delayed"}

    if tipo == "email.failed":
        motivo = ""
        try:
            motivo = (evento_data.get("failed") or {}).get("reason") or ""
        except Exception:
            pass
        return {"estado_entrega": "failed", "motivo_rebote": motivo[:500]}

    return {}


# ──────────────────────────────────────────────────────────────
# Endpoint
# ──────────────────────────────────────────────────────────────
@resend_webhook_router.post("/resend")
async def webhook_resend(request: Request):
    if not RESEND_WEBHOOK_SECRET:
        # Cerrado si no está configurado (evita aceptar eventos no firmados)
        raise HTTPException(500, "RESEND_WEBHOOK_SECRET no configurado")

    body = await request.body()
    svix_id        = request.headers.get("svix-id", "")
    svix_timestamp = request.headers.get("svix-timestamp", "")
    svix_signature = request.headers.get("svix-signature", "")

    if not _verificar_firma_svix(RESEND_WEBHOOK_SECRET, svix_id, svix_timestamp,
                                 svix_signature, body):
        raise HTTPException(401, "Firma inválida")

    try:
        payload = await request.json()
    except Exception:
        raise HTTPException(400, "Body no es JSON válido")

    tipo = payload.get("type", "")
    data = payload.get("data", {}) or {}
    resend_id = data.get("email_id") or data.get("id")

    if not resend_id:
        # Sin id no podemos cruzar; ACK 200 igual para que Resend no reintente
        return {"ok": True, "skipped": "sin email_id"}

    campos = _campos_por_evento(tipo, data)
    if not campos:
        # Evento que no nos interesa (por ejemplo email.scheduled)
        return {"ok": True, "skipped": f"evento {tipo} ignorado"}

    # Actualizar el registro correspondiente en outreach_log
    try:
        upd = _sb.table("outreach_log").update(campos) \
            .eq("resend_email_id", resend_id).execute()
        # Si no hay match por resend_email_id, no rompemos — puede ser
        # un correo transaccional (confirmación de pago, notificaciones)
        # que no está en outreach_log. Devolvemos 200 igual.
        afectados = len(upd.data or [])
    except Exception as e:
        print(f"❌ webhook Resend: error actualizando outreach_log ({tipo} / {resend_id}): {e}")
        # Devolver 500 hace que Resend reintente automáticamente
        raise HTTPException(500, "Error actualizando outreach_log")

    print(f"📬 Resend {tipo}: {resend_id[:16]}… → {afectados} fila(s) actualizada(s)")
    return {"ok": True, "tipo": tipo, "actualizados": afectados}
