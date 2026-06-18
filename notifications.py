import os
import json
import re
from pywebpush import webpush, WebPushException

VAPID_PRIVATE_KEY = os.getenv("VAPID_PRIVATE_KEY")
VAPID_PUBLIC_KEY  = os.getenv("VAPID_PUBLIC_KEY")
VAPID_EMAIL       = os.getenv("VAPID_EMAIL", "mailto:luisantigua990@gmail.com")


def enviar_notificacion(subscription_info: dict, titulo: str, cuerpo: str, url: str = "/"):
    """
    Envía notificación push web.
    Retorna:
      True  — enviado OK
      False — error general (loguea el detalle)
      "410" — suscripción expirada/eliminada (hay que desactivarla en BD)
    """
    if not VAPID_PRIVATE_KEY:
        print("❌ VAPID_PRIVATE_KEY no configurada en Railway")
        return False

    try:
        data = json.dumps({
            "title": titulo,
            "body": cuerpo,
            "url": url
        })
        webpush(
            subscription_info=subscription_info,
            data=data,
            vapid_private_key=VAPID_PRIVATE_KEY,
            vapid_claims={"sub": VAPID_EMAIL}
        )
        return True

    except WebPushException as e:
        resp   = getattr(e, "response", None)
        status = getattr(resp, "status_code", 0) if resp else 0
        body   = ""
        if resp:
            try:
                body = resp.text[:200]
            except Exception:
                pass

        # ── FALLBACK: si pywebpush no popula status_code, parsearlo del mensaje ──
        # El mensaje típico es: "Push failed: 410 Gone" o "Push failed: 404 Not Found"
        if status == 0:
            msg_str = str(e)
            m = re.search(r"Push failed:\s*(\d{3})", msg_str)
            if m:
                status = int(m.group(1))

        # ── Y si tampoco, detectar por contenido del body ──
        if status == 0 and body:
            if '"reason":"Unregistered"' in body or '"reason":"ExpiredSubscription"' in body:
                status = 410

        print(f"❌ WebPushException [{status}] endpoint={subscription_info.get('endpoint','')[:60]}: {str(e)[:150]} | resp: {body}")

        if status in (404, 410):
            return "410"
        # 400 con VapidPkHashMismatch = claves VAPID rotadas, suscripción inválida
        if status == 400 and "VapidPkHashMismatch" in body:
            return "410"
        # Gone en el mensaje aunque no haya status code
        if "Gone" in str(e) or "Unregistered" in body:
            return "410"
        return False

    except Exception as e:
        import traceback
        print(f"❌ Error inesperado en push: {type(e).__name__}: {str(e)[:150]}")
        traceback.print_exc()
        return False
