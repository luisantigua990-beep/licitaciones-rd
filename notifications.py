import os
import json
from pywebpush import webpush, WebPushException

VAPID_PRIVATE_KEY = os.getenv("VAPID_PRIVATE_KEY")
VAPID_PUBLIC_KEY  = os.getenv("VAPID_PUBLIC_KEY")
VAPID_EMAIL       = os.getenv("VAPID_EMAIL", "mailto:luisantigua990@gmail.com")

def enviar_notificacion(subscription_info: dict, titulo: str, cuerpo: str, url: str = "/") -> bool:
    """
    Retorna:
      True  — enviado OK
      False — error general
      "410" — suscripción expirada/eliminada (hay que desactivarla en BD)
    """
    try:
        data = json.dumps({"title": titulo, "body": cuerpo, "url": url})
        webpush(
            subscription_info=subscription_info,
            data=data,
            vapid_private_key=VAPID_PRIVATE_KEY,
            vapid_claims={"sub": VAPID_EMAIL}
        )
        return True
    except WebPushException as e:
        resp = getattr(e, "response", None)
        status = getattr(resp, "status_code", 0) if resp else 0
        if status in (404, 410):
            # Suscripción expirada o eliminada — indicar al caller que la limpie
            return "410"
        print(f"Error enviando notificación: {e}")
        return False
