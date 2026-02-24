import os
import json
from pywebpush import webpush, WebPushException

VAPID_PRIVATE_KEY = os.getenv("VAPID_PRIVATE_KEY")
VAPID_PUBLIC_KEY = os.getenv("VAPID_PUBLIC_KEY")
VAPID_EMAIL = os.getenv("VAPID_EMAIL", "mailto:luisantigua990@gmail.com")

def enviar_notificacion(subscription_info: dict, titulo: str, cuerpo: str, url: str = "/") -> bool:
    try:
        data = json.dumps({
            "title": titulo,
            "body": cuerpo,
            "url": url,
            "icon": "/icons/icon-192.png"
        })
        
        webpush(
            subscription_info=subscription_info,
            data=data,
            vapid_private_key=VAPID_PRIVATE_KEY,
            vapid_claims={"sub": VAPID_EMAIL}
        )
        return True
    except WebPushException as e:
        print(f"Error enviando notificación: {e}")
        return False
