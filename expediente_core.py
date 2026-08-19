"""
expediente_core.py — Núcleo compartido del Expediente Digital v2
=================================================================
Fuente única de verdad para:
- Enums de estado (regla D3 de RECOMENDACIONES_PRODUCCION.md)
- resolver_estado(): propagación documento → pieza del Sobre A (regla D2)
- Validación de archivos por magic bytes (MIME real, no extensión)
- Helper de auditoría (bid_auditoria)
- Rate limiter en memoria (sin dependencias)
- Envelope de error consistente

Este módulo NO importa FastAPI ni Supabase a nivel de lógica pura,
para que resolver_estado y la validación MIME sean testeables sin red.
"""

import time
import threading
from collections import deque
from datetime import datetime, timezone

# ══════════════════════════════════════════════════════════════
# D3 — Taxonomía única de estados (espejo en JS: expediente-enums.js)
# ══════════════════════════════════════════════════════════════

ESTADO_REGISTRO = ("completo", "incompleto", "vencido", "por_vencer", "sin_verificar")
ESTADO_VERIFICACION = ("validado", "pendiente", "rechazado")
ESTADO_RENGLON = ("cumple", "en_riesgo", "no_cumple")
ESTADO_PIEZA = ("ok", "warn", "err")

ENTIDADES_ADJUNTOS = (
    "personal", "equipos", "experiencia", "socios",
    "representantes", "certificaciones", "financieros",
)

# Entidad → tabla donde vive el registro padre (para validar ownership)
TABLA_POR_ENTIDAD = {
    "personal": "bid_personal",
    "equipos": "bid_equipos",
    "experiencia": "bid_experiencia",
    "socios": "bid_socios",
    "representantes": "bid_representantes",
    "certificaciones": "bid_certificaciones",
    "financieros": "bid_financieros",
}


def resolver_estado(verificacion: str | None, vencido: bool = False) -> str:
    """
    Regla D2 — propagación documento → pieza del Sobre A.
    Devuelve estado_pieza ('ok' | 'warn' | 'err').

    | verificación         | vencido | pieza | comportamiento al generar          |
    |----------------------|---------|-------|------------------------------------|
    | validado             | no      | ok    | se incluye normal                  |
    | pendiente            | no      | warn  | se incluye marcado "sin confirmar" |
    | rechazado / None     | -       | err   | bloqueado / hueco resaltado        |
    | (cualquiera)         | sí      | err   | JAMÁS entra al ZIP                 |
    """
    if vencido:
        return "err"
    if verificacion == "validado":
        return "ok"
    if verificacion == "pendiente":
        return "warn"
    return "err"  # rechazado, None o valor desconocido


# ══════════════════════════════════════════════════════════════
# Validación de archivos — MIME real por magic bytes
# (sin libmagic: firmas binarias, cero dependencias de sistema)
# ══════════════════════════════════════════════════════════════

MAX_BYTES_ARCHIVO = 15 * 1024 * 1024   # 15 MB por archivo
MAX_BYTES_REQUEST = 50 * 1024 * 1024   # 50 MB por request
MAX_ARCHIVOS_POR_REQUEST = 5

# extensiones permitidas → mime canónico que guardamos en BD
EXT_PERMITIDAS = {
    "pdf":  "application/pdf",
    "doc":  "application/msword",
    "docx": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
    "xls":  "application/vnd.ms-excel",
    "xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    "jpg":  "image/jpeg",
    "jpeg": "image/jpeg",
    "png":  "image/png",
}

_FIRMAS = (
    (b"%PDF",                 {"pdf"}),
    (b"\xff\xd8\xff",         {"jpg", "jpeg"}),
    (b"\x89PNG\r\n\x1a\n",    {"png"}),
    (b"PK\x03\x04",           {"docx", "xlsx"}),        # OOXML = zip
    (b"\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1", {"doc", "xls"}),  # OLE2 legacy
)


def validar_contenido(nombre: str, contenido: bytes) -> tuple[str, str]:
    """
    Valida extensión + firma binaria real del contenido.
    Devuelve (ext, mime_canonico) o lanza ValueError con código estable.
    """
    if not contenido:
        raise ValueError("ARCHIVO_VACIO")
    if len(contenido) > MAX_BYTES_ARCHIVO:
        raise ValueError("ARCHIVO_MUY_GRANDE")

    ext = (nombre or "").rsplit(".", 1)[-1].lower() if "." in (nombre or "") else ""
    if ext not in EXT_PERMITIDAS:
        raise ValueError("TIPO_NO_PERMITIDO")

    cabecera = contenido[:16]
    for firma, exts in _FIRMAS:
        if cabecera.startswith(firma):
            if ext in exts:
                return ext, EXT_PERMITIDAS[ext]
            raise ValueError("CONTENIDO_NO_COINCIDE")  # ej. .exe renombrado a .pdf
    raise ValueError("CONTENIDO_NO_RECONOCIDO")


MENSAJES_ERROR = {
    "ARCHIVO_VACIO":          "El archivo está vacío",
    "ARCHIVO_MUY_GRANDE":     f"Máximo {MAX_BYTES_ARCHIVO // 1024 // 1024} MB por archivo",
    "REQUEST_MUY_GRANDE":     f"Máximo {MAX_BYTES_REQUEST // 1024 // 1024} MB por subida",
    "DEMASIADOS_ARCHIVOS":    f"Máximo {MAX_ARCHIVOS_POR_REQUEST} archivos por subida",
    "TIPO_NO_PERMITIDO":      "Solo se aceptan PDF, Word, Excel, JPG y PNG",
    "CONTENIDO_NO_COINCIDE":  "El contenido del archivo no corresponde a su extensión",
    "CONTENIDO_NO_RECONOCIDO": "No se pudo verificar el tipo real del archivo",
    "ENTIDAD_INVALIDA":       "Entidad inválida",
    "ESTADO_INVALIDO":        "Estado de verificación inválido",
    "RATE_LIMIT":             "Demasiadas solicitudes, intenta en un momento",
}


def error_body(code: str, field: str | None = None) -> dict:
    """Envelope de error consistente: {error:{code,message,field}}."""
    return {"error": {
        "code": code,
        "message": MENSAJES_ERROR.get(code, code),
        "field": field,
    }}


# ══════════════════════════════════════════════════════════════
# Rate limiter en memoria (ventana deslizante, por clave)
# Suficiente para una instancia en Railway; si escalas horizontal,
# migrar a un contador en Supabase/Redis.
# ══════════════════════════════════════════════════════════════

class RateLimiter:
    def __init__(self):
        self._hits: dict[str, deque] = {}
        self._lock = threading.Lock()

    def permitir(self, clave: str, max_hits: int, ventana_seg: int) -> bool:
        ahora = time.monotonic()
        with self._lock:
            cola = self._hits.setdefault(clave, deque())
            while cola and ahora - cola[0] > ventana_seg:
                cola.popleft()
            if len(cola) >= max_hits:
                return False
            cola.append(ahora)
            return True


rate_limiter = RateLimiter()

LIMITE_SUBIDAS = (30, 60)          # 30 por minuto
LIMITE_SOBRE_A = (10, 3600)        # 10 por hora
LIMITE_PROPUESTA_FORZADA = (5, 3600)


# ══════════════════════════════════════════════════════════════
# Auditoría — helper único para toda mutación del expediente
# ══════════════════════════════════════════════════════════════

def registrar_auditoria(sb, empresa_id: str, actor: str, accion: str,
                        entidad: str | None = None, registro_id: str | None = None,
                        detalle: dict | None = None) -> None:
    """
    Escribe en bid_auditoria. Nunca rompe el request si falla:
    la auditoría es importante pero no puede tumbar la operación principal.
    actor: 'usuario:{uuid}' | 'ia' | 'sistema'
    """
    try:
        sb.table("bid_auditoria").insert({
            "empresa_id": empresa_id,
            "actor": actor,
            "accion": accion,
            "entidad": entidad,
            "registro_id": registro_id,
            "detalle": detalle or {},
            "creado_en": datetime.now(timezone.utc).isoformat(),
        }).execute()
    except Exception:
        pass  # log-and-continue: no tumbar la operación por la auditoría
