"""
sobre_a_adjuntos.py — Fase 4b del Expediente Digital v2.

Cuando el análisis de un proceso detecta formularios/plantillas que NO están
en el catálogo generable de sobre_a_core (p. ej. FR-CYC-002 de INAPA, o un
"Formulario 45" propio de la institución), este módulo:

  1. Detecta esos requisitos en bid_matching_pliegos.requisitos.
  2. Revisa la MISMA página del portal transaccional de donde se descargó el
     pliego (procesos.url → popup de documentos → pares documentFileId/mkey,
     la misma técnica de descargar_y_extraer_texto_pdf en main.py, pero
     devolviendo el inventario completo en vez de solo el pliego).
  3. Matchea por nombre (códigos SNCC/FR-XXX + solape de tokens) los
     requisitos contra los archivos del portal.
  4. Descarga los que matcheen al bucket privado y los registra en
     bid_formularios_proceso, para que el usuario los llene y suba su
     versión llenada (llenado_storage_path).
  5. sobre_a_core.listar_piezas los muestra en el catálogo del Sobre A:
     - solo plantilla  → warn  "Descargado del portal — llenar y subir"
     - llenado subido  → ok    (entra al ZIP la versión llenada)

bid_formularios_sync evita re-scrapear el portal en cada apertura del modal:
un intento por proceso cada REINTENTO_HORAS como máximo.

Módulo sin FastAPI: recibe el cliente de Supabase; el router lo orquesta.
"""

from __future__ import annotations

import re
import unicodedata
import uuid
from datetime import datetime, timedelta, timezone

import requests
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

BASE_PORTAL = "https://comunidad.comprasdominicana.gob.do"
BASE_DOWNLOAD = f"{BASE_PORTAL}/Public/Tendering/OpportunityDetail/DownloadFile"
CARPETA_BUCKET = "formularios_proceso"
REINTENTO_HORAS = 6
MAX_DESCARGA_BYTES = 25 * 1024 * 1024  # 25MB por archivo del portal

# Palabras que identifican un requisito como "formulario/plantilla a llenar"
_PALABRAS_FORMULARIO = ("formulario", "plantilla", "constancia", "certificacion de conocimiento",
                        "compromiso etico", "declaracion")
# Códigos de formulario reconocibles en nombres (SNCC.F.034, FR-CYC-002, F-045…)
_RE_CODIGO = re.compile(r"(?:sncc[.\s]?[fdc][.\s]?\d{2,3}|fr-[a-z]{2,4}-\d{2,4}|f[.\s-]?0?\d{2,3})",
                        re.IGNORECASE)

# Requisitos que NO son plantillas descargables aunque digan "certificación"
_EXCLUIR = ("rpe", "dgii", "tss", "antecedentes", "registro mercantil", "codia",
            "estatutos", "cedula", "cédula", "nomina de accionistas", "asamblea",
            "estados financieros", "poder de representacion", "mipyme")


def _normalizar(texto: str) -> str:
    t = unicodedata.normalize("NFD", (texto or "").lower())
    t = "".join(c for c in t if unicodedata.category(c) != "Mn")
    return re.sub(r"\s+", " ", t).strip()


def _tokens(texto: str) -> set[str]:
    stop = {"de", "del", "la", "el", "los", "las", "y", "o", "a", "en", "copia",
            "formulario", "para", "sobre", "con", "por", "al", "un", "una"}
    return {w for w in re.findall(r"[a-z0-9]{3,}", _normalizar(texto)) if w not in stop}


def _codigos(texto: str) -> set[str]:
    return {re.sub(r"[.\s-]", "", c.lower()) for c in _RE_CODIGO.findall(texto or "")}


# ══════════════════════════════════════════════════════════════
# 1) Detección de formularios requeridos no generables
# ══════════════════════════════════════════════════════════════

def detectar_formularios_requeridos(sb, empresa_id: str, referencia: str) -> list[dict]:
    """
    Requisitos del análisis que parecen formularios/plantillas propias y que
    el catálogo de sobre_a_core NO genera. Devuelve [{nombre, texto_original}].
    """
    from sobre_a_core import FORMULARIOS
    proc = (sb.table("bid_matching_pliegos").select("requisitos")
            .eq("empresa_id", empresa_id).eq("referencia", referencia)
            .order("creado_en", desc=True).limit(1).execute().data or [None])[0]
    if not proc or not isinstance(proc.get("requisitos"), dict):
        return []

    generables = {_normalizar(t) for _c, t in FORMULARIOS.values()}
    generables_cod = set()
    for cod, _t in FORMULARIOS.values():
        generables_cod |= _codigos(cod)

    encontrados, vistos = [], set()
    for item in (proc["requisitos"].get("otros") or []):
        nombre = (item or {}).get("nombre") or ""
        norm = _normalizar(nombre)
        if not norm or norm in vistos:
            continue
        if any(x in norm for x in _EXCLUIR):
            continue
        es_formulario = (any(p in norm for p in _PALABRAS_FORMULARIO)
                         or bool(_codigos(nombre)))
        if not es_formulario:
            continue
        # ¿Ya lo generamos nosotros? (por código o por nombre casi igual)
        cods = _codigos(nombre)
        if cods & generables_cod:
            continue
        if any(norm in g or g in norm for g in generables):
            continue
        vistos.add(norm)
        encontrados.append({"nombre": nombre,
                            "texto_original": (item or {}).get("texto_original") or ""})
    return encontrados


# ══════════════════════════════════════════════════════════════
# 2) Inventario de documentos del proceso en el portal
# ══════════════════════════════════════════════════════════════

def _url_proceso(sb, empresa_id: str, referencia: str) -> str | None:
    """URL de la página del proceso: procesos.url o pliego_url del matching."""
    p = (sb.table("procesos").select("url")
         .eq("codigo_proceso", referencia).limit(1).execute().data or [None])[0]
    if p and p.get("url"):
        return p["url"]
    m = (sb.table("bid_matching_pliegos").select("pliego_url")
         .eq("empresa_id", empresa_id).eq("referencia", referencia)
         .order("creado_en", desc=True).limit(1).execute().data or [None])[0]
    url = (m or {}).get("pliego_url") or ""
    return url if "noticeUID" in url else None


def inventario_portal(url_proceso: str) -> list[dict]:
    """
    Devuelve el inventario COMPLETO de documentos del proceso:
    [{nombre, file_id, mkey}] — misma técnica del scraper del pliego
    (main.py::descargar_y_extraer_texto_pdf) pero sin descartar nada.
    OJO: el mkey es por sesión; hay que descargar con la MISMA session,
    por eso también se devuelve la session en el segundo valor.
    """
    url_limpia = (url_proceso or "").replace("gob.do//", "gob.do/")
    headers_base = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                      "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,*/*;q=0.9",
        "Accept-Language": "es-ES,es;q=0.9",
    }
    headers_popup = {
        **headers_base,
        "Accept": "text/html, */*; q=0.01",
        "Referer": f"{BASE_PORTAL}/Public/Tendering/ContractNoticeManagement/Index",
        "X-Requested-With": "XMLHttpRequest",
    }
    session = requests.Session()
    session.verify = False
    try:
        session.get(f"{BASE_PORTAL}/Public/Tendering/ContractNoticeManagement/Index",
                    headers=headers_base, timeout=15)
    except requests.RequestException:
        pass

    # Variantes de URL (índice del proceso y popup de documentos)
    urls = [url_limpia]
    m = re.search(r"noticeUID=([\w.\-]+)", url_limpia)
    if m:
        uidp = m.group(1)
        urls = [
            f"{BASE_PORTAL}/Public/Tendering/OpportunityDetail/Index?noticeUID={uidp}",
            f"{BASE_PORTAL}/Public/Tendering/ProcurementContractManagement/"
            f"GetContractNoticeDocumentsPopUp?noticeUID={uidp}",
            url_limpia,
        ]

    regexes = [
        r"documentFileId=(\d+)&(?:amp;)?mkey=([\w\-]+)",
        r'"documentFileId"\s*:\s*(\d+)[^}]*?"mkey"\s*:\s*"([\w\-]+)"',
        r'data-(?:file-id|fileid|documentfileid)=["\'](\d+)["\'][^>]*?data-mkey=["\']([^"\']+)["\']',
    ]
    html_final, pares = "", []
    for u in urls:
        try:
            resp = session.get(u, headers=headers_popup if "PopUp" in u else headers_base,
                               timeout=20)
            html = resp.text
        except requests.RequestException:
            continue
        for rx in regexes:
            pares = re.findall(rx, html, re.IGNORECASE)
            if pares:
                break
        if pares:
            html_final = html
            break  # NO más requests: el mkey es por sesión (regla del scraper)

    inventario = []
    for file_id, mkey in pares:
        idx = html_final.find(str(file_id))
        ctx = html_final[max(0, idx - 800):idx + 100] if idx >= 0 else ""
        nm = (re.search(r">([\w\s\-\._\(\)]+\.(?:pdf|zip|xlsx|docx|doc))<", ctx, re.IGNORECASE)
              or re.search(r'"([\w\s\-\._\(\)]+\.(?:pdf|zip|xlsx|docx|doc))"', ctx, re.IGNORECASE))
        inventario.append({"nombre": nm.group(1).strip() if nm else "",
                           "file_id": file_id, "mkey": mkey})
    return inventario, session


# ══════════════════════════════════════════════════════════════
# 3) Match requisito ↔ archivo del portal
# ══════════════════════════════════════════════════════════════

def _matchear(requeridos: list[dict], inventario: list[dict]) -> list[tuple[dict, dict]]:
    """Empareja cada requisito con el mejor archivo del portal (o ninguno)."""
    parejas, usados = [], set()
    for req in requeridos:
        req_cod = _codigos(req["nombre"] + " " + req.get("texto_original", ""))
        req_tok = _tokens(req["nombre"])
        mejor, mejor_puntos = None, 0.0
        for doc in inventario:
            if doc["file_id"] in usados or not doc["nombre"]:
                continue
            doc_cod = _codigos(doc["nombre"])
            if req_cod and doc_cod and (req_cod & doc_cod):
                puntos = 10.0  # match por código: gana siempre
            else:
                inter = req_tok & _tokens(doc["nombre"])
                puntos = len(inter) / max(len(req_tok), 1)
                if puntos < 0.5:  # umbral: la mitad de los tokens del requisito
                    continue
            if puntos > mejor_puntos:
                mejor, mejor_puntos = doc, puntos
        if mejor:
            usados.add(mejor["file_id"])
            parejas.append((req, mejor))
    return parejas


# ══════════════════════════════════════════════════════════════
# 4) Sincronización: descarga + registro (idempotente)
# ══════════════════════════════════════════════════════════════

def _debe_intentar(sb, empresa_id: str, referencia: str, forzar: bool) -> bool:
    if forzar:
        return True
    reg = (sb.table("bid_formularios_sync").select("ultimo_intento")
           .eq("empresa_id", empresa_id).eq("referencia", referencia)
           .limit(1).execute().data or [None])[0]
    if not reg:
        return True
    try:
        ultimo = datetime.fromisoformat(str(reg["ultimo_intento"]).replace("Z", "+00:00"))
    except ValueError:
        return True
    return datetime.now(timezone.utc) - ultimo > timedelta(hours=REINTENTO_HORAS)


def _marcar_intento(sb, empresa_id: str, referencia: str, exito: bool, detalle: str):
    sb.table("bid_formularios_sync").upsert({
        "empresa_id": empresa_id, "referencia": referencia,
        "ultimo_intento": datetime.now(timezone.utc).isoformat(),
        "exito": exito, "detalle": detalle[:500],
    }).execute()


def sincronizar_formularios_proceso(sb, empresa_id: str, referencia: str,
                                    forzar: bool = False) -> dict:
    """
    Flujo completo. Devuelve
      {"descargados": [...], "sin_match": [...], "omitido": motivo|None}.
    Idempotente: no re-descarga archivos ya registrados y no re-scrapea el
    portal más de una vez cada REINTENTO_HORAS (salvo forzar=True).
    """
    from router_bid_manager import STORAGE_BUCKET

    requeridos = detectar_formularios_requeridos(sb, empresa_id, referencia)
    if not requeridos:
        return {"descargados": [], "sin_match": [], "omitido": "sin_formularios_nuevos"}

    existentes = {r["nombre_archivo"] for r in
                  (sb.table("bid_formularios_proceso").select("nombre_archivo")
                   .eq("empresa_id", empresa_id).eq("referencia", referencia)
                   .execute().data or [])}

    if not _debe_intentar(sb, empresa_id, referencia, forzar):
        return {"descargados": [], "sin_match": [r["nombre"] for r in requeridos],
                "omitido": "intento_reciente"}

    url = _url_proceso(sb, empresa_id, referencia)
    if not url:
        _marcar_intento(sb, empresa_id, referencia, False, "sin URL del portal")
        return {"descargados": [], "sin_match": [r["nombre"] for r in requeridos],
                "omitido": "sin_url_portal"}

    try:
        inventario, session = inventario_portal(url)
    except Exception as e:  # scraping nunca debe tumbar el flujo
        _marcar_intento(sb, empresa_id, referencia, False, f"scraper: {e}")
        return {"descargados": [], "sin_match": [r["nombre"] for r in requeridos],
                "omitido": "error_portal"}

    if not inventario:
        _marcar_intento(sb, empresa_id, referencia, False, "portal sin documentos")
        return {"descargados": [], "sin_match": [r["nombre"] for r in requeridos],
                "omitido": "portal_vacio"}

    parejas = _matchear(requeridos, inventario)
    con_match = {id(req) for req, _doc in parejas}
    descargados, errores = [], []

    for req, doc in parejas:
        if doc["nombre"] in existentes:
            continue  # ya descargado antes
        try:
            r = session.get(f"{BASE_DOWNLOAD}?documentFileId={doc['file_id']}"
                            f"&mkey={doc['mkey']}", timeout=30, stream=True)
            contenido = r.content
            if r.status_code != 200 or not contenido:
                raise ValueError(f"status {r.status_code}")
            if len(contenido) > MAX_DESCARGA_BYTES:
                raise ValueError("archivo demasiado grande")
        except Exception as e:
            errores.append(f"{doc['nombre']}: {e}")
            continue

        ext = (doc["nombre"].rsplit(".", 1)[-1] or "bin").lower()
        path = f"{CARPETA_BUCKET}/{empresa_id}/{referencia}/{uuid.uuid4()}.{ext}"
        mime = {"pdf": "application/pdf", "doc": "application/msword",
                "docx": "application/vnd.openxmlformats-officedocument"
                        ".wordprocessingml.document",
                "xlsx": "application/vnd.openxmlformats-officedocument"
                        ".spreadsheetml.sheet",
                "zip": "application/zip"}.get(ext, "application/octet-stream")
        try:
            sb.storage.from_(STORAGE_BUCKET).upload(
                path, contenido, file_options={"content-type": mime})
        except Exception as e:
            errores.append(f"{doc['nombre']}: upload {e}")
            continue

        sb.table("bid_formularios_proceso").upsert({
            "empresa_id": empresa_id, "referencia": referencia,
            "requisito": req["nombre"], "nombre_archivo": doc["nombre"],
            "storage_path": path, "mime": mime, "tamano_bytes": len(contenido),
            "origen": "portal",
        }, on_conflict="empresa_id,referencia,nombre_archivo").execute()
        descargados.append(doc["nombre"])

    sin_match = [r["nombre"] for r in requeridos if id(r) not in con_match]
    detalle = f"{len(descargados)} descargados, {len(sin_match)} sin match"
    if errores:
        detalle += f"; errores: {'; '.join(errores)[:200]}"
    _marcar_intento(sb, empresa_id, referencia, bool(descargados), detalle)
    return {"descargados": descargados, "sin_match": sin_match, "omitido": None}


# ══════════════════════════════════════════════════════════════
# 5) Piezas para el catálogo del Sobre A
# ══════════════════════════════════════════════════════════════

def piezas_formularios_proceso(sb, empresa_id: str, referencia: str) -> list[dict]:
    """
    Piezas 'documento' para sobre_a_core.listar_piezas:
    - llenado subido → ok (al ZIP entra la versión llenada)
    - solo plantilla → warn (al ZIP entra la plantilla para llenar a mano)
    Usa _legacy_path para que construir_paquete los descargue sin cambios.
    """
    filas = (sb.table("bid_formularios_proceso").select("*")
             .eq("empresa_id", empresa_id).eq("referencia", referencia)
             .order("nombre_archivo").execute().data or [])
    piezas = []
    for f in filas:
        llenado = bool(f.get("llenado_storage_path"))
        piezas.append({
            "id": f"formproc:{f['id']}",
            "tipo": "documento",
            "nombre": f.get("requisito") or f["nombre_archivo"],
            "estado": "ok" if llenado else "warn",
            "motivo": None if llenado else
            "Plantilla descargada del portal — llénala y sube tu versión "
            "(entrará la plantilla vacía mientras tanto)",
            "seccion": "Formularios de la institución",
            "registro_id": f["id"], "entidad": "formularios_proceso",
            "_legacy_path": f.get("llenado_storage_path") or f["storage_path"],
        })
    return piezas
