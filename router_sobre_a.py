"""
router_sobre_a.py — Fase 4 del Expediente Digital v2.

Generación granular y ASÍNCRONA del Sobre A (jamás síncrono: timeout en
Railway). Endpoints bajo el mismo prefijo del módulo:

  GET  /api/bid/expediente/sobre-a/piezas?referencia=
       Catálogo con estado D2 por pieza. La primera vez dispara en
       background la sincronización de formularios del portal (Fase 4b).

  POST /api/bid/expediente/sobre-a/generar
       {referencia, piezas: [...]|"todas", formato: "docx"|"pdf"}
       → 202 {job_id}. Job persistido en bid_sobre_jobs; worker con
       BackgroundTasks. Rate limit 10/hora (LIMITE_SOBRE_A). Audita
       'genero_sobre_a'. Pieza individual usa el MISMO contrato de job
       (puede quedar 'listo' casi al instante).

  GET  /api/bid/expediente/sobre-a/jobs/{id}
       {estado, progreso:{pieza_actual, completadas, total},
        resultado:{url_firmada, nombre}, pendientes:[]}

  GET  /api/bid/expediente/sobre-a/jobs?referencia=
       Historial (pestaña 2 del modal).

  POST /api/bid/expediente/sobre-a/sincronizar-formularios?referencia=
       Fuerza la búsqueda/descarga de formularios propios en el portal.

  POST /api/bid/expediente/sobre-a/formularios/{id}/llenado
       Sube la versión LLENADA de un formulario del portal (multipart).

Seguridad no negociable (idéntica a Fase 0-2): _auth/_empresa_id,
ownership EN el WHERE, 404 (nunca 403) para registros ajenos, MIME real
por magic bytes en toda subida.
"""

from __future__ import annotations

import uuid as uuid_lib
from datetime import datetime, timedelta, timezone

from fastapi import (APIRouter, BackgroundTasks, File, Header, HTTPException,
                     Query, UploadFile)
from pydantic import BaseModel

from expediente_core import (LIMITE_SOBRE_A, error_body, rate_limiter,
                             registrar_auditoria, validar_contenido)
from router_bid_manager import STORAGE_BUCKET, _auth, _empresa_id, _sb

sobre_a_router = APIRouter(prefix="/api/bid/expediente", tags=["sobre-a"])

TTL_ZIP_DIAS = 7
URL_FIRMADA_SEG = 300  # 5 minutos, igual que descargas de Fase 0
CARPETA_SOBRES = "sobre_a"


def _err(status: int, code: str, field: str | None = None):
    raise HTTPException(status_code=status, detail=error_body(code, field)["error"])


def _ahora() -> str:
    return datetime.now(timezone.utc).isoformat()


# ══════════════════════════════════════════════════════════════
# 1. Catálogo de piezas
# ══════════════════════════════════════════════════════════════

@sobre_a_router.get("/sobre-a/piezas")
def piezas_sobre_a(referencia: str = Query(..., min_length=3),
                   background_tasks: BackgroundTasks = None,
                   authorization: str | None = Header(default=None)):
    uid = _auth(authorization)
    eid = _empresa_id(uid)

    # Fase 4b: primera vez (o cada 6h) intenta traer formularios del portal
    # en background — la respuesta de piezas no espera al scraper.
    try:
        from sobre_a_adjuntos import _debe_intentar, sincronizar_formularios_proceso
        if background_tasks is not None and _debe_intentar(_sb, eid, referencia, False):
            background_tasks.add_task(
                sincronizar_formularios_proceso, _sb, eid, referencia, False)
    except Exception:
        pass  # el catálogo nunca se cae por el scraper

    from sobre_a_core import listar_piezas, soffice_disponible
    piezas = listar_piezas(_sb, eid, referencia)
    return {
        "referencia": referencia,
        "pdf_disponible": soffice_disponible(),
        "piezas": piezas,
        "resumen": {
            "total": len(piezas),
            "ok": sum(1 for p in piezas if p["estado"] == "ok"),
            "warn": sum(1 for p in piezas if p["estado"] == "warn"),
            "err": sum(1 for p in piezas if p["estado"] == "err"),
        },
    }


# ══════════════════════════════════════════════════════════════
# 2. Generar (202 + job persistido)
# ══════════════════════════════════════════════════════════════

class GenerarBody(BaseModel):
    referencia: str
    piezas: list[str] | str = "todas"
    formato: str = "docx"


@sobre_a_router.post("/sobre-a/generar", status_code=202)
def generar_sobre_a(body: GenerarBody, background_tasks: BackgroundTasks,
                    authorization: str | None = Header(default=None)):
    uid = _auth(authorization)
    eid = _empresa_id(uid)

    if not rate_limiter.permitir(f"sobre:{uid}", *LIMITE_SOBRE_A):
        _err(429, "rate_limit")
    if body.formato not in ("docx", "pdf"):
        _err(422, "formato_invalido", "formato")
    if isinstance(body.piezas, list) and not body.piezas:
        _err(422, "piezas_vacias", "piezas")

    job = _sb.table("bid_sobre_jobs").insert({
        "empresa_id": eid,
        "referencia": body.referencia,
        "piezas": body.piezas if isinstance(body.piezas, list) else ["todas"],
        "formato": body.formato,
        "estado": "en_cola",
        "progreso": {"pieza_actual": None, "completadas": 0, "total": 0},
        "expira_en": (datetime.now(timezone.utc)
                      + timedelta(days=TTL_ZIP_DIAS)).isoformat(),
    }).execute().data[0]

    registrar_auditoria(_sb, eid, f"usuario:{uid}", "genero_sobre_a",
                        entidad="sobre_a", registro_id=job["id"],
                        detalle={"referencia": body.referencia,
                                 "formato": body.formato,
                                 "piezas": (body.piezas if isinstance(body.piezas, list)
                                            else "todas")})

    background_tasks.add_task(_worker_generar, job["id"], eid, uid)
    return {"job_id": job["id"], "estado": "en_cola"}


def _worker_generar(job_id: str, empresa_id: str, uid: str) -> None:
    """Worker (BackgroundTasks): genera el paquete, sube el ZIP y cierra el job."""
    from sobre_a_core import construir_paquete

    job = (_sb.table("bid_sobre_jobs").select("*")
           .eq("id", job_id).eq("empresa_id", empresa_id)
           .limit(1).execute().data or [None])[0]
    if not job:
        return

    def _upd(campos: dict):
        campos["actualizado_en"] = _ahora()
        _sb.table("bid_sobre_jobs").update(campos) \
            .eq("id", job_id).eq("empresa_id", empresa_id).execute()

    _upd({"estado": "procesando"})

    def _progreso(pieza_actual: str, completadas: int, total: int):
        _upd({"progreso": {"pieza_actual": pieza_actual,
                           "completadas": completadas, "total": total}})

    try:
        piezas = job["piezas"]
        if piezas == ["todas"]:
            piezas = "todas"
        r = construir_paquete(_sb, empresa_id, job["referencia"], piezas,
                              formato=job["formato"], progreso_cb=_progreso)

        path = (f"{CARPETA_SOBRES}/{empresa_id}/{job['referencia']}/"
                f"{job_id}/{r['nombre']}")
        _sb.storage.from_(STORAGE_BUCKET).upload(
            path, r["zip_bytes"], file_options={"content-type": "application/zip"})

        _upd({
            "estado": "listo",
            "resultado": {"storage_path": path, "nombre": r["nombre"],
                          "bytes": len(r["zip_bytes"]),
                          "indice": r["indice"], "omitidas": r["omitidas"]},
            "pendientes": r["pendientes"],
        })
        _notificar_push(uid, "Sobre A listo 📦",
                        f"Tu Sobre A de {job['referencia']} está listo para descargar.",
                        f"/#bid?ref={job['referencia']}")
    except Exception as e:
        _upd({"estado": "error", "error": str(e)[:500]})
        _notificar_push(uid, "Sobre A con error",
                        f"La generación del Sobre A de {job['referencia']} falló. "
                        "Inténtalo de nuevo.",
                        f"/#bid?ref={job['referencia']}")


def _notificar_push(uid: str, titulo: str, cuerpo: str, url: str) -> None:
    """Push al usuario con las suscripciones VAPID existentes. Nunca rompe."""
    try:
        from notifications import enviar_notificacion
        subs = (_sb.table("user_subscriptions").select("endpoint,auth,p256dh")
                .eq("user_id", uid).eq("active", True).execute().data or [])
        for s in subs:
            try:
                enviar_notificacion(
                    subscription_info={"endpoint": s["endpoint"],
                                       "keys": {"auth": s["auth"],
                                                "p256dh": s["p256dh"]}},
                    titulo=titulo, cuerpo=cuerpo, url=url)
            except Exception:
                continue
    except Exception:
        pass


# ══════════════════════════════════════════════════════════════
# 3. Estado del job (polling) e historial
# ══════════════════════════════════════════════════════════════

@sobre_a_router.get("/sobre-a/jobs/{job_id}")
def estado_job(job_id: str, authorization: str | None = Header(default=None)):
    uid = _auth(authorization)
    eid = _empresa_id(uid)
    try:
        uuid_lib.UUID(job_id)
    except ValueError:
        _err(404, "no_encontrado")

    job = (_sb.table("bid_sobre_jobs").select("*")
           .eq("id", job_id).eq("empresa_id", eid)   # ownership EN el WHERE
           .limit(1).execute().data or [None])[0]
    if not job:
        _err(404, "no_encontrado")                    # 404, nunca 403

    resultado = dict(job.get("resultado") or {})
    if job["estado"] == "listo" and resultado.get("storage_path"):
        try:
            res = _sb.storage.from_(STORAGE_BUCKET).create_signed_url(
                resultado["storage_path"], URL_FIRMADA_SEG,
                {"download": resultado.get("nombre") or "Sobre_A.zip"})
            url = (res.get("signedURL") or res.get("signedUrl")
                   or res.get("signed_url") or res.get("url")) \
                if isinstance(res, dict) else (
                    getattr(res, "signedURL", None) or getattr(res, "signedUrl", None)
                    or getattr(res, "signed_url", None) or getattr(res, "url", None))
            resultado["url_firmada"] = url
        except Exception:
            resultado["url_firmada"] = None
        resultado.pop("storage_path", None)  # la ruta interna no sale al cliente

    return {"id": job["id"], "referencia": job["referencia"],
            "formato": job["formato"], "estado": job["estado"],
            "progreso": job.get("progreso") or {},
            "resultado": resultado,
            "pendientes": job.get("pendientes") or [],
            "error": job.get("error"),
            "creado_en": job["creado_en"], "expira_en": job.get("expira_en")}


@sobre_a_router.get("/sobre-a/jobs")
def historial_jobs(referencia: str = Query(None),
                   authorization: str | None = Header(default=None)):
    uid = _auth(authorization)
    eid = _empresa_id(uid)
    q = (_sb.table("bid_sobre_jobs")
         .select("id,referencia,formato,estado,creado_en,expira_en,pendientes,resultado")
         .eq("empresa_id", eid))
    if referencia:
        q = q.eq("referencia", referencia)
    filas = q.order("creado_en", desc=True).limit(20).execute().data or []
    ahora = datetime.now(timezone.utc)
    salida = []
    for f in filas:
        res = f.get("resultado") or {}
        vencido = False
        try:
            vencido = (f.get("expira_en") and datetime.fromisoformat(
                str(f["expira_en"]).replace("Z", "+00:00")) < ahora)
        except ValueError:
            pass
        salida.append({
            "id": f["id"], "referencia": f["referencia"], "formato": f["formato"],
            "estado": "expirado" if (f["estado"] == "listo" and vencido) else f["estado"],
            "nombre": res.get("nombre"),
            "pendientes": len(f.get("pendientes") or []),
            "creado_en": f["creado_en"], "expira_en": f.get("expira_en"),
        })
    return {"jobs": salida}


# ══════════════════════════════════════════════════════════════
# 4. Formularios del portal (Fase 4b)
# ══════════════════════════════════════════════════════════════

@sobre_a_router.post("/sobre-a/sincronizar-formularios")
def sincronizar_formularios(referencia: str = Query(..., min_length=3),
                            authorization: str | None = Header(default=None)):
    uid = _auth(authorization)
    eid = _empresa_id(uid)
    if not rate_limiter.permitir(f"syncform:{uid}", 6, 3600):  # 6/hora
        _err(429, "rate_limit")
    from sobre_a_adjuntos import sincronizar_formularios_proceso
    r = sincronizar_formularios_proceso(_sb, eid, referencia, forzar=True)
    registrar_auditoria(_sb, eid, f"usuario:{uid}", "sincronizo_formularios",
                        entidad="formularios_proceso",
                        detalle={"referencia": referencia,
                                 "descargados": r["descargados"],
                                 "sin_match": r["sin_match"]})
    return r


@sobre_a_router.post("/sobre-a/formularios/{formulario_id}/llenado")
async def subir_llenado(formulario_id: str,
                        archivo: UploadFile = File(...),
                        authorization: str | None = Header(default=None)):
    """Sube la versión LLENADA de un formulario descargado del portal."""
    uid = _auth(authorization)
    eid = _empresa_id(uid)

    reg = (_sb.table("bid_formularios_proceso").select("id,referencia")
           .eq("id", formulario_id).eq("empresa_id", eid)  # ownership EN el WHERE
           .limit(1).execute().data or [None])[0]
    if not reg:
        _err(404, "no_encontrado")

    contenido = await archivo.read()
    try:
        ext, mime = validar_contenido(archivo.filename or "archivo", contenido)
    except ValueError as e:
        code = str(e)
        _err(413 if code == "ARCHIVO_MUY_GRANDE" else 415, code)

    path = (f"{CARPETA_SOBRES}/llenados/{eid}/{reg['referencia']}/"
            f"{uuid_lib.uuid4()}.{ext}")
    _sb.storage.from_(STORAGE_BUCKET).upload(
        path, contenido, file_options={"content-type": mime})

    _sb.table("bid_formularios_proceso").update({
        "llenado_storage_path": path,
        "llenado_nombre": archivo.filename,
        "actualizado_en": _ahora(),
    }).eq("id", formulario_id).eq("empresa_id", eid).execute()

    registrar_auditoria(_sb, eid, f"usuario:{uid}", "subio_llenado_formulario",
                        entidad="formularios_proceso", registro_id=formulario_id,
                        detalle={"nombre": archivo.filename})
    return {"ok": True, "llenado_nombre": archivo.filename}
