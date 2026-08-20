"""
router_bid_archivos.py — Adjuntos por registro (Expediente Digital v2, Fase 0)
==============================================================================
Adjuntos genéricos para personal, equipos, experiencia, socios,
representantes, certificaciones y financieros. Tabla: bid_archivos.

Seguridad (ver RECOMENDACIONES_PRODUCCION.md §1):
- Auth con el mismo _auth/_empresa_id de router_bid_manager.
- Ownership del registro padre validado EN LA QUERY (404, nunca 403).
- MIME real por magic bytes; nombre saneado a uuid; bucket privado.
- Descarga solo vía signed URL de corta duración.
- Rate limit 30 subidas/min por usuario.
- Soft delete (eliminado_en); purga física por cron a 30 días.
- Toda mutación escribe en bid_auditoria.

Integración en main.py (2 líneas):
    from router_bid_archivos import bid_archivos_router
    app.include_router(bid_archivos_router)
"""

import uuid as uuidlib
from datetime import datetime, timezone

from fastapi import APIRouter, HTTPException, Header, File, UploadFile
from typing import List

from router_bid_manager import _auth, _empresa_id, _sb, STORAGE_BUCKET
from expediente_core import (
    ENTIDADES_ADJUNTOS, TABLA_POR_ENTIDAD, ESTADO_VERIFICACION,
    MAX_BYTES_REQUEST, MAX_ARCHIVOS_POR_REQUEST,
    validar_contenido, error_body,
    rate_limiter, LIMITE_SUBIDAS,
    registrar_auditoria,
)

bid_archivos_router = APIRouter(prefix="/api/bid/expediente", tags=["bid-archivos"])

SIGNED_URL_SEG = 300  # 5 minutos


# ── Helpers ────────────────────────────────────────────────

def _err(status: int, code: str, field: str | None = None):
    raise HTTPException(status_code=status, detail=error_body(code, field)["error"])


def _validar_entidad(entidad: str) -> str:
    if entidad not in ENTIDADES_ADJUNTOS:
        _err(422, "ENTIDAD_INVALIDA", "entidad")
    return entidad


def _validar_registro(entidad: str, registro_id: str, empresa_id: str) -> None:
    """Ownership en el WHERE: el registro debe existir Y ser de la empresa.
    404 en ambos casos — no revelar existencia de registros ajenos."""
    tabla = TABLA_POR_ENTIDAD[entidad]
    res = (_sb.table(tabla).select("id")
           .eq("id", registro_id).eq("empresa_id", empresa_id)
           .limit(1).execute())
    if not res.data:
        raise HTTPException(404, "Registro no encontrado")


def _archivo_de_empresa(archivo_id: str, empresa_id: str) -> dict:
    res = (_sb.table("bid_archivos").select("*")
           .eq("id", archivo_id).eq("empresa_id", empresa_id)
           .is_("eliminado_en", "null")
           .limit(1).execute())
    if not res.data:
        raise HTTPException(404, "Archivo no encontrado")
    return res.data[0]


def _serializar(a: dict) -> dict:
    return {
        "id": a["id"],
        "nombre": a["nombre"],
        "mime": a["mime"],
        "tamano": a["tamano_bytes"],
        "estado": a["estado"],
        "subido_por": a.get("subido_por"),
        "creado_en": a.get("creado_en"),
        "entidad": a.get("entidad"),
        "registro_id": a.get("registro_id"),
    }


# ── Endpoints ──────────────────────────────────────────────

@bid_archivos_router.post("/{entidad}/{registro_id}/archivos", status_code=201)
async def subir_archivos(
    entidad: str,
    registro_id: str,
    files: List[UploadFile] | None = File(default=None),
    authorization: str | None = Header(default=None),
):
    """Sube hasta 5 archivos (PDF/Word/Excel/JPG/PNG) a un registro del expediente.
    La auth corre ANTES que cualquier validación de body: sin token siempre es 401."""
    uid = _auth(authorization)
    eid = _empresa_id(uid)
    _validar_entidad(entidad)
    _validar_registro(entidad, registro_id, eid)
    if not files:
        _err(422, "ARCHIVO_VACIO", "files")

    if not rate_limiter.permitir(f"up:{uid}", *LIMITE_SUBIDAS):
        _err(429, "RATE_LIMIT")
    if len(files) > MAX_ARCHIVOS_POR_REQUEST:
        _err(422, "DEMASIADOS_ARCHIVOS", "files")

    # Leer y validar TODO antes de subir nada (o todo o nada)
    total = 0
    validados = []
    for f in files:
        contenido = await f.read()
        total += len(contenido)
        if total > MAX_BYTES_REQUEST:
            _err(413, "REQUEST_MUY_GRANDE")
        try:
            ext, mime = validar_contenido(f.filename or "", contenido)
        except ValueError as e:
            code = str(e)
            status = 413 if code == "ARCHIVO_MUY_GRANDE" else 415
            _err(status, code, f.filename)
        validados.append((f.filename or f"archivo.{ext}", ext, mime, contenido))

    creados = []
    ahora = datetime.now(timezone.utc).isoformat()
    for nombre, ext, mime, contenido in validados:
        archivo_id = str(uuidlib.uuid4())
        path = f"{eid}/expediente/{entidad}/{registro_id}/{archivo_id}.{ext}"
        try:
            _sb.storage.from_(STORAGE_BUCKET).upload(
                path=path, file=contenido,
                file_options={"content-type": mime, "upsert": "false"},
            )
        except Exception as e:
            raise HTTPException(500, f"Error subiendo archivo: {e}")

        row = _sb.table("bid_archivos").insert({
            "id": archivo_id,
            "empresa_id": eid,
            "entidad": entidad,
            "registro_id": registro_id,
            "nombre": nombre,
            "storage_path": path,
            "mime": mime,
            "tamano_bytes": len(contenido),
            "estado": "pendiente",
            "subido_por": uid,
            "creado_en": ahora,
        }).execute().data[0]
        creados.append(_serializar(row))

        registrar_auditoria(_sb, eid, f"usuario:{uid}", "subio_archivo",
                            entidad, registro_id,
                            {"nombre": nombre, "mime": mime, "bytes": len(contenido)})

    return creados


@bid_archivos_router.get("/{entidad}/{registro_id}/archivos")
def listar_archivos(
    entidad: str,
    registro_id: str,
    authorization: str | None = Header(default=None),
):
    uid = _auth(authorization)
    eid = _empresa_id(uid)
    _validar_entidad(entidad)
    _validar_registro(entidad, registro_id, eid)

    res = (_sb.table("bid_archivos").select("*")
           .eq("empresa_id", eid).eq("entidad", entidad).eq("registro_id", registro_id)
           .is_("eliminado_en", "null")
           .order("creado_en", desc=True).execute())
    return [_serializar(a) for a in (res.data or [])]


@bid_archivos_router.get("/archivos/conteos")
def conteos_por_entidad(
    entidad: str,
    authorization: str | None = Header(default=None),
):
    """
    Conteo de adjuntos por registro de una entidad, en UNA query.
    El frontend lo mergea con el listado de la sección → columna 'Adjuntos'
    sin N+1 y sin tocar los endpoints de listado existentes.
    Respuesta: { "<registro_id>": {"n": 3, "tipos": "PDF · JPG"} }
    """
    uid = _auth(authorization)
    eid = _empresa_id(uid)
    _validar_entidad(entidad)

    res = (_sb.table("bid_archivos").select("registro_id, mime")
           .eq("empresa_id", eid).eq("entidad", entidad)
           .is_("eliminado_en", "null").execute())

    ETIQUETA = {
        "application/pdf": "PDF",
        "application/msword": "DOC",
        "application/vnd.openxmlformats-officedocument.wordprocessingml.document": "DOC",
        "application/vnd.ms-excel": "XLS",
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": "XLS",
        "image/jpeg": "JPG",
        "image/png": "PNG",
    }
    out: dict = {}
    for a in (res.data or []):
        rid = a["registro_id"]
        et = ETIQUETA.get(a["mime"], "?")
        entry = out.setdefault(rid, {"n": 0, "_tipos": []})
        entry["n"] += 1
        if et not in entry["_tipos"]:
            entry["_tipos"].append(et)
    for entry in out.values():
        entry["tipos"] = " · ".join(entry.pop("_tipos"))
    return out


@bid_archivos_router.get("/archivos/{archivo_id}/descargar")
def descargar_archivo(
    archivo_id: str,
    authorization: str | None = Header(default=None),
):
    """Devuelve una signed URL de 5 minutos tras validar ownership."""
    uid = _auth(authorization)
    eid = _empresa_id(uid)
    a = _archivo_de_empresa(archivo_id, eid)
    try:
        # {"download": nombre} fuerza descarga con el nombre original del
        # archivo, en vez de navegar a la URL cruda de Supabase.
        try:
            res = _sb.storage.from_(STORAGE_BUCKET).create_signed_url(
                a["storage_path"], SIGNED_URL_SEG, {"download": a["nombre"]})
        except TypeError:  # versiones viejas del SDK sin options
            res = _sb.storage.from_(STORAGE_BUCKET).create_signed_url(
                a["storage_path"], SIGNED_URL_SEG)
        signed = (res or {}).get("signedURL") or (res or {}).get("signed_url")
        if not signed:
            raise HTTPException(500, "No se pudo generar la URL firmada")
        return {"url": signed, "expires_in": SIGNED_URL_SEG, "nombre": a["nombre"]}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(500, f"Error generando URL firmada: {e}")


@bid_archivos_router.patch("/archivos/{archivo_id}")
def actualizar_estado_archivo(
    archivo_id: str,
    request: dict | None = None,
    authorization: str | None = Header(default=None),
):
    """Confirma la verificación humana: {"estado": "validado" | "rechazado"}."""
    uid = _auth(authorization)
    eid = _empresa_id(uid)
    estado = (request or {}).get("estado")
    if estado not in ESTADO_VERIFICACION:
        _err(422, "ESTADO_INVALIDO", "estado")
    a = _archivo_de_empresa(archivo_id, eid)

    row = (_sb.table("bid_archivos")
           .update({"estado": estado,
                    "actualizado_en": datetime.now(timezone.utc).isoformat()})
           .eq("id", archivo_id).eq("empresa_id", eid)
           .execute().data[0])

    registrar_auditoria(_sb, eid, f"usuario:{uid}", "verifico_archivo",
                        a["entidad"], a["registro_id"],
                        {"archivo": a["nombre"], "estado": estado})
    return _serializar(row)


@bid_archivos_router.delete("/archivos/{archivo_id}", status_code=204)
def eliminar_archivo(
    archivo_id: str,
    authorization: str | None = Header(default=None),
):
    """Soft delete: el archivo puede estar referenciado por un Sobre A generado.
    La purga física del storage la hace un cron a 30 días."""
    uid = _auth(authorization)
    eid = _empresa_id(uid)
    a = _archivo_de_empresa(archivo_id, eid)

    (_sb.table("bid_archivos")
     .update({"eliminado_en": datetime.now(timezone.utc).isoformat()})
     .eq("id", archivo_id).eq("empresa_id", eid).execute())

    registrar_auditoria(_sb, eid, f"usuario:{uid}", "elimino_archivo",
                        a["entidad"], a["registro_id"], {"archivo": a["nombre"]})
    return None


@bid_archivos_router.post("/cron/purgar-archivos")
def cron_purgar_archivos(x_admin_key: str = Header(default=None)):
    """
    Purga física de archivos con soft delete > 30 días (storage + fila).
    Llamado por el cron de n8n. Header: X-Admin-Key (mismo patrón del repo).
    Idempotente: si el storage ya no tiene el archivo, borra la fila igual.
    """
    import os
    admin_secret = os.getenv("ADMIN_SECRET", "")
    if not admin_secret or x_admin_key != admin_secret:
        raise HTTPException(403, "No autorizado")

    from datetime import timedelta
    corte = (datetime.now(timezone.utc) - timedelta(days=30)).isoformat()
    res = (_sb.table("bid_archivos").select("id, empresa_id, storage_path")
           .lt("eliminado_en", corte).not_.is_("eliminado_en", "null")
           .limit(200).execute())
    purgados, errores = 0, 0
    for a in (res.data or []):
        try:
            _sb.storage.from_(STORAGE_BUCKET).remove([a["storage_path"]])
        except Exception:
            pass  # ya no existe en storage: seguimos con la fila
        try:
            _sb.table("bid_archivos").delete().eq("id", a["id"]).execute()
            purgados += 1
        except Exception:
            errores += 1
    return {"purgados": purgados, "errores": errores, "corte": corte}


@bid_archivos_router.get("/actividad")
def actividad_reciente(
    limit: int = 10,
    authorization: str | None = Header(default=None),
):
    """Actividad reciente del expediente (alimenta el Resumen y la trazabilidad)."""
    uid = _auth(authorization)
    eid = _empresa_id(uid)
    limit = max(1, min(limit, 50))
    res = (_sb.table("bid_auditoria").select("actor, accion, entidad, registro_id, detalle, creado_en")
           .eq("empresa_id", eid)
           .order("creado_en", desc=True).limit(limit).execute())
    return res.data or []
