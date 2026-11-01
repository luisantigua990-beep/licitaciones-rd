"""
router_obras.py — Control de Obra para LicitacionLab (uso interno CONSER)
=========================================================================
Equivalente mínimo a SINCO ADPRO: presupuesto contratado vs. ejecutado vs. gastado.

Entidades:
- Obra (contrato)                 /api/obras
- Partidas contratadas            /api/obras/{id}/partidas   (bulk desde el listado de la oferta)
- Cubicaciones + detalle          /api/obras/{id}/cubicaciones
- Gastos (costo real)             /api/obras/{id}/gastos
- Tablero                         /api/obras/{id}/tablero     (sale de las vistas v_obra_*)

Integración en main.py (2 líneas):
    from router_obras import obras_router
    app.include_router(obras_router)

Requiere la migración migrations/2026-10-23_obras_fase0.sql aplicada.
"""

import os
import json
import uuid
import base64
import urllib.request
from datetime import datetime, date
from typing import Optional, List

from fastapi import APIRouter, HTTPException, Header, UploadFile, File, Form
from pydantic import BaseModel, Field
from supabase_client import crear_cliente

import obras_import

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
SUPABASE_SERVICE_KEY = os.getenv("SUPABASE_SERVICE_KEY", SUPABASE_KEY)

_sb = crear_cliente(SUPABASE_URL, SUPABASE_SERVICE_KEY)

obras_router = APIRouter(prefix="/api/obras", tags=["obras"])

ESTADOS_OBRA = {"planificada", "en_ejecucion", "suspendida", "terminada", "cerrada"}
ESTADOS_CUB = {"borrador", "presentada", "aprobada", "pagada"}
TIPOS_GASTO = {"material", "mano_obra", "equipo", "subcontrato", "transporte", "indirecto"}


# ── Auth (mismo patrón que bid_manager) ─────────────────────
def _auth(authorization: str | None) -> str:
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


def _empresa_id(user_id: str) -> str:
    res = _sb.table("perfiles_empresa").select("id").eq("user_id", user_id).execute()
    if not res.data:
        raise HTTPException(404, "No tienes perfil de empresa. Completa el onboarding primero.")
    return res.data[0]["id"]


def _ctx(authorization: str | None) -> str:
    """Atajo: token -> empresa_id."""
    return _empresa_id(_auth(authorization))


def _obra_de(empresa_id: str, obra_id: str) -> dict:
    res = _sb.table("obras").select("*").eq("id", obra_id).eq("empresa_id", empresa_id).execute()
    if not res.data:
        raise HTTPException(404, "Obra no encontrada")
    return res.data[0]


def _limpiar(d: dict) -> dict:
    """Quita None y serializa fechas para Supabase."""
    out = {}
    for k, v in d.items():
        if v is None:
            continue
        out[k] = v.isoformat() if isinstance(v, (date, datetime)) else v
    return out


# ══════════════════════════════════════════════════════════════
# 1. OBRAS
# ══════════════════════════════════════════════════════════════

class ObraIn(BaseModel):
    nombre: str
    referencia: Optional[str] = None
    entidad: Optional[str] = None
    contrato_no: Optional[str] = None
    monto_contrato: float = 0
    fecha_inicio: Optional[date] = None
    fecha_fin: Optional[date] = None
    plazo_dias: Optional[int] = None
    estado: str = "planificada"
    notas: Optional[str] = None


@obras_router.get("")
def listar_obras(authorization: str | None = Header(default=None)):
    """Lista de obras con KPIs (desde v_obra_resumen)."""
    eid = _ctx(authorization)
    res = _sb.table("v_obra_resumen").select("*").eq("empresa_id", eid).execute()
    filas = res.data or []
    filas.sort(key=lambda r: (r.get("estado") != "en_ejecucion", r.get("nombre") or ""))
    return filas


@obras_router.post("")
def crear_obra(body: ObraIn, authorization: str | None = Header(default=None)):
    eid = _ctx(authorization)
    if body.estado not in ESTADOS_OBRA:
        raise HTTPException(400, f"Estado inválido. Usa uno de: {sorted(ESTADOS_OBRA)}")
    data = _limpiar(body.model_dump())
    data["empresa_id"] = eid
    res = _sb.table("obras").insert(data).execute()
    return res.data[0]


@obras_router.get("/{obra_id}")
def obtener_obra(obra_id: str, authorization: str | None = Header(default=None)):
    eid = _ctx(authorization)
    return _obra_de(eid, obra_id)


@obras_router.put("/{obra_id}")
def actualizar_obra(obra_id: str, body: ObraIn, authorization: str | None = Header(default=None)):
    eid = _ctx(authorization)
    _obra_de(eid, obra_id)
    if body.estado not in ESTADOS_OBRA:
        raise HTTPException(400, f"Estado inválido. Usa uno de: {sorted(ESTADOS_OBRA)}")
    data = _limpiar(body.model_dump())
    data["actualizado_en"] = datetime.utcnow().isoformat()
    res = _sb.table("obras").update(data).eq("id", obra_id).eq("empresa_id", eid).execute()
    return res.data[0]


@obras_router.delete("/{obra_id}")
def eliminar_obra(obra_id: str, authorization: str | None = Header(default=None)):
    eid = _ctx(authorization)
    _obra_de(eid, obra_id)
    _sb.table("obras").delete().eq("id", obra_id).eq("empresa_id", eid).execute()
    return {"ok": True}


# ══════════════════════════════════════════════════════════════
# 2. PARTIDAS CONTRATADAS
# ══════════════════════════════════════════════════════════════

class PartidaIn(BaseModel):
    codigo: Optional[str] = None
    capitulo: Optional[str] = None
    descripcion: str
    unidad: Optional[str] = None
    cantidad: float = 0
    precio_unitario: float = 0
    costo_unitario_apu: Optional[float] = None
    orden: Optional[int] = None


class PartidasBulk(BaseModel):
    """Carga masiva: pegas el listado de cantidades de la oferta.
    reemplazar=True borra las partidas existentes (solo si la obra no tiene cubicaciones)."""
    partidas: List[PartidaIn]
    reemplazar: bool = False


@obras_router.get("/{obra_id}/partidas")
def listar_partidas(obra_id: str, authorization: str | None = Header(default=None)):
    eid = _ctx(authorization)
    _obra_de(eid, obra_id)
    res = _sb.table("obra_partidas").select("*").eq("obra_id", obra_id).order("orden").execute()
    return res.data or []


@obras_router.post("/{obra_id}/partidas")
def cargar_partidas(obra_id: str, body: PartidasBulk, authorization: str | None = Header(default=None)):
    eid = _ctx(authorization)
    _obra_de(eid, obra_id)
    if not body.partidas:
        raise HTTPException(400, "No hay partidas que cargar")

    if body.reemplazar:
        cubs = _sb.table("obra_cubicaciones").select("id").eq("obra_id", obra_id).limit(1).execute()
        if cubs.data:
            raise HTTPException(409, "La obra ya tiene cubicaciones; no se puede reemplazar el listado. Edita partida por partida.")
        _sb.table("obra_partidas").delete().eq("obra_id", obra_id).execute()
        base = 0
    else:
        ult = _sb.table("obra_partidas").select("orden").eq("obra_id", obra_id).order("orden", desc=True).limit(1).execute()
        base = (ult.data[0]["orden"] + 1) if ult.data else 0

    filas = []
    for i, p in enumerate(body.partidas):
        d = _limpiar(p.model_dump())
        d["empresa_id"] = eid
        d["obra_id"] = obra_id
        if p.orden is None:
            d["orden"] = base + i
        filas.append(d)

    res = _sb.table("obra_partidas").insert(filas).execute()
    return {"ok": True, "insertadas": len(res.data or [])}


@obras_router.put("/{obra_id}/partidas/{partida_id}")
def actualizar_partida(obra_id: str, partida_id: str, body: PartidaIn, authorization: str | None = Header(default=None)):
    eid = _ctx(authorization)
    _obra_de(eid, obra_id)
    data = _limpiar(body.model_dump())
    data["actualizado_en"] = datetime.utcnow().isoformat()
    res = _sb.table("obra_partidas").update(data).eq("id", partida_id).eq("obra_id", obra_id).execute()
    if not res.data:
        raise HTTPException(404, "Partida no encontrada")
    return res.data[0]


@obras_router.delete("/{obra_id}/partidas/{partida_id}")
def eliminar_partida(obra_id: str, partida_id: str, authorization: str | None = Header(default=None)):
    eid = _ctx(authorization)
    _obra_de(eid, obra_id)
    res = _sb.table("obra_partidas").delete().eq("id", partida_id).eq("obra_id", obra_id).execute()
    if not res.data:
        raise HTTPException(404, "Partida no encontrada")
    return {"ok": True}


# ══════════════════════════════════════════════════════════════
# 3. CUBICACIONES
# ══════════════════════════════════════════════════════════════

class DetalleIn(BaseModel):
    partida_id: str
    cantidad: float


class CubicacionIn(BaseModel):
    periodo_desde: Optional[date] = None
    periodo_hasta: Optional[date] = None
    estado: str = "borrador"
    notas: Optional[str] = None
    detalle: List[DetalleIn] = Field(default_factory=list)


class CubicacionEstado(BaseModel):
    estado: str
    monto_aprobado: Optional[float] = None
    fecha_pago: Optional[date] = None


def _monto_detalle(obra_id: str, detalle: List[DetalleIn]) -> float:
    """Suma cantidad × PU de la obra para el detalle enviado."""
    if not detalle:
        return 0.0
    ids = [d.partida_id for d in detalle]
    res = _sb.table("obra_partidas").select("id,precio_unitario").eq("obra_id", obra_id).in_("id", ids).execute()
    pu = {r["id"]: float(r["precio_unitario"] or 0) for r in (res.data or [])}
    faltan = [i for i in ids if i not in pu]
    if faltan:
        raise HTTPException(400, f"Partidas que no pertenecen a esta obra: {faltan[:3]}")
    return round(sum(float(d.cantidad) * pu[d.partida_id] for d in detalle), 2)


@obras_router.get("/{obra_id}/cubicaciones")
def listar_cubicaciones(obra_id: str, authorization: str | None = Header(default=None)):
    eid = _ctx(authorization)
    _obra_de(eid, obra_id)
    res = _sb.table("obra_cubicaciones").select("*").eq("obra_id", obra_id).order("numero").execute()
    return res.data or []


@obras_router.get("/{obra_id}/cubicaciones/{cub_id}")
def obtener_cubicacion(obra_id: str, cub_id: str, authorization: str | None = Header(default=None)):
    eid = _ctx(authorization)
    _obra_de(eid, obra_id)
    cab = _sb.table("obra_cubicaciones").select("*").eq("id", cub_id).eq("obra_id", obra_id).execute()
    if not cab.data:
        raise HTTPException(404, "Cubicación no encontrada")
    det = _sb.table("obra_cubicacion_detalle").select("*, obra_partidas(codigo,descripcion,unidad,precio_unitario)") \
        .eq("cubicacion_id", cub_id).execute()
    return {**cab.data[0], "detalle": det.data or []}


@obras_router.post("/{obra_id}/cubicaciones")
def crear_cubicacion(obra_id: str, body: CubicacionIn, authorization: str | None = Header(default=None)):
    eid = _ctx(authorization)
    _obra_de(eid, obra_id)
    if body.estado not in ESTADOS_CUB:
        raise HTTPException(400, f"Estado inválido. Usa uno de: {sorted(ESTADOS_CUB)}")

    ult = _sb.table("obra_cubicaciones").select("numero").eq("obra_id", obra_id).order("numero", desc=True).limit(1).execute()
    numero = (ult.data[0]["numero"] + 1) if ult.data else 1
    monto = _monto_detalle(obra_id, body.detalle)

    cab = {
        "empresa_id": eid, "obra_id": obra_id, "numero": numero,
        "estado": body.estado, "monto_presentado": monto, "notas": body.notas,
    }
    if body.periodo_desde: cab["periodo_desde"] = body.periodo_desde.isoformat()
    if body.periodo_hasta: cab["periodo_hasta"] = body.periodo_hasta.isoformat()
    cab = _limpiar(cab)
    res = _sb.table("obra_cubicaciones").insert(cab).execute()
    cub = res.data[0]

    if body.detalle:
        filas = [{"cubicacion_id": cub["id"], "partida_id": d.partida_id, "cantidad": d.cantidad} for d in body.detalle]
        _sb.table("obra_cubicacion_detalle").insert(filas).execute()

    return {**cub, "detalle": body.detalle}


@obras_router.put("/{obra_id}/cubicaciones/{cub_id}")
def actualizar_cubicacion(obra_id: str, cub_id: str, body: CubicacionIn, authorization: str | None = Header(default=None)):
    """Reemplaza el detalle completo (solo en borrador)."""
    eid = _ctx(authorization)
    _obra_de(eid, obra_id)
    cab = _sb.table("obra_cubicaciones").select("estado").eq("id", cub_id).eq("obra_id", obra_id).execute()
    if not cab.data:
        raise HTTPException(404, "Cubicación no encontrada")
    if cab.data[0]["estado"] != "borrador":
        raise HTTPException(409, "Solo se edita el detalle de una cubicación en borrador")

    monto = _monto_detalle(obra_id, body.detalle)
    upd = _limpiar({
        "periodo_desde": body.periodo_desde, "periodo_hasta": body.periodo_hasta,
        "notas": body.notas, "monto_presentado": monto,
        "actualizado_en": datetime.utcnow(),
    })
    _sb.table("obra_cubicaciones").update(upd).eq("id", cub_id).execute()
    _sb.table("obra_cubicacion_detalle").delete().eq("cubicacion_id", cub_id).execute()
    if body.detalle:
        filas = [{"cubicacion_id": cub_id, "partida_id": d.partida_id, "cantidad": d.cantidad} for d in body.detalle]
        _sb.table("obra_cubicacion_detalle").insert(filas).execute()
    return {"ok": True, "monto_presentado": monto}


@obras_router.patch("/{obra_id}/cubicaciones/{cub_id}/estado")
def cambiar_estado_cubicacion(obra_id: str, cub_id: str, body: CubicacionEstado, authorization: str | None = Header(default=None)):
    """borrador -> presentada -> aprobada -> pagada. Con monto_aprobado si la entidad recortó."""
    eid = _ctx(authorization)
    _obra_de(eid, obra_id)
    if body.estado not in ESTADOS_CUB:
        raise HTTPException(400, f"Estado inválido. Usa uno de: {sorted(ESTADOS_CUB)}")
    upd = _limpiar({
        "estado": body.estado, "monto_aprobado": body.monto_aprobado,
        "fecha_pago": body.fecha_pago, "actualizado_en": datetime.utcnow(),
    })
    res = _sb.table("obra_cubicaciones").update(upd).eq("id", cub_id).eq("obra_id", obra_id).execute()
    if not res.data:
        raise HTTPException(404, "Cubicación no encontrada")
    return res.data[0]


@obras_router.delete("/{obra_id}/cubicaciones/{cub_id}")
def eliminar_cubicacion(obra_id: str, cub_id: str, authorization: str | None = Header(default=None)):
    eid = _ctx(authorization)
    _obra_de(eid, obra_id)
    res = _sb.table("obra_cubicaciones").delete().eq("id", cub_id).eq("obra_id", obra_id).eq("estado", "borrador").execute()
    if not res.data:
        raise HTTPException(409, "Solo se elimina una cubicación en borrador")
    return {"ok": True}


# ══════════════════════════════════════════════════════════════
# 4. GASTOS (costo real)
# ══════════════════════════════════════════════════════════════

class GastoIn(BaseModel):
    fecha: Optional[date] = None
    tipo: str = "material"
    partida_id: Optional[str] = None       # obligatoria al crear/editar desde la app
    proveedor: Optional[str] = None
    rnc: Optional[str] = None
    descripcion: str
    documento: Optional[str] = None        # NCF / No. factura / conduce
    monto: float
    itbis: float = 0
    pagado: bool = False
    archivo_path: Optional[str] = None     # foto/PDF de la factura en Storage
    origen: Optional[str] = None           # manual | foto | excel


def _validar_gastos(obra_id: str, eid: str, gastos: List["GastoIn"]):
    """Cada gasto debe ir a una partida de esta obra y con tipo válido."""
    ids = {g.partida_id for g in gastos if g.partida_id}
    if any(not g.partida_id for g in gastos):
        raise HTTPException(400, "Cada gasto debe estar asignado a una partida")
    res = _sb.table("obra_partidas").select("id").eq("obra_id", obra_id).in_("id", list(ids)).execute()
    validas = {r["id"] for r in (res.data or [])}
    if ids - validas:
        raise HTTPException(400, "Hay gastos asignados a partidas que no son de esta obra")
    for g in gastos:
        if g.tipo not in TIPOS_GASTO:
            raise HTTPException(400, f"Tipo inválido. Usa uno de: {sorted(TIPOS_GASTO)}")
        if g.archivo_path and not g.archivo_path.startswith(f"{eid}/obras/{obra_id}/"):
            raise HTTPException(400, "Archivo de factura inválido")


@obras_router.get("/{obra_id}/gastos")
def listar_gastos(obra_id: str, authorization: str | None = Header(default=None)):
    eid = _ctx(authorization)
    _obra_de(eid, obra_id)
    res = _sb.table("obra_gastos").select("*, obra_partidas(codigo,descripcion)") \
        .eq("obra_id", obra_id).order("fecha", desc=True).execute()
    return res.data or []


@obras_router.post("/{obra_id}/gastos")
def crear_gasto(obra_id: str, body: GastoIn, authorization: str | None = Header(default=None)):
    eid = _ctx(authorization)
    _obra_de(eid, obra_id)
    _validar_gastos(obra_id, eid, [body])
    data = _limpiar(body.model_dump())
    data.setdefault("origen", "manual")
    data["empresa_id"] = eid
    data["obra_id"] = obra_id
    res = _sb.table("obra_gastos").insert(data).execute()
    return res.data[0]


class GastosLote(BaseModel):
    gastos: List[GastoIn]


@obras_router.post("/{obra_id}/gastos/lote")
def crear_gastos_lote(obra_id: str, body: GastosLote, authorization: str | None = Header(default=None)):
    """Guarda varios gastos de una vez (renglones de una factura o filas de un Excel)."""
    eid = _ctx(authorization)
    _obra_de(eid, obra_id)
    if not body.gastos:
        raise HTTPException(400, "No hay gastos que guardar")
    _validar_gastos(obra_id, eid, body.gastos)
    filas = []
    for g in body.gastos:
        d = _limpiar(g.model_dump())
        d.setdefault("origen", "manual")
        d["empresa_id"] = eid
        d["obra_id"] = obra_id
        filas.append(d)
    insertados = 0
    for i in range(0, len(filas), 500):
        res = _sb.table("obra_gastos").insert(filas[i:i + 500]).execute()
        insertados += len(res.data or [])
    return {"ok": True, "insertados": insertados}


@obras_router.put("/{obra_id}/gastos/{gasto_id}")
def actualizar_gasto(obra_id: str, gasto_id: str, body: GastoIn, authorization: str | None = Header(default=None)):
    eid = _ctx(authorization)
    _obra_de(eid, obra_id)
    _validar_gastos(obra_id, eid, [body])
    data = _limpiar(body.model_dump())
    data["actualizado_en"] = datetime.utcnow().isoformat()
    res = _sb.table("obra_gastos").update(data).eq("id", gasto_id).eq("obra_id", obra_id).execute()
    if not res.data:
        raise HTTPException(404, "Gasto no encontrado")
    return res.data[0]


@obras_router.delete("/{obra_id}/gastos/{gasto_id}")
def eliminar_gasto(obra_id: str, gasto_id: str, authorization: str | None = Header(default=None)):
    eid = _ctx(authorization)
    _obra_de(eid, obra_id)
    res = _sb.table("obra_gastos").delete().eq("id", gasto_id).eq("obra_id", obra_id).execute()
    if not res.data:
        raise HTTPException(404, "Gasto no encontrado")
    return {"ok": True}


# ══════════════════════════════════════════════════════════════
# 5. TABLERO (lo que ADPRO muestra como "control presupuestal")
# ══════════════════════════════════════════════════════════════

@obras_router.get("/{obra_id}/tablero")
def tablero(obra_id: str, authorization: str | None = Header(default=None)):
    eid = _ctx(authorization)
    obra = _obra_de(eid, obra_id)

    res = _sb.table("v_obra_resumen").select("*").eq("obra_id", obra_id).execute()
    resumen = res.data[0] if res.data else {}

    part = _sb.table("v_obra_partida_estado").select("*").eq("obra_id", obra_id).order("orden").execute()
    partidas = part.data or []

    presupuesto = float(resumen.get("presupuesto") or 0)
    ejecutado = float(resumen.get("ejecutado") or 0)
    directo = float(resumen.get("gastado_directo") or 0)
    indirecto = float(resumen.get("gastado_indirecto") or 0)
    gastado = directo + indirecto
    cobrado = float(resumen.get("cobrado") or 0)

    # Partidas que ya consumen más de lo que han producido (alerta temprana)
    alertas = [
        {"partida_id": p["partida_id"], "codigo": p["codigo"], "descripcion": p["descripcion"],
         "margen": p["margen"], "avance_pct": p["avance_pct"]}
        for p in partidas
        if float(p.get("gastado") or 0) > float(p.get("monto_ejecutado") or 0) and float(p.get("gastado") or 0) > 0
    ]

    # Agrupar por capítulo para el resumen ejecutivo
    capitulos = {}
    for p in partidas:
        k = p.get("capitulo") or "SIN CAPÍTULO"
        c = capitulos.setdefault(k, {"capitulo": k, "contratado": 0, "ejecutado": 0, "gastado": 0})
        c["contratado"] += float(p.get("monto_contratado") or 0)
        c["ejecutado"] += float(p.get("monto_ejecutado") or 0)
        c["gastado"] += float(p.get("gastado") or 0)
    for c in capitulos.values():
        c["avance_pct"] = round(c["ejecutado"] / c["contratado"] * 100, 2) if c["contratado"] else 0
        c["margen"] = round(c["ejecutado"] - c["gastado"], 2)

    return {
        "obra": obra,
        "kpis": {
            "presupuesto": presupuesto,
            "monto_contrato": float(obra.get("monto_contrato") or 0),
            "ejecutado": ejecutado,
            "avance_financiero_pct": float(resumen.get("avance_financiero_pct") or 0),
            "gastado": gastado,
            "gastado_directo": directo,
            "gastado_indirecto": indirecto,
            "margen_bruto": round(ejecutado - gastado, 2),
            "margen_pct": round((ejecutado - gastado) / ejecutado * 100, 2) if ejecutado else 0,
            "cobrado": cobrado,
            "por_cobrar": round(ejecutado - cobrado, 2),
            "por_ejecutar": round(presupuesto - ejecutado, 2),
        },
        "capitulos": list(capitulos.values()),
        "partidas": partidas,
        "alertas": alertas,
    }


# ══════════════════════════════════════════════════════════════
# 6. CARGA AUTOMÁTICA: Excel de presupuesto, Excel de gastos y foto de factura
#    Nada se guarda aquí: se devuelve una vista previa que el usuario confirma
#    con POST /partidas o POST /gastos/lote.
# ══════════════════════════════════════════════════════════════

MAX_ARCHIVO = 25 * 1024 * 1024
STORAGE_BUCKET = "bid-manager-docs"
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
GEMINI_MODEL = os.getenv("GEMINI_MODEL", "gemini-2.5-flash")


async def _leer_archivo(file: UploadFile) -> bytes:
    contenido = await file.read()
    if not contenido:
        raise HTTPException(400, "El archivo está vacío")
    if len(contenido) > MAX_ARCHIVO:
        raise HTTPException(413, "El archivo pasa de 25 MB")
    return contenido


def _mapeo(txt: Optional[str]) -> Optional[dict]:
    if not txt:
        return None
    try:
        d = json.loads(txt)
        return d if isinstance(d, dict) else None
    except Exception:
        raise HTTPException(400, "Mapeo de columnas inválido")


def _gemini_json(partes: list, timeout: int = 90) -> Optional[dict]:
    """Llama a Gemini y devuelve el JSON de la respuesta, o None si no está configurado o falla."""
    if not GEMINI_API_KEY:
        return None
    payload = {
        "contents": [{"parts": partes}],
        "generationConfig": {"temperature": 0.1, "response_mime_type": "application/json"},
    }
    url = (f"https://generativelanguage.googleapis.com/v1beta/models/"
           f"{GEMINI_MODEL}:generateContent?key={GEMINI_API_KEY}")
    try:
        req = urllib.request.Request(url, data=json.dumps(payload).encode(),
                                     headers={"Content-Type": "application/json"}, method="POST")
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            body = json.loads(resp.read().decode())
        texto = body["candidates"][0]["content"]["parts"][0]["text"].strip()
        if texto.startswith("```"):
            texto = texto.strip("`")
            if texto.lower().startswith("json"):
                texto = texto[4:]
        return json.loads(texto)
    except Exception as e:
        print(f"[obras] Gemini falló: {e}")
        return None


def _partidas_obra(obra_id: str) -> list:
    res = _sb.table("obra_partidas").select("id,codigo,capitulo,descripcion,unidad") \
        .eq("obra_id", obra_id).order("orden").execute()
    return res.data or []


def _catalogo(partidas: list) -> str:
    """Listado compacto para el prompt: P1 | 1.01 | capítulo | descripción."""
    return "\n".join(
        f"P{i + 1} | {p.get('codigo') or ''} | {(p.get('capitulo') or '')[:50]} | {(p.get('descripcion') or '')[:160]}"
        for i, p in enumerate(partidas))


def _sugerencias(textos: List[str], partidas: list, usar_ia: bool = True) -> List[dict]:
    """Por texto: {partida_id, alternativas[], fuente}. IA si hay clave; si no, por palabras."""
    por_palabras = obras_import.sugerir_partidas(textos, partidas)
    ia = {}
    if usar_ia and GEMINI_API_KEY and partidas and textos:
        lineas = "\n".join(f"G{i + 1}: {t[:200]}" for i, t in enumerate(textos[:300]))
        prompt = f"""Eres ingeniero de costos de una constructora dominicana.
Asigna cada gasto de obra a la partida del presupuesto que lo consume (material, mano de obra o equipo de esa partida).
Si un gasto es claramente indirecto (seguros, oficina, dirección técnica, transporte general) asígnalo a la partida indirecta que corresponda si existe.
Si no hay ninguna razonable, usa null.

PARTIDAS (id | código | capítulo | descripción):
{_catalogo(partidas)}

GASTOS:
{lineas}

Responde SOLO JSON: {{"G1": "P12", "G2": null, ...}}"""
        r = _gemini_json([{"text": prompt}], timeout=60) or {}
        for k, v in r.items():
            try:
                gi = int(str(k).lstrip("Gg")) - 1
                pi = int(str(v).lstrip("Pp")) - 1 if v else None
            except (ValueError, TypeError):
                continue
            if 0 <= gi < len(textos) and pi is not None and 0 <= pi < len(partidas):
                ia[gi] = partidas[pi]["id"]
    out = []
    for i, alts in enumerate(por_palabras):
        pid = ia.get(i) or (alts[0]["partida_id"] if alts else None)
        out.append({"partida_id": pid, "fuente": "ia" if i in ia else ("palabras" if alts else None),
                    "alternativas": alts})
    return out


@obras_router.post("/{obra_id}/partidas/importar")
async def importar_partidas_excel(
    obra_id: str,
    file: UploadFile = File(...),
    hoja: Optional[str] = Form(default=None),
    mapeo: Optional[str] = Form(default=None),
    authorization: str | None = Header(default=None),
):
    """Lee el Excel del presupuesto y devuelve las partidas detectadas (no guarda)."""
    eid = _ctx(authorization)
    _obra_de(eid, obra_id)
    contenido = await _leer_archivo(file)
    try:
        return obras_import.leer_presupuesto(contenido, file.filename or "", hoja or None, _mapeo(mapeo))
    except ValueError as e:
        raise HTTPException(400, str(e))
    except Exception as e:
        raise HTTPException(400, f"No pude leer el Excel: {e}")


@obras_router.post("/{obra_id}/gastos/importar")
async def importar_gastos_excel(
    obra_id: str,
    file: UploadFile = File(...),
    hoja: Optional[str] = Form(default=None),
    mapeo: Optional[str] = Form(default=None),
    authorization: str | None = Header(default=None),
):
    """Lee una relación de gastos en Excel y sugiere la partida de cada fila (no guarda)."""
    eid = _ctx(authorization)
    _obra_de(eid, obra_id)
    contenido = await _leer_archivo(file)
    try:
        r = obras_import.leer_gastos_excel(contenido, file.filename or "", hoja or None, _mapeo(mapeo))
    except ValueError as e:
        raise HTTPException(400, str(e))
    except Exception as e:
        raise HTTPException(400, f"No pude leer el Excel: {e}")

    partidas = _partidas_obra(obra_id)
    por_codigo = {(p.get("codigo") or "").strip(): p["id"] for p in partidas if p.get("codigo")}
    textos = [f"{g['descripcion']} · {g.get('proveedor') or ''}" for g in r["gastos"]]
    pendientes = [i for i, g in enumerate(r["gastos"]) if not por_codigo.get(g.get("partida_codigo") or "")]
    sug = _sugerencias([textos[i] for i in pendientes], partidas) if pendientes else []
    sug_por_fila = dict(zip(pendientes, sug))
    for i, g in enumerate(r["gastos"]):
        if i in sug_por_fila:
            g.update(sug_por_fila[i])
        else:
            g.update({"partida_id": por_codigo[g["partida_codigo"]], "fuente": "excel", "alternativas": []})
    r["ia"] = bool(GEMINI_API_KEY)
    return r


_PROMPT_FACTURA = """Eres asistente contable de una constructora en República Dominicana.
La imagen es una factura, conduce, recibo o cotización pagada de un gasto de obra.
Extrae los datos y asigna cada renglón a la partida del presupuesto que lo consume.

PARTIDAS DEL PRESUPUESTO (id | código | capítulo | descripción):
{catalogo}

Responde SOLO con JSON válido con esta estructura:
{{
  "proveedor": "nombre comercial o razón social",
  "rnc": "RNC o cédula del proveedor, solo dígitos y guiones",
  "ncf": "NCF (B01..., E31...) o número de factura si no hay NCF",
  "fecha": "AAAA-MM-DD",
  "subtotal": 0,
  "itbis": 0,
  "total": 0,
  "tipo": "material | mano_obra | equipo | subcontrato | transporte | indirecto",
  "items": [
    {{"descripcion": "texto del renglón", "cantidad": 0, "unidad": "", "importe": 0, "partida": "P12"}}
  ],
  "confianza": "alta | media | baja",
  "notas": "algo dudoso, ilegible o que no cuadre"
}}
Reglas:
- Números sin comas ni símbolos (1500.50).
- "importe" de cada renglón SIN ITBIS. Si la factura solo trae precios con ITBIS, quítaselo.
- Si no se distinguen renglones, pon un solo item con el subtotal.
- "partida" = id de la lista (P1, P2...) o null si ninguna aplica.
- Fechas dominicanas vienen como DD/MM/AAAA."""


@obras_router.post("/{obra_id}/gastos/factura")
async def leer_factura(
    obra_id: str,
    file: UploadFile = File(...),
    authorization: str | None = Header(default=None),
):
    """Guarda la foto/PDF de la factura y devuelve los datos leídos con la partida sugerida por renglón."""
    eid = _ctx(authorization)
    _obra_de(eid, obra_id)
    contenido = await _leer_archivo(file)
    mime = (file.content_type or "").lower()
    nombre = (file.filename or "factura").lower()
    if not mime or mime == "application/octet-stream":
        mime = "application/pdf" if nombre.endswith(".pdf") else "image/jpeg"
    if not (mime.startswith("image/") or mime == "application/pdf"):
        raise HTTPException(400, "Sube una foto (JPG, PNG) o un PDF de la factura")

    ext = ".pdf" if mime == "application/pdf" else ("." + mime.split("/")[1].replace("jpeg", "jpg"))
    path = f"{eid}/obras/{obra_id}/gastos/{uuid.uuid4().hex}{ext}"
    try:
        _sb.storage.from_(STORAGE_BUCKET).upload(path=path, file=contenido,
                                                 file_options={"content-type": mime, "upsert": "false"})
    except Exception as e:
        print(f"[obras] no se pudo guardar la factura: {e}")
        path = None

    partidas = _partidas_obra(obra_id)
    if not GEMINI_API_KEY:
        raise HTTPException(503, "La lectura de facturas no está configurada (falta GEMINI_API_KEY)")
    datos = _gemini_json([
        {"inline_data": {"mime_type": mime, "data": base64.b64encode(contenido).decode()}},
        {"text": _PROMPT_FACTURA.format(catalogo=_catalogo(partidas) or "(sin partidas cargadas)")},
    ], timeout=120)
    if not datos:
        raise HTTPException(502, "No pude leer la factura. Prueba con una foto más clara o cárgala a mano.")

    def _n(v):
        try:
            return round(float(str(v).replace(",", "")), 2)
        except (TypeError, ValueError):
            return 0.0

    items = datos.get("items") or []
    if not items:
        items = [{"descripcion": datos.get("proveedor") or "Factura", "importe": datos.get("subtotal") or datos.get("total")}]
    textos = [str(it.get("descripcion") or "") for it in items]
    por_palabras = obras_import.sugerir_partidas(textos, partidas)
    salida = []
    for it, alts in zip(items, por_palabras):
        pid = None
        try:
            k = int(str(it.get("partida") or "").lstrip("Pp")) - 1
            if 0 <= k < len(partidas):
                pid = partidas[k]["id"]
        except ValueError:
            pass
        salida.append({
            "descripcion": str(it.get("descripcion") or "").strip()[:300],
            "cantidad": _n(it.get("cantidad")) or None,
            "unidad": (it.get("unidad") or None),
            "importe": _n(it.get("importe")),
            "partida_id": pid or (alts[0]["partida_id"] if alts else None),
            "fuente": "ia" if pid else ("palabras" if alts else None),
            "alternativas": alts,
        })
    tipo = datos.get("tipo") if datos.get("tipo") in TIPOS_GASTO else obras_import.tipo_por_texto(" ".join(textos))
    return {
        "archivo_path": path,
        "proveedor": datos.get("proveedor"),
        "rnc": datos.get("rnc"),
        "documento": datos.get("ncf"),
        "fecha": obras_import._fecha(datos.get("fecha")) or None,
        "subtotal": _n(datos.get("subtotal")),
        "itbis": _n(datos.get("itbis")),
        "total": _n(datos.get("total")),
        "tipo": tipo,
        "items": salida,
        "confianza": datos.get("confianza"),
        "notas": datos.get("notas"),
    }


@obras_router.get("/{obra_id}/gastos/{gasto_id}/archivo")
def archivo_gasto(obra_id: str, gasto_id: str, authorization: str | None = Header(default=None)):
    """URL temporal para ver la foto/PDF de la factura."""
    eid = _ctx(authorization)
    _obra_de(eid, obra_id)
    res = _sb.table("obra_gastos").select("archivo_path").eq("id", gasto_id).eq("obra_id", obra_id).execute()
    if not res.data or not res.data[0].get("archivo_path"):
        raise HTTPException(404, "Este gasto no tiene factura adjunta")
    path = res.data[0]["archivo_path"]
    try:
        r = _sb.storage.from_(STORAGE_BUCKET).create_signed_url(path, 3600)
        url = (r or {}).get("signedURL") or (r or {}).get("signed_url") if isinstance(r, dict) else \
            getattr(r, "signed_url", None) or getattr(r, "signedURL", None)
    except Exception as e:
        raise HTTPException(500, f"No pude abrir la factura: {e}")
    if not url:
        raise HTTPException(500, "No pude abrir la factura")
    return {"url": url}
