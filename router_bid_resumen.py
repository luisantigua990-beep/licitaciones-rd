"""
router_bid_resumen.py — Resumen del Expediente v2 + preferencias (Fase 1)
==========================================================================
Regla D1: la completitud ({cumplidos, total, pct}) y las metas de cada
sección (regla D4) las calcula SOLO este backend. El frontend pinta.

Endpoints (path distinto al /api/bid/expediente/resumen viejo, que sigue
funcionando para la UI actual — rollout sin romper nada):

  GET  /api/bid/expediente/resumen-v2?referencia=LPN-...
  GET  /api/bid/expediente/prefs
  PATCH /api/bid/expediente/prefs      {"lente": true, "densidad": "compacta", ...}

Integración en main.py (2 líneas):
    from router_bid_resumen import bid_resumen_router
    app.include_router(bid_resumen_router)
"""

from datetime import datetime, timezone

from fastapi import APIRouter, HTTPException, Header

from router_bid_manager import _auth, _empresa_id, _sb
from expediente_core import registrar_auditoria

bid_resumen_router = APIRouter(prefix="/api/bid/expediente", tags=["bid-resumen"])


# ── Pesos de cada sección en la completitud global (suman 100) ──
PESOS = {
    "empresa": 20, "documentos": 20, "personal": 15,
    "representantes": 10, "equipos": 10, "experiencia": 10,
    "financieros": 10, "socios": 5,
}

# Campos de perfiles_empresa que alimentan los formularios SNCC.
# Si falta uno, la sección Empresa no llega a 100 y la meta lo dice.
# Cada entrada: (campos equivalentes — basta con que UNO esté lleno, etiqueta)
CAMPOS_EMPRESA_CLAVE = [
    (("razon_social",), "Razón social"),
    (("rnc",), "RNC"),
    (("rpe",), "RPE"),
    (("email_empresa",), "Correo institucional"),
    (("telefono_principal", "telefono"), "Teléfono"),
    (("direccion_completa", "domicilio", "direccion"), "Dirección"),
    (("clasificacion_mipyme",), "Clasificación MIPYME"),
    (("fecha_constitucion",), "Fecha de constitución"),
]

PREFS_PERMITIDAS = {"lente", "densidad", "proceso_activo", "seccion_activa"}


def _count(tabla: str, eid: str, extra=None) -> int:
    q = _sb.table(tabla).select("id", count="exact").eq("empresa_id", eid)
    if extra:
        for k, v in extra.items():
            q = q.in_(k, v) if isinstance(v, list) else q.eq(k, v)
    return q.execute().count or 0


def _sec(id_, count, pct, kind, meta):
    return {"id": id_, "count": count, "pct": max(0, min(100, round(pct))),
            "kind": kind, "meta": meta}


@bid_resumen_router.get("/resumen-v2")
def resumen_v2(referencia: str | None = None,
               authorization: str | None = Header(default=None)):
    uid = _auth(authorization)
    eid = _empresa_id(uid)

    # ── Empresa: campos clave llenos ──
    emp_rows = (_sb.table("perfiles_empresa").select("*")
                .eq("id", eid).limit(1).execute().data or [{}])
    emp = emp_rows[0]
    faltan = [label for campos, label in CAMPOS_EMPRESA_CLAVE
              if not any(emp.get(c) for c in campos)]
    llenos = len(CAMPOS_EMPRESA_CLAVE) - len(faltan)
    sec_empresa = _sec(
        "empresa", len(CAMPOS_EMPRESA_CLAVE),
        llenos / len(CAMPOS_EMPRESA_CLAVE) * 100,
        "ok" if not faltan else "warn",
        "Datos completos" if not faltan
        else f"{len(faltan)} campo{'s' if len(faltan) > 1 else ''} incompleto{'s' if len(faltan) > 1 else ''}: {', '.join(faltan[:3])}",
    )

    # ── Documentos: vencidos / por vencer mandan el tono ──
    try:  # sincroniza estados antes de contar (RPC existente)
        _sb.rpc("fn_actualizar_estados_certificaciones", {"p_empresa_id": eid}).execute()
    except Exception:
        pass
    docs = _count("bid_certificaciones", eid)
    vencidos = _count("bid_certificaciones", eid, {"estado": "vencido"})
    por_vencer = _count("bid_certificaciones", eid, {"estado": "por_vencer"})
    partes = []
    if vencidos:
        partes.append(f"{vencidos} vencido{'s' if vencidos > 1 else ''}")
    if por_vencer:
        partes.append(f"{por_vencer} por vencer")
    sec_docs = _sec(
        "certificaciones", docs,
        0 if not docs else (100 if not vencidos and not por_vencer else (55 if vencidos else 75)),
        "err" if vencidos else ("warn" if por_vencer else ("ok" if docs else "warn")),
        " · ".join(partes) if partes else ("Al día" if docs else "Sin documentos aún"),
    )

    # ── Secciones por conteo ──
    def por_conteo(id_, tabla, singular, plural, minimo=1):
        n = _count(tabla, eid)
        pct = 100 if n >= minimo else 0
        meta = f"{n} {plural if n != 1 else singular}" if n else f"Sin {plural} aún"
        return _sec(id_, n, pct, "ok" if n >= minimo else "warn", meta)

    sec_repr = por_conteo("representantes", "bid_representantes", "registrado", "registrados")
    sec_socios = por_conteo("socios", "bid_socios", "socio", "socios")
    sec_personal = por_conteo("personal", "bid_personal", "profesional", "profesionales")
    sec_equipos = por_conteo("equipos", "bid_equipos", "equipo", "equipos")
    sec_exp = por_conteo("experiencia", "bid_experiencia", "obra", "obras")
    sec_fin = por_conteo("financieros", "bid_financieros", "período", "períodos")

    secciones = [sec_empresa, sec_repr, sec_socios, sec_docs,
                 sec_personal, sec_equipos, sec_exp, sec_fin]

    # ── Completitud global ponderada (D1: única fuente de verdad) ──
    clave_peso = {"certificaciones": "documentos"}
    total_pct = sum(
        s["pct"] * PESOS[clave_peso.get(s["id"], s["id"])] / 100
        for s in secciones
    )
    global_ = {"pct": round(total_pct), "secciones": secciones}

    # ── Parte relativa al proceso (matching ya calculado) ──
    proceso = None
    if referencia:
        rows = (_sb.table("bid_matching_pliegos").select("*")
                .eq("empresa_id", eid).eq("referencia", referencia)
                .order("creado_en", desc=True).limit(1).execute().data or [])
        if not rows:
            proceso = {"referencia": referencia, "analizado": False}
        else:
            m = rows[0]
            resultado = m.get("resultado") or {}
            secs = resultado.get("secciones") or {}
            evaluadas = [s for s in secs.values()
                         if isinstance(s, dict) and s.get("cumple") is not None]
            cumplidos = sum(1 for s in evaluadas if s.get("cumple"))
            total = len(evaluadas)
            faltantes = resultado.get("faltantes") or []
            MAPA_SECCION = (("personal", "personal"), ("equipo", "equipos"),
                            ("obra", "experiencia"), ("experiencia", "experiencia"),
                            ("crédito", "financieros"), ("credito", "financieros"),
                            ("liquidez", "financieros"), ("solvencia", "financieros"),
                            ("capital", "financieros"))
            bloqueos = []
            for f in faltantes[:5]:
                texto = f if isinstance(f, str) else str(f.get("detalle") or f)
                seccion = next((s for k, s in MAPA_SECCION if k in texto.lower()),
                               "certificaciones")
                bloqueos.append({"titulo": texto, "seccion": seccion})
            proceso = {
                "referencia": referencia,
                "analizado": True,
                "nombre": m.get("nombre_proceso"),
                "institucion": m.get("institucion"),
                "presupuesto_base": m.get("presupuesto_base"),
                "cumplidos": cumplidos,
                "total": total,
                "pct": round(cumplidos / total * 100) if total else None,
                "cumple_global": resultado.get("cumple_global"),
                "bloqueos": bloqueos,
            }

    return {"global": global_, "proceso": proceso}


# ── Preferencias de usuario (lente, densidad, proceso activo) ──

@bid_resumen_router.get("/prefs")
def obtener_prefs(authorization: str | None = Header(default=None)):
    uid = _auth(authorization)
    rows = (_sb.table("bid_user_prefs").select("clave, valor")
            .eq("user_id", uid).execute().data or [])
    return {r["clave"]: r["valor"] for r in rows}


@bid_resumen_router.patch("/prefs")
def actualizar_prefs(request: dict | None = None,
                     authorization: str | None = Header(default=None)):
    uid = _auth(authorization)
    _empresa_id(uid)  # exige perfil (mismo contrato que el resto del módulo)
    cambios = {k: v for k, v in (request or {}).items() if k in PREFS_PERMITIDAS}
    if not cambios:
        raise HTTPException(422, "Sin preferencias válidas. Permitidas: "
                            + ", ".join(sorted(PREFS_PERMITIDAS)))
    ahora = datetime.now(timezone.utc).isoformat()
    for clave, valor in cambios.items():
        if valor is None:
            # valor es NOT NULL en bid_user_prefs: null significa "limpiar la
            # pref" (ej. cerrar el proceso activo) → borramos la fila en vez
            # de upsert, que antes explotaba con 500 (violacion de constraint)
            _sb.table("bid_user_prefs").delete() \
                .eq("user_id", uid).eq("clave", clave).execute()
            continue
        _sb.table("bid_user_prefs").upsert({
            "user_id": uid, "clave": clave, "valor": valor,
            "actualizado_en": ahora,
        }, on_conflict="user_id,clave").execute()
    return {"ok": True, "aplicadas": sorted(cambios.keys())}
