#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
═══════════════════════════════════════════════════════════════
LICITACIONLAB · ETL InfoPago (DGCP)
API descubierta: https://infopagoservice.dgcp.gob.do/api
Sin autenticación — solo requiere headers de Origin/Referer.
Respuestas con formato: {"operation": bool, "message": str, "data": ...}

USO:
  python etl_infopago.py --explorar                  # 1º PASO: ver estructura real de cada endpoint
  python etl_infopago.py --modo ranking --periodo 2026
  python etl_infopago.py --modo facturas --rnc 130723354
  python etl_infopago.py --modo facturas --desde-contratos --limit 200
  python etl_infopago.py --modo proveedor --nombre CONSER

ENV requeridas (mismas de tus otros ETLs):
  SUPABASE_URL, SUPABASE_SERVICE_KEY
═══════════════════════════════════════════════════════════════
"""

import os
import sys
import json
import time
import argparse
from datetime import datetime

import requests

# ── Config ──────────────────────────────────────────────────────
BASE = "https://infopagoservice.dgcp.gob.do/api"
SUPABASE_URL = os.environ.get("SUPABASE_URL", "")
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_KEY", "")

HEADERS = {
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "es-ES,es;q=0.9",
    "Origin": "https://infopago.dgcp.gob.do",
    "Referer": "https://infopago.dgcp.gob.do/",
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                   "(KHTML, like Gecko) Chrome/148.0.0.0 Safari/537.36"),
}

SLEEP = 0.35          # pausa entre requests — buen ciudadano con el API público
TIMEOUT = 30
RETRIES = 3

session = requests.Session()
session.headers.update(HEADERS)


# ── HTTP con reintentos ─────────────────────────────────────────
def api_get(path, params=None):
    return _request("GET", path, params=params)


def api_post(path, body=None):
    return _request("POST", path, json_body=body)


def _request(method, path, params=None, json_body=None):
    url = f"{BASE}/{path}"
    for intento in range(1, RETRIES + 1):
        try:
            if method == "GET":
                r = session.get(url, params=params, timeout=TIMEOUT)
            else:
                r = session.post(url, json=json_body, timeout=TIMEOUT)
            if r.status_code == 200:
                j = r.json()
                if isinstance(j, dict) and "data" in j:
                    return j.get("data")
                return j
            print(f"  ⚠ {r.status_code} {url} (intento {intento})")
            if 400 <= r.status_code < 500:
                return None  # error de cliente: reintentar no ayuda (evita quemar minutos)
        except Exception as e:
            print(f"  ⚠ {type(e).__name__}: {e} (intento {intento})")
        time.sleep(1.5 * intento)
    return None


# ── Supabase REST helpers ───────────────────────────────────────
def sb_upsert(tabla, filas, on_conflict):
    if not filas:
        return 0
    if not SUPABASE_URL or not SUPABASE_KEY:
        print("  ✖ Faltan SUPABASE_URL / SUPABASE_SERVICE_KEY"); sys.exit(1)
    url = f"{SUPABASE_URL}/rest/v1/{tabla}?on_conflict={on_conflict}"
    h = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates,return=minimal",
    }
    r = requests.post(url, headers=h, data=json.dumps(filas, default=str), timeout=60)
    if r.status_code not in (200, 201, 204):
        print(f"  ✖ Supabase {r.status_code}: {r.text[:300]}")
        return 0
    return len(filas)


def sb_select(tabla, query):
    url = f"{SUPABASE_URL}/rest/v1/{tabla}?{query}"
    h = {"apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}"}
    r = requests.get(url, headers=h, timeout=60)
    return r.json() if r.status_code == 200 else []


def sb_patch(tabla, filtro, cambios):
    url = f"{SUPABASE_URL}/rest/v1/{tabla}?{filtro}"
    h = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "return=minimal",
    }
    r = requests.patch(url, headers=h, data=json.dumps(cambios, default=str), timeout=60)
    return r.status_code in (200, 204)


# ── Utilidades de mapeo flexible ────────────────────────────────
# Los nombres exactos de los campos del API se confirman con --explorar.
# pick() busca la primera clave que exista para tolerar variaciones.
def pick(d, *claves):
    if not isinstance(d, dict):
        return None
    lower = {k.lower(): v for k, v in d.items()}
    for c in claves:
        if c.lower() in lower and lower[c.lower()] not in (None, ""):
            return lower[c.lower()]
    return None


def parse_fecha(v):
    if not v:
        return None
    s = str(v)[:10]
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y"):
        try:
            return datetime.strptime(s, fmt).date().isoformat()
        except ValueError:
            continue
    return None


# ── MODO EXPLORAR: imprime la estructura real de cada endpoint ──
def modo_explorar(rnc_prueba, contrato_prueba):
    pruebas = [
        ("GET", "FacturaFiscal/GetFacturasContrato", {"contrato": contrato_prueba}, None),
        ("POST", "TrazabilidadPago/GetComprobantesFiscalesProveedores", None,
         {"documentoIdentidad": rnc_prueba, "periodo": "todos", "pageNumber": 1, "pageSize": 5}),
        ("POST", "Proveedor/GetReferenciaProveedor", None, {"type": "name", "value": "CONSER"}),
        ("GET", "TrazabilidadPago/GetMontoContratadoByInstitucion", {"numeroDocumento": rnc_prueba}, None),
        ("GET", "TrazabilidadPago/GetEstadisticaComprobantesProveedores", {"periodo": "todos"}, None),
        ("GET", "RankingInstitucional/GetRankingFacturasPagadasByInstitucion",
         {"periodo": "2026", "unidadCompraCode": "000240"}, None),
    ]
    for metodo, path, params, body in pruebas:
        print(f"\n{'═'*70}\n{metodo} {path}\n  params={params} body={body}")
        data = api_get(path, params) if metodo == "GET" else api_post(path, body)
        time.sleep(SLEEP)
        if data is None:
            print("  → SIN RESPUESTA / ERROR")
            continue
        muestra = data[:2] if isinstance(data, list) else data
        print(json.dumps(muestra, indent=2, ensure_ascii=False, default=str)[:2500])
        if isinstance(data, list):
            print(f"  → lista con {len(data)} elementos")


# ── MODO FACTURAS ───────────────────────────────────────────────
def normalizar_factura(f, contrato=None, rnc=None):
    fecha_emision = parse_fecha(pick(f, "fechaRegistro", "fechaRegistroFacturaFiscal", "fechaEmision", "fechaFactura"))
    fecha_pago = parse_fecha(pick(f, "fechaComprobanteConciliado", "fechaPago", "fechaConciliacion", "fechaComprobanteEntregado"))
    dias = None
    if fecha_emision and fecha_pago:
        dias = (datetime.fromisoformat(fecha_pago) - datetime.fromisoformat(fecha_emision)).days
    return {
        "comprobante_fiscal": pick(f, "comprobanteFiscal", "ncf", "comprobante", "numeroComprobante"),
        "contrato": pick(f, "contrato", "numeroContrato", "codigoContrato") or contrato,
        "rnc_proveedor": pick(f, "beneficiario", "rnc", "documentoIdentidad", "numeroDocumento", "rncProveedor") or rnc,
        "nombre_proveedor": pick(f, "proveedor", "nombreProveedor", "razonSocial"),
        "institucion": pick(f, "institucion", "nombreUnidadCompra", "unidadCompra", "nombreInstitucion", "entidad"),
        "unidad_compra_code": pick(f, "unidadCompraCode", "codigoUnidadCompra"),
        "periodo": pick(f, "periodo", "anio", "ano"),
        "estado": pick(f, "estado", "estatus", "estadoPago"),
        "monto": pick(f, "monto", "montoFactura", "montoTotal", "valor"),
        "fecha_emision": fecha_emision,
        "fecha_pago": fecha_pago,
        "dias_pago": dias,
        "raw": f,
    }


def facturas_por_rnc(rnc, periodo="todos"):
    filas, page = [], 1
    while True:
        data = api_post("TrazabilidadPago/GetComprobantesFiscalesProveedores",
                        {"documentoIdentidad": rnc, "periodo": periodo,
                         "pageNumber": page, "pageSize": 50})
        time.sleep(SLEEP)
        items = data if isinstance(data, list) else (pick(data or {}, "items", "lista", "comprobantes", "data") or [])
        if not items:
            break
        filas += [normalizar_factura(f, rnc=rnc) for f in items]
        if len(items) < 50:
            break
        page += 1
        if page > 100:  # tope de seguridad
            break
    return filas


def facturas_por_contrato(contrato):
    data = api_get("FacturaFiscal/GetFacturasContrato", {"contrato": contrato})
    time.sleep(SLEEP)
    items = data if isinstance(data, list) else []
    return [normalizar_factura(f, contrato=contrato) for f in items]


def _contratos_de_rnc(rnc):
    """Contratos de un proveedor vía GetProcesosContratacion (paginado).
    El body exacto no está documentado públicamente: probamos variantes."""
    candidatos = [
        lambda p: {"documentoIdentidad": rnc, "pageNumber": p, "pageSize": 50},
        lambda p: {"numeroDocumento": rnc, "pageNumber": p, "pageSize": 50},
        lambda p: {"type": "document", "value": rnc, "pageNumber": p, "pageSize": 50},
    ]
    for body_fn in candidatos:
        data = api_post("TrazabilidadPago/GetProcesosContratacion", body_fn(1))
        time.sleep(SLEEP)
        bloque = data[0] if isinstance(data, list) and data else (data if isinstance(data, dict) else None)
        if not bloque or not isinstance(bloque, dict) or "contratosProveedor" not in bloque:
            continue
        prov = bloque.get("datosProveedor") or {}
        cp = bloque.get("contratosProveedor") or {}
        regs = list(cp.get("registros") or [])
        total_pag = int(cp.get("totalPaginas") or 1)
        vistos = {r.get("contrato") for r in regs}
        page = 2
        while page <= total_pag and page <= 60:
            d2 = api_post("TrazabilidadPago/GetProcesosContratacion", body_fn(page))
            time.sleep(SLEEP)
            b2 = d2[0] if isinstance(d2, list) and d2 else {}
            r2 = (b2.get("contratosProveedor") or {}).get("registros") or []
            nuevos = [r for r in r2 if r.get("contrato") not in vistos]
            if not nuevos:
                break  # el API ignoró la paginación o no hay más
            regs += nuevos
            vistos.update(r.get("contrato") for r in nuevos)
            page += 1
        return prov, regs
    return {}, []


def _dedup(filas, claves):
    vistos, out = set(), []
    for f in filas:
        k = tuple(f.get(c) for c in claves)
        if k not in vistos:
            vistos.add(k); out.append(f)
    return out


# ── RESOLVER RPE → RNC (vía InfoPago GetReferenciaProveedor) ───
# DESCUBRIMIENTO: empresas_estado.rnc contiene el RPE (2-6 dígitos),
# NO el RNC. InfoPago exige RNC/Cédula (9/11 dígitos) como
# documentoIdentidad. Este modo resuelve el documento real una sola
# vez por empresa y lo cachea en empresas_estado.rnc_real.

import re as _re_doc

def _solo_digitos(v):
    return _re_doc.sub(r"\D", "", str(v or ""))


def _norm_nombre(s):
    s = (s or "").lower()
    for a, b in (("á","a"),("é","e"),("í","i"),("ó","o"),("ú","u"),("ñ","n")):
        s = s.replace(a, b)
    return _re_doc.sub(r"[^a-z0-9]", "", s)


def _extraer_documento(data, nombre_esperado):
    """Busca un RNC/cédula (9 u 11 dígitos) en la respuesta de
    GetReferenciaProveedor, validando contra el nombre si hay varios."""
    if data is None:
        return None
    items = data if isinstance(data, list) else \
        (pick(data, "items", "lista", "registros", "data") or [data])
    if not isinstance(items, list):
        items = [items]
    objetivo = _norm_nombre(nombre_esperado)
    candidatos = []
    for it in items:
        if not isinstance(it, dict):
            continue
        doc = _solo_digitos(pick(it, "documentoIdentidad", "numeroDocumento",
                                 "rnc", "documento", "cedula", "rncCedula"))
        if len(doc) not in (9, 11):
            continue
        nom = _norm_nombre(pick(it, "razonSocial", "nombre", "nombreProveedor", "proveedor"))
        score = 2 if (nom and objetivo and (nom == objetivo or objetivo in nom or nom in objetivo)) else 1
        candidatos.append((score, doc))
    if not candidatos:
        return None
    candidatos.sort(reverse=True)
    # Si hay un solo resultado o hay match de nombre, lo aceptamos
    if len(items) == 1 or candidatos[0][0] == 2:
        return candidatos[0][1]
    return None  # varios resultados sin match de nombre → no adivinar


def resolver_rnc_empresa(rpe, nombre):
    """Intenta resolver el documento real probando varias vías."""
    # 1) Por RPE directo (por si el API lo soporta)
    for tipo in ("rpe", "document"):
        doc = _extraer_documento(
            api_post("Proveedor/GetReferenciaProveedor", {"type": tipo, "value": str(rpe)}),
            nombre)
        time.sleep(SLEEP)
        if doc:
            return doc
    # 2) Por nombre (vía confirmada en --explorar)
    doc = _extraer_documento(
        api_post("Proveedor/GetReferenciaProveedor", {"type": "name", "value": nombre}),
        nombre)
    time.sleep(SLEEP)
    return doc


def modo_resolver_rnc(args):
    print(f"▶ Resolviendo RNC real de hasta {args.limit} empresas (más activas primero)")
    empresas = sb_select(
        "empresas_estado",
        "select=id,rnc,nombre&rnc_real=is.null&nombre=not.is.null"
        f"&order=total_contratos.desc.nullslast&limit={args.limit}")
    if not empresas:
        print("✔ No hay empresas pendientes de resolver (o falló la consulta)")
        return
    ok = fail = 0
    for i, e in enumerate(empresas, 1):
        doc = resolver_rnc_empresa(e.get("rnc"), e.get("nombre"))
        if doc:
            sb_patch("empresas_estado", f"id=eq.{e['id']}", {"rnc_real": doc})
            ok += 1
        else:
            fail += 1
        if i % 20 == 0 or i == len(empresas):
            print(f"  [{i}/{len(empresas)}] resueltos={ok} sin_match={fail}")
    print(f"✅ {ok} RNCs resueltos, {fail} sin match")
    if ok == 0:
        print("✖ No se resolvió ningún RNC — revisar formato de GetReferenciaProveedor con --explorar")
        sys.exit(1)


def modo_facturas(args):
    if args.rnc:
        rncs = [{"rnc": args.rnc, "nombre": ""}]
    elif args.contrato:
        filas = [f for f in facturas_por_contrato(args.contrato) if f["comprobante_fiscal"]]
        n = sb_upsert("infopago_facturas", _dedup(filas, ["comprobante_fiscal", "contrato"]),
                      "comprobante_fiscal,contrato")
        print(f"✅ {n} facturas guardadas"); return
    elif args.desde_contratos:
        # Empresas con más contratos primero. OJO: la columna "rnc" es el RPE;
        # el documento real está en rnc_real (poblado por --modo resolver-rnc).
        print(f"▶ Tomando {args.limit} empresas con rnc_real (más activas primero)")
        rncs = sb_select("empresas_estado",
                         f"select=rnc_real,nombre&rnc_real=not.is.null&order=total_contratos.desc.nullslast&limit={args.limit}")
        rncs = [{"rnc": r.get("rnc_real"), "nombre": r.get("nombre")} for r in rncs]
        if not rncs:
            print("✖ Ninguna empresa tiene rnc_real. Corre primero: --modo resolver-rnc")
            sys.exit(1)
    else:
        print("✖ Indica --rnc, --contrato o --desde-contratos"); return

    tot_fact, tot_contr = 0, 0
    for i, e in enumerate(rncs, 1):
        rnc = (e.get("rnc") or "").strip()
        if len(rnc) not in (9, 11) or not rnc.isdigit():
            print(f"  [{i}] saltando documento inválido: {rnc!r} ({e.get('nombre')})")
            continue
        prov, contratos = _contratos_de_rnc(rnc)
        if not contratos:
            continue
        # Guardar vínculo proveedor ↔ contratos
        pc = [{
            "rnc": rnc,
            "rpe": pick(prov, "rpe"),
            "razon_social": pick(prov, "razonSocial") or e.get("nombre"),
            "estado_rpe": pick(prov, "estadoRPE"),
            "monto_total_contratado": pick(prov, "montoTotalContratado"),
            "contrato": c.get("contrato"),
            "proceso_compra": c.get("procesoCompra"),
            "notice_uid": c.get("noticeUID"),
            "periodo": str(c.get("periodo") or ""),
            "fecha_contrato": parse_fecha(c.get("fecha")),
            "raw": c,
        } for c in contratos if c.get("contrato")]
        sb_upsert("infopago_proveedor_contratos", _dedup(pc, ["rnc", "contrato"]), "rnc,contrato")
        tot_contr += len(pc)
        # Facturas de los contratos más recientes (vienen ordenados desc)
        filas = []
        for c in contratos[:args.max_contratos]:
            if c.get("contrato"):
                filas += facturas_por_contrato(c["contrato"])
        filas = [dict(f, rnc_proveedor=f["rnc_proveedor"] or rnc) for f in filas if f["comprobante_fiscal"]]
        n = sb_upsert("infopago_facturas", _dedup(filas, ["comprobante_fiscal", "contrato"]),
                      "comprobante_fiscal,contrato")
        tot_fact += n
        print(f"  [{i}/{len(rncs)}] {e.get('nombre') or rnc}: {len(pc)} contratos, {n} facturas")
    print(f"✅ TOTAL: {tot_contr} contratos y {tot_fact} facturas guardadas")
    if tot_fact == 0 and tot_contr == 0:
        print("✖ Carga vacía — marcando el job como fallido para que GitHub lo muestre en rojo")
        sys.exit(1)


# ── MODO RANKING (campos reales confirmados del API) ───────────
def modo_ranking(args):
    """Descarga el ranking completo de instituciones (203 aprox., paginado)."""
    filas, page = [], 1
    while True:
        data = api_get("RankingInstitucional/GetRankingFacturasPagadasByInstitucion",
                       {"periodo": args.periodo, "pageNumber": page, "pageSize": 50})
        time.sleep(SLEEP)
        if data is None:
            break
        regs = data.get("registros", []) if isinstance(data, dict) else (data if isinstance(data, list) else [])
        total_paginas = data.get("totalPaginas") if isinstance(data, dict) else None
        if not regs:
            break
        for r in regs:
            filas.append({
                "periodo": str(pick(r, "periodo") or args.periodo),
                "unidad_compra_code": pick(r, "unidadCompraCode") or "",
                "institucion": pick(r, "nombreUnidadCompra", "institucion"),
                "mediana_dias_pago": pick(r, "medianaDiasPago"),
                "promedio_dias_pago": pick(r, "promedioDiasPago"),
                "moda_dias_pago": pick(r, "modaDiasPago"),
                "cantidad_facturas": pick(r, "cantidadFacturas"),
                "porc_pagos_20_dias": pick(r, "porcPagos20Dias"),
                "porc_pagos_40_dias": pick(r, "porcPagos40Dias"),
                "porc_pagos_60_dias": pick(r, "porcPagos60Dias"),
                "porc_fuera_plazo": pick(r, "porcFueraPlazo"),
                "monto_total_pagado": pick(r, "montoTotalPagado"),
                "raw": r,
            })
        print(f"  página {page}: {len(regs)} instituciones (total acumulado: {len(filas)})")
        # Condiciones de parada: última página o el API ignora la paginación
        if total_paginas and page >= total_paginas:
            break
        if len(regs) < 10:
            break
        if page > 1 and filas[-1]["unidad_compra_code"] == filas[len(filas)-len(regs)-1]["unidad_compra_code"]:
            break  # misma data repetida → el API ignoró pageNumber
        page += 1
        if page > 60:
            break
    # Dedup por (periodo, code) por si hubo repetidos
    vistos, unicas = set(), []
    for f in filas:
        k = (f["periodo"], f["unidad_compra_code"])
        if k not in vistos:
            vistos.add(k); unicas.append(f)
    n = sb_upsert("infopago_ranking_instituciones", unicas, "periodo,unidad_compra_code")
    print(f"✅ {n} instituciones guardadas en infopago_ranking_instituciones")


# ── MODO TRAZABILIDAD (roadmap por factura) ────────────────────
# Etapas SIAFE confirmadas: preventivo → compromiso → facturaFiscal →
# devengado → ordenPago → ordenamiento → comprobante → conciliado
ROADMAP_PATH = os.environ.get("INFOPAGO_ROADMAP_PATH", "TrazabilidadPago/GetTrazabilidadPagoFacturas")

def _roadmap_via_pagina(comprobante, contrato, estado="Conciliado"):
    """Plan B garantizado: el roadmap viene embebido en el payload RSC de la
    página de InfoPago (Next.js). Lo pedimos con header rsc:1 y extraemos el
    objeto JSON que contiene numPreventivo."""
    import re as _re
    url = "https://infopago.dgcp.gob.do/trazabilidad-ciclos-pago/roadmap"
    h = dict(HEADERS)
    h["rsc"] = "1"
    h["Referer"] = "https://infopago.dgcp.gob.do/trazabilidad-ciclos-pago"
    try:
        r = requests.get(url, params={"comprobanteFiscal": comprobante,
                                      "contrato": contrato, "estado": estado},
                         headers=h, timeout=TIMEOUT)
        # El payload RSC trae el JSON con escapes; probar crudo y des-escapado
        for texto in (r.text, r.text.replace('\\"', '"')):
            m = _re.search(r'\{[^{}]*?"numPreventivo"[^{}]*?\}', texto)
            if m:
                try:
                    return [json.loads(m.group(0))]
                except json.JSONDecodeError:
                    continue
    except Exception as e:
        print(f"  ⚠ roadmap página: {e}")
    return []

def trazabilidad_factura(comprobante, contrato, estado="Conciliado"):
    # Vía 1: API directa (si el endpoint existe con este nombre)
    data = api_get(ROADMAP_PATH, {"comprobanteFiscal": comprobante, "contrato": contrato})
    time.sleep(SLEEP)
    items = data if isinstance(data, list) else ([data] if data else [])
    # Vía 2 (fallback garantizado): extraer del payload de la página
    if not items:
        items = _roadmap_via_pagina(comprobante, contrato, estado)
        time.sleep(SLEEP)
    filas = []
    for r in items:
        # Actualizar también la factura con fecha de pago real y días
        fact = normalizar_factura(r, contrato=contrato)
        if fact["comprobante_fiscal"]:
            sb_upsert("infopago_facturas", [fact], "comprobante_fiscal,contrato")
        filas.append({
            "comprobante_fiscal": pick(r, "comprobanteFiscal") or comprobante,
            "contrato": pick(r, "contrato") or contrato,
            "etapas": r,
        })
    return filas

def modo_trazabilidad(args):
    if args.comprobante and args.contrato:
        filas = trazabilidad_factura(args.comprobante, args.contrato)
    else:
        # Recorrer facturas ya cargadas que no tengan trazabilidad
        print("▶ Buscando facturas sin trazabilidad en Supabase...")
        facts = sb_select("infopago_facturas",
                          f"select=comprobante_fiscal,contrato,estado&fecha_pago=is.null&limit={args.limit}")
        filas = []
        for i, f in enumerate(facts, 1):
            filas += trazabilidad_factura(f["comprobante_fiscal"], f["contrato"], f.get("estado") or "Conciliado")
            if i % 25 == 0:
                print(f"  [{i}/{len(facts)}]")
    n = sb_upsert("infopago_trazabilidad", filas, "comprobante_fiscal,contrato")
    print(f"✅ {n} trazabilidades guardadas")


# ── MODO PROVEEDOR (búsqueda/prueba) ────────────────────────────
def modo_proveedor(args):
    data = api_post("Proveedor/GetReferenciaProveedor", {"type": "name", "value": args.nombre})
    print(json.dumps(data, indent=2, ensure_ascii=False, default=str)[:3000])


# ── MAIN ────────────────────────────────────────────────────────
if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="ETL InfoPago DGCP → Supabase")
    ap.add_argument("--explorar", action="store_true", help="Imprimir estructura real de cada endpoint")
    ap.add_argument("--modo", choices=["facturas", "ranking", "proveedor", "trazabilidad", "resolver-rnc"])
    ap.add_argument("--rnc", help="RNC del proveedor")
    ap.add_argument("--contrato", help="Código de contrato (ej. CAASD-2025-00188)")
    ap.add_argument("--desde-contratos", action="store_true",
                    help="Recorrer contratos_adjudicados de Supabase")
    ap.add_argument("--limit", type=int, default=200)
    ap.add_argument("--max-contratos", type=int, default=30, help="Máx. contratos por empresa a consultar facturas")
    ap.add_argument("--periodo", default="todos")
    ap.add_argument("--codes", help="unidadCompraCode separados por coma (ranking)")
    ap.add_argument("--nombre", default="CONSER", help="Nombre a buscar (modo proveedor)")
    ap.add_argument("--comprobante", help="Comprobante fiscal (modo trazabilidad)")
    args = ap.parse_args()

    if args.explorar:
        modo_explorar(args.rnc or "130723354", args.contrato or "CAASD-2025-00188")
    elif args.modo == "resolver-rnc":
        modo_resolver_rnc(args)
    elif args.modo == "facturas":
        modo_facturas(args)
    elif args.modo == "ranking":
        args.periodo = args.periodo if args.periodo != "todos" else "2026"
        modo_ranking(args)
    elif args.modo == "proveedor":
        modo_proveedor(args)
    elif args.modo == "trazabilidad":
        modo_trazabilidad(args)
    else:
        ap.print_help()
