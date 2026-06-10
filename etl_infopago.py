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
    fecha_emision = parse_fecha(pick(f, "fechaEmision", "fecha_emision", "fechaFactura", "fechaComprobante"))
    fecha_pago = parse_fecha(pick(f, "fechaPago", "fecha_pago", "fechaConciliacion", "fechaDeposito"))
    dias = None
    if fecha_emision and fecha_pago:
        dias = (datetime.fromisoformat(fecha_pago) - datetime.fromisoformat(fecha_emision)).days
    return {
        "comprobante_fiscal": pick(f, "comprobanteFiscal", "ncf", "comprobante", "numeroComprobante"),
        "contrato": pick(f, "contrato", "numeroContrato", "codigoContrato") or contrato,
        "rnc_proveedor": pick(f, "rnc", "documentoIdentidad", "numeroDocumento", "rncProveedor") or rnc,
        "nombre_proveedor": pick(f, "proveedor", "nombreProveedor", "razonSocial"),
        "institucion": pick(f, "institucion", "unidadCompra", "nombreInstitucion", "entidad"),
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


def modo_facturas(args):
    filas = []
    if args.rnc:
        print(f"▶ Facturas del RNC {args.rnc} (periodo={args.periodo})")
        filas = facturas_por_rnc(args.rnc, args.periodo)
    elif args.contrato:
        print(f"▶ Facturas del contrato {args.contrato}")
        filas = facturas_por_contrato(args.contrato)
    elif args.desde_contratos:
        print(f"▶ Recorriendo contratos_adjudicados de Supabase (limit {args.limit})")
        contratos = sb_select(
            "contratos_adjudicados",
            f"select=codigo_contrato&order=fecha_contrato.desc&limit={args.limit}"
        )
        codigos = [c.get("codigo_contrato") for c in contratos if c.get("codigo_contrato")]
        print(f"  {len(codigos)} contratos a consultar")
        for i, cod in enumerate(codigos, 1):
            fs = facturas_por_contrato(cod)
            if fs:
                filas += fs
                print(f"  [{i}/{len(codigos)}] {cod}: {len(fs)} facturas")
            if i % 50 == 0:
                n = sb_upsert("infopago_facturas",
                              [f for f in filas if f["comprobante_fiscal"]],
                              "comprobante_fiscal,contrato")
                print(f"  💾 lote guardado: {n}")
                filas = []
    else:
        print("✖ Indica --rnc, --contrato o --desde-contratos"); return

    filas = [f for f in filas if f["comprobante_fiscal"]]
    n = sb_upsert("infopago_facturas", filas, "comprobante_fiscal,contrato")
    print(f"✅ {n} facturas guardadas en infopago_facturas")


# ── MODO RANKING ────────────────────────────────────────────────
def modo_ranking(args):
    codes = [c.strip() for c in (args.codes or "").split(",") if c.strip()] or [""]
    filas = []
    for code in codes:
        params = {"periodo": args.periodo}
        if code:
            params["unidadCompraCode"] = code
        print(f"▶ Ranking periodo={args.periodo} unidadCompraCode={code or '(global)'}")
        data = api_get("RankingInstitucional/GetRankingFacturasPagadasByInstitucion", params)
        time.sleep(SLEEP)
        if data is None:
            continue
        filas.append({
            "periodo": str(args.periodo),
            "unidad_compra_code": code or "GLOBAL",
            "institucion": pick(data if isinstance(data, dict) else {}, "institucion", "nombreInstitucion"),
            "raw": data,
        })
    n = sb_upsert("infopago_ranking_instituciones", filas, "periodo,unidad_compra_code")
    print(f"✅ {n} registros de ranking guardados")


# ── MODO PROVEEDOR (búsqueda/prueba) ────────────────────────────
def modo_proveedor(args):
    data = api_post("Proveedor/GetReferenciaProveedor", {"type": "name", "value": args.nombre})
    print(json.dumps(data, indent=2, ensure_ascii=False, default=str)[:3000])


# ── MAIN ────────────────────────────────────────────────────────
if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="ETL InfoPago DGCP → Supabase")
    ap.add_argument("--explorar", action="store_true", help="Imprimir estructura real de cada endpoint")
    ap.add_argument("--modo", choices=["facturas", "ranking", "proveedor"])
    ap.add_argument("--rnc", help="RNC del proveedor")
    ap.add_argument("--contrato", help="Código de contrato (ej. CAASD-2025-00188)")
    ap.add_argument("--desde-contratos", action="store_true",
                    help="Recorrer contratos_adjudicados de Supabase")
    ap.add_argument("--limit", type=int, default=200)
    ap.add_argument("--periodo", default="todos")
    ap.add_argument("--codes", help="unidadCompraCode separados por coma (ranking)")
    ap.add_argument("--nombre", default="CONSER", help="Nombre a buscar (modo proveedor)")
    args = ap.parse_args()

    if args.explorar:
        modo_explorar(args.rnc or "130723354", args.contrato or "CAASD-2025-00188")
    elif args.modo == "facturas":
        modo_facturas(args)
    elif args.modo == "ranking":
        args.periodo = args.periodo if args.periodo != "todos" else "2026"
        modo_ranking(args)
    elif args.modo == "proveedor":
        modo_proveedor(args)
    else:
        ap.print_help()
