"""
ETL: Contratos Adjudicados — LicitacionLab
============================================
Extrae adjudicaciones desde la API DGCP y las carga en Supabase.
Sigue el mismo patrón de monitor.py (payload.content, paginación, cron_log).

Uso:
    python etl_contratos_adjudicados.py             # carga últimos 30 días
    python etl_contratos_adjudicados.py --full      # carga histórico completo (2021→hoy)
    python etl_contratos_adjudicados.py --year 2023 # carga un año específico

Variables de entorno (mismas que monitor.py):
    SUPABASE_URL
    SUPABASE_SERVICE_KEY  (o SUPABASE_KEY)
"""

import os
import sys
import time
import argparse
import requests
from datetime import datetime, timedelta
from dotenv import load_dotenv
from supabase import create_client

load_dotenv()

# ── Config — mismo patrón que monitor.py ─────────────────────
SUPABASE_URL  = os.getenv("SUPABASE_URL")
SUPABASE_KEY  = os.getenv("SUPABASE_SERVICE_KEY", os.getenv("SUPABASE_KEY"))
API_BASE_URL  = "https://datosabiertos.dgcp.gob.do/api-dgcp/v1"

ENDPOINT_CONTRATOS = "/contratos"   # ✅ confirmado

PAGE_SIZE     = 1000
DELAY_BETWEEN_PAGES = 0.3  # seg — igual que en monitor.py

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)


# ─────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────

def registrar_cron_log(job, status="ok", detalle=None, duracion_ms=None):
    """Mismo helper que monitor.py."""
    try:
        supabase.table("cron_log").insert({
            "job": job,
            "status": status,
            "detalle": detalle or {},
            "duracion_ms": duracion_ms,
        }).execute()
    except Exception as e:
        print(f"⚠️  cron_log error: {e}")


def parse_fecha(valor):
    """Convierte string ISO a date string YYYY-MM-DD o None."""
    if not valor:
        return None
    try:
        return str(valor)[:10]
    except Exception:
        return None


# ─────────────────────────────────────────────────────────────
# LLAMADA A LA API — mismo patrón que obtener_procesos_api()
# ─────────────────────────────────────────────────────────────

def obtener_contratos_api(fecha_desde, fecha_hasta, page=1):
    """
    Llama al endpoint de contratos/adjudicaciones de la DGCP.
    Retorna lista de contratos o [] si hay error.

    El payload sigue el mismo patrón que /procesos:
      { "hasError": false, "payload": { "content": [...] } }
    """
    params = {
        "startdate": fecha_desde,
        "enddate":   fecha_hasta,
        "page":      page,
        "limit":     PAGE_SIZE,
    }

    try:
        url = f"{API_BASE_URL}{ENDPOINT_CONTRATOS}"
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()

        if data.get("hasError"):
            print(f"⚠️  API hasError=True en página {page}")
            return []

        payload = data.get("payload")
        if payload is None:
            return []

        if isinstance(payload, list):
            contratos = payload
        elif isinstance(payload, dict):
            contratos = payload.get("content", []) or []
        else:
            contratos = []

        print(f"📥 API contratos: {len(contratos)} registros (pág {page})")
        return contratos

    except requests.exceptions.RequestException as e:
        print(f"❌ Error API contratos: {e}")
        return []


def obtener_todas_las_paginas_contratos(fecha_desde, fecha_hasta):
    """Pagina hasta agotar resultados — igual que obtener_todas_las_paginas()."""
    todos = []
    page  = 1
    while True:
        contratos = obtener_contratos_api(fecha_desde, fecha_hasta, page=page)
        if not contratos:
            break
        todos.extend(contratos)
        if len(contratos) < PAGE_SIZE:
            break
        page += 1
        time.sleep(DELAY_BETWEEN_PAGES)
    return todos


# ─────────────────────────────────────────────────────────────
# UPSERT HELPERS
# ─────────────────────────────────────────────────────────────

_cache_inst  = {}   # codigo_unidad → uuid
_cache_emp   = {}   # rnc → uuid  (si no hay RNC, usar nombre normalizado)

def upsert_institucion(codigo, nombre):
    """Inserta o recupera institución. Cachea para no repetir llamadas."""
    key = str(codigo)
    if key in _cache_inst:
        return _cache_inst[key]

    try:
        r = supabase.table("instituciones_compradoras") \
            .upsert({"codigo": key, "nombre": nombre or key}, on_conflict="codigo") \
            .execute()
        uid = r.data[0]["id"]
        _cache_inst[key] = uid
        return uid
    except Exception as e:
        print(f"⚠️  upsert_institucion error: {e}")
        return None


def obtener_datos_rpe(rnc):
    """
    Consulta el endpoint /proveedores de la DGCP con el RNC
    y devuelve datos de contacto SOLO si el RNC del resultado coincide.

    BUG CONOCIDO DE LA API DGCP: cuando el RNC no existe en el RPE,
    la API retorna el primer resultado del índice en lugar de devolver
    una lista vacía. Por eso validamos que el RNC del resultado sea
    exactamente el que consultamos — si no coincide, descartamos.
    """
    if not rnc:
        return {}
    rnc_str = str(rnc).strip()
    try:
        r = requests.get(
            f"{API_BASE_URL}/proveedores",
            params={"rnc": rnc_str, "limit": 1},
            timeout=15,
        )
        r.raise_for_status()
        data = r.json()
        if data.get("hasError"):
            return {}
        payload = data.get("payload", {})
        content = payload.get("content", payload) if isinstance(payload, dict) else payload
        if not content or not isinstance(content, list):
            return {}
        p = content[0]

        # ✅ VALIDACIÓN CRÍTICA: verificar que el RNC del resultado coincida
        # La API devuelve el primer registro del índice cuando no encuentra el RNC
        rnc_resultado = str(p.get("rnc") or p.get("rpe") or "").strip()
        if rnc_resultado and rnc_resultado != rnc_str:
            # La API devolvió un registro diferente — no es el proveedor correcto
            return {}

        # Validar que al menos tenga un dato de contacto real
        email = p.get("correo_comercial") or p.get("correo_contacto")
        tel   = p.get("telefono_comercial") or p.get("telefono_contacto")
        if not email and not tel:
            return {}

        return {
            "correo_comercial":   p.get("correo_comercial"),
            "correo_contacto":    p.get("correo_contacto"),
            "telefono_comercial": p.get("telefono_comercial"),
            "telefono_contacto":  p.get("telefono_contacto"),
            "nombre_contacto":    p.get("contacto"),
            "posicion_contacto":  p.get("posicion_contacto"),
            "provincia":          p.get("provincia"),
            "direccion":          p.get("direccion"),
            "estado_rpe":         p.get("estado"),
            "enriquecido_rpe":    True,
        }
    except Exception as e:
        print(f"⚠️  obtener_datos_rpe({rnc}): {e}")
        return {}


def upsert_empresa(rnc, nombre):
    """
    Inserta o recupera empresa adjudicada.
    Si tiene RNC → upsert por RNC + enriquece con datos del RPE.
    Si no tiene RNC → buscar por nombre, insertar si no existe.
    """
    nombre_clean = (nombre or "").strip()
    if not nombre_clean:
        return None

    cache_key = rnc if rnc else f"_nombre_{nombre_clean.lower()}"
    if cache_key in _cache_emp:
        return _cache_emp[cache_key]

    try:
        if rnc:
            # Base de la empresa
            datos = {"rnc": rnc, "nombre": nombre_clean}

            # Enriquecer con RPE (email, teléfono, contacto)
            rpe_data = obtener_datos_rpe(rnc)
            if rpe_data:
                datos.update(rpe_data)
            else:
                datos["enriquecido_rpe"] = False

            r = supabase.table("empresas_estado") \
                .upsert(datos, on_conflict="rnc") \
                .execute()
            uid = r.data[0]["id"]
        else:
            existente = supabase.table("empresas_estado") \
                .select("id") \
                .eq("nombre", nombre_clean) \
                .maybe_single() \
                .execute()
            if existente.data:
                uid = existente.data["id"]
            else:
                r = supabase.table("empresas_estado") \
                    .insert({"nombre": nombre_clean, "enriquecido_rpe": False}) \
                    .execute()
                uid = r.data[0]["id"]

        _cache_emp[cache_key] = uid
        return uid

    except Exception as e:
        print(f"⚠️  upsert_empresa error ({nombre_clean}): {e}")
        return None


# ─────────────────────────────────────────────────────────────
# MAPEO DEL PAYLOAD
# ─────────────────────────────────────────────────────────────

def mapear_contrato(c):
    """
    Convierte un registro del API DGCP al esquema de contratos_adjudicados.

    Campos confirmados del endpoint /contratos (verificado 2026-04-30):
      codigo_contrato, codigo_proceso, descripcion, estado_contrato,
      fecha_adjudicacion, valor_contratado, divisa, metodo_pago,
      unidad_compra, codigo_unidad_compra,
      rpe (= RNC del proveedor), razon_social (= nombre del proveedor),
      fecha_creacion_contrato, url_contrato
    """
    return {
        # ✅ ID único del contrato
        "ocid": c.get("codigo_contrato", ""),

        # ✅ Proceso relacionado
        "codigo_proceso":  c.get("codigo_proceso"),

        # ✅ Título / descripción
        "titulo_proceso":  (c.get("descripcion") or "").strip(),

        # La API /contratos no devuelve modalidad directamente
        "modalidad":       c.get("metodo_pago"),
        "objeto_proceso":  c.get("estado_contrato"),

        # ✅ Monto real adjudicado
        "monto_adjudicado": c.get("valor_contratado"),
        "divisa":           c.get("divisa", "DOP"),

        # ✅ Fechas
        "fecha_adjudicacion": parse_fecha(c.get("fecha_adjudicacion")),
        "fecha_contrato":     parse_fecha(c.get("fecha_creacion_contrato")),

        # ✅ Proveedor adjudicado
        "_nombre_proveedor": (c.get("razon_social") or "").strip(),
        "_rnc_proveedor":     c.get("rpe"),   # rpe = RNC en la API DGCP

        # ✅ Institución compradora
        "_codigo_unidad": str(c.get("codigo_unidad_compra", "SIN_CODIGO")),
        "_nombre_unidad":  c.get("unidad_compra", "Sin nombre"),
    }


# ─────────────────────────────────────────────────────────────
# CARGA A SUPABASE
# ─────────────────────────────────────────────────────────────

def cargar_contratos(contratos_raw):
    """
    Procesa y carga un batch de contratos en Supabase.
    Retorna cantidad de registros procesados.
    """
    if not contratos_raw:
        return 0

    filas = []
    empresas_modificadas = set()

    for c in contratos_raw:
        mapped = mapear_contrato(c)

        # Omitir si no hay OCID válido
        if not mapped["ocid"]:
            continue

        # Omitir si no hay proveedor
        if not mapped["_nombre_proveedor"]:
            continue

        # Resolver empresa
        emp_id = upsert_empresa(mapped["_rnc_proveedor"], mapped["_nombre_proveedor"])
        if not emp_id:
            continue

        # Resolver institución
        inst_id = upsert_institucion(mapped["_codigo_unidad"], mapped["_nombre_unidad"])

        empresas_modificadas.add(emp_id)

        filas.append({
            "ocid":               mapped["ocid"],
            "codigo_proceso":     mapped["codigo_proceso"],
            "empresa_id":         emp_id,
            "institucion_id":     inst_id,
            "titulo_proceso":     mapped["titulo_proceso"],
            "modalidad":          mapped["modalidad"],
            "objeto_proceso":     mapped["objeto_proceso"],
            "monto_adjudicado":   mapped["monto_adjudicado"],
            "divisa":             mapped["divisa"],
            "fecha_adjudicacion": mapped["fecha_adjudicacion"],
            "fecha_contrato":     mapped["fecha_contrato"],
        })

    if not filas:
        return 0

    # Upsert en lotes de 250 (igual que monitor.py con lotes de 100)
    insertados = 0
    for i in range(0, len(filas), 250):
        try:
            r = supabase.table("contratos_adjudicados") \
                .upsert(filas[i:i+250], on_conflict="ocid") \
                .execute()
            insertados += len(r.data)
        except Exception as e:
            print(f"❌ Error upsert contratos (lote {i}): {e}")

    # Actualizar caches de totales en empresas_estado
    for emp_id in empresas_modificadas:
        try:
            supabase.rpc("actualizar_cache_empresa", {"p_empresa_id": emp_id}).execute()
        except Exception as e:
            print(f"⚠️  actualizar_cache_empresa error: {e}")

    return insertados


# ─────────────────────────────────────────────────────────────
# DIAGNÓSTICO — VERIFICAR ENDPOINT Y CAMPOS
# ─────────────────────────────────────────────────────────────

def diagnosticar_api():
    """
    Llama al endpoint con 1 registro y muestra los campos disponibles.
    Ejecutar PRIMERO antes del ETL completo para confirmar nombres de campos.

    Uso: python etl_contratos_adjudicados.py --diagnostico
    """
    print("\n🔬 DIAGNÓSTICO DE LA API")
    print("="*50)

    candidatos = [
        "/contratos",
        "/adjudicaciones",
        "/procesos/adjudicacion",
    ]

    fecha_reciente = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
    fecha_hoy      = datetime.now().strftime("%Y-%m-%d")

    for endpoint in candidatos:
        url = f"{API_BASE_URL}{endpoint}"
        params = {"startdate": fecha_reciente, "enddate": fecha_hoy, "page": 1, "limit": 2}
        try:
            r = requests.get(url, params=params, timeout=15)
            if r.status_code == 200:
                data = r.json()
                if not data.get("hasError"):
                    payload = data.get("payload", {})
                    content = payload.get("content", payload) if isinstance(payload, dict) else payload
                    if content and isinstance(content, list) and len(content) > 0:
                        print(f"\n✅ ENDPOINT VÁLIDO: {endpoint}")
                        print(f"   Campos disponibles en el primer registro:")
                        for k, v in content[0].items():
                            print(f"   · {k}: {repr(v)[:80]}")
                        return endpoint
                    else:
                        print(f"⚠️  {endpoint} → respuesta vacía (sin error)")
                else:
                    print(f"❌ {endpoint} → hasError=True")
            else:
                print(f"❌ {endpoint} → HTTP {r.status_code}")
        except Exception as e:
            print(f"❌ {endpoint} → {e}")

    print("\n⛔ Ningún endpoint respondió. Verifica la URL base o contacta a la DGCP.")
    return None


# ─────────────────────────────────────────────────────────────
# ETL PRINCIPAL
# ─────────────────────────────────────────────────────────────

def run_etl(fecha_desde, fecha_hasta, modo="incremental"):
    """
    Extrae y carga contratos para un rango de fechas.
    """
    t0 = time.time()
    print(f"\n{'='*50}")
    print(f"🏗️  ETL Contratos | {fecha_desde} → {fecha_hasta} | modo={modo}")
    print(f"{'='*50}")

    total_extraidos  = 0
    total_insertados = 0

    # Para histórico: itera mes a mes para no saturar la API
    if modo == "full":
        fecha_ini = datetime.strptime(fecha_desde, "%Y-%m-%d")
        fecha_fin = datetime.strptime(fecha_hasta, "%Y-%m-%d")
        cursor    = fecha_ini

        while cursor < fecha_fin:
            mes_hasta = min(cursor + timedelta(days=30), fecha_fin)
            desde_str = cursor.strftime("%Y-%m-%d")
            hasta_str = mes_hasta.strftime("%Y-%m-%d")

            print(f"\n📅 Procesando {desde_str} → {hasta_str}...")
            contratos = obtener_todas_las_paginas_contratos(desde_str, hasta_str)
            total_extraidos += len(contratos)

            if contratos:
                insertados = cargar_contratos(contratos)
                total_insertados += insertados
                print(f"   ✓ {insertados}/{len(contratos)} cargados")

            cursor = mes_hasta + timedelta(days=1)
            time.sleep(1)  # pausa educada entre meses
    else:
        # Incremental: rango directo
        contratos = obtener_todas_las_paginas_contratos(fecha_desde, fecha_hasta)
        total_extraidos = len(contratos)
        if contratos:
            total_insertados = cargar_contratos(contratos)

    duracion_ms = int((time.time() - t0) * 1000)

    registrar_cron_log(
        job=f"etl_contratos_{modo}",
        status="ok",
        detalle={
            "fecha_desde":       fecha_desde,
            "fecha_hasta":       fecha_hasta,
            "total_extraidos":   total_extraidos,
            "total_insertados":  total_insertados,
        },
        duracion_ms=duracion_ms,
    )

    print(f"\n✅ ETL completado en {duracion_ms/1000:.1f}s")
    print(f"   Extraídos:  {total_extraidos}")
    print(f"   Insertados: {total_insertados}")
    return total_insertados


# ─────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="ETL Contratos Adjudicados — LicitacionLab")
    parser.add_argument("--full",        action="store_true", help="Carga histórico completo desde 2021")
    parser.add_argument("--year",        type=int,            help="Carga un año específico (ej: --year 2023)")
    parser.add_argument("--diagnostico", action="store_true", help="Detecta el endpoint correcto y muestra campos")
    parser.add_argument("--desde",       type=str,            help="Fecha inicio YYYY-MM-DD")
    parser.add_argument("--hasta",       type=str,            help="Fecha fin YYYY-MM-DD")
    args = parser.parse_args()

    if args.diagnostico:
        # PASO 1: siempre ejecutar esto primero
        endpoint_detectado = diagnosticar_api()
        if endpoint_detectado:
            print(f"\n👉 Actualiza ENDPOINT_CONTRATOS = \"{endpoint_detectado}\" en este archivo y corre el ETL.")
        sys.exit(0)

    if args.full:
        run_etl("2021-01-01", datetime.now().strftime("%Y-%m-%d"), modo="full")

    elif args.year:
        run_etl(f"{args.year}-01-01", f"{args.year}-12-31", modo="full")

    elif args.desde and args.hasta:
        run_etl(args.desde, args.hasta, modo="custom")

    else:
        # Default: últimos 30 días (modo incremental, igual que monitor.py)
        desde = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
        hasta = datetime.now().strftime("%Y-%m-%d")
        run_etl(desde, hasta, modo="incremental")
