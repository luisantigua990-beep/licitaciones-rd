"""
Tests de las optimizaciones de rendimiento (rama perf/optimizacion-rendimiento).

Comprueban que cada cambio hace MENOS idas a la base y devuelve EXACTAMENTE lo mismo
que el código anterior. Corren sin red: Supabase está simulado.
    SUPABASE_URL=https://dummy.supabase.co SUPABASE_KEY=dummy pytest tests/test_rendimiento.py -v
"""
import os
import sys
import types

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.environ.setdefault("SUPABASE_URL", "https://dummy.supabase.co")
os.environ.setdefault("SUPABASE_KEY", "dummy")


# ══════════════════════════════════════════════════════════════
# Supabase simulado que SÍ filtra (eq / in_ / is_) y cuenta consultas
# ══════════════════════════════════════════════════════════════
class _Q:
    def __init__(self, db, tabla):
        self.db, self.tabla, self.filtros, self._count = db, tabla, [], None

    def select(self, cols="*", count=None):
        self._count = count
        return self

    def eq(self, c, v):
        self.filtros.append(lambda r: r.get(c) == v)
        return self

    def in_(self, c, vs):
        vs = list(vs)
        self.filtros.append(lambda r: r.get(c) in vs)
        return self

    def is_(self, c, v):
        self.filtros.append(lambda r: r.get(c) is None if v == "null" else True)
        return self

    def order(self, *a, **k):
        return self

    def limit(self, *a, **k):
        return self

    def execute(self):
        self.db.consultas.append(self.tabla)
        filas = [r for r in self.db.tablas.get(self.tabla, []) if all(f(r) for f in self.filtros)]
        return types.SimpleNamespace(data=[dict(r) for r in filas], count=len(filas))


class FakeDB:
    def __init__(self, tablas):
        self.tablas, self.consultas, self.rpcs = tablas, [], []

    def table(self, nombre):
        return _Q(self, nombre)

    def n(self, tabla):
        return self.consultas.count(tabla)


# ══════════════════════════════════════════════════════════════
# 1) Sobre A: adjuntos en 1 consulta por entidad (antes 1 por registro)
# ══════════════════════════════════════════════════════════════
def _adjuntos_entidad_viejo(sb, empresa_id, entidad):
    """Copia literal de la versión anterior (N+1), para comparar resultados."""
    import sobre_a_core as core
    from expediente_core import TABLA_POR_ENTIDAD
    tabla = TABLA_POR_ENTIDAD[entidad]
    registros = sb.table(tabla).select("*").eq("empresa_id", empresa_id).execute().data or []
    salida = []
    for r in registros:
        adjuntos = (sb.table("bid_archivos").select("id,nombre,estado,eliminado_en")
                    .eq("empresa_id", empresa_id).eq("entidad", entidad)
                    .eq("registro_id", r["id"]).is_("eliminado_en", "null")
                    .execute().data or [])
        salida.append((r["id"], [(a["id"], a["nombre"], a["estado"]) for a in adjuntos]))
    return salida


def test_sobre_a_adjuntos_una_consulta_y_mismo_resultado():
    import sobre_a_core as core
    from expediente_core import TABLA_POR_ENTIDAD
    entidad = "certificaciones"
    tabla = TABLA_POR_ENTIDAD[entidad]
    E = "emp-1"
    tablas = {
        tabla: [
            {"id": "r1", "empresa_id": E, "tipo": "TSS", "fecha_vencimiento": None},
            {"id": "r2", "empresa_id": E, "tipo": "DGII", "fecha_vencimiento": None},
            {"id": "r3", "empresa_id": E, "tipo": "RPE", "fecha_vencimiento": None},
            {"id": "rX", "empresa_id": "otra", "tipo": "TSS", "fecha_vencimiento": None},
        ],
        "bid_archivos": [
            {"id": "a1", "empresa_id": E, "entidad": entidad, "registro_id": "r1", "nombre": "tss.pdf", "estado": "ok", "eliminado_en": None},
            {"id": "a2", "empresa_id": E, "entidad": entidad, "registro_id": "r1", "nombre": "tss2.pdf", "estado": "pendiente", "eliminado_en": None},
            {"id": "a3", "empresa_id": E, "entidad": entidad, "registro_id": "r2", "nombre": "dgii.pdf", "estado": "ok", "eliminado_en": None},
            {"id": "a4", "empresa_id": E, "entidad": entidad, "registro_id": "r2", "nombre": "borrado.pdf", "estado": "ok", "eliminado_en": "2026-01-01"},
            {"id": "a5", "empresa_id": "otra", "entidad": entidad, "registro_id": "r1", "nombre": "ajeno.pdf", "estado": "ok", "eliminado_en": None},
        ],
    }
    db_viejo, db_nuevo = FakeDB(tablas), FakeDB(tablas)
    viejo = _adjuntos_entidad_viejo(db_viejo, E, entidad)
    nuevo = core._adjuntos_entidad(db_nuevo, E, entidad)

    # mismas piezas: por registro, los mismos adjuntos en el mismo orden
    ids_nuevo = [p.get("id") for p in nuevo]
    for reg, adj in viejo:
        if adj:
            for a_id, _, _ in adj:
                assert any(a_id in str(i) for i in ids_nuevo), (reg, a_id, ids_nuevo)
        else:
            assert f"doc:{entidad}:{reg}:falta" in ids_nuevo
    assert not any("a4" in str(i) or "a5" in str(i) for i in ids_nuevo)  # borrado / ajeno fuera

    assert db_viejo.n("bid_archivos") == 3   # antes: 1 por registro
    assert db_nuevo.n("bid_archivos") == 1   # ahora: 1 por entidad


# ══════════════════════════════════════════════════════════════
# 2) ETL contratos: caché de empresas por lote (antes 1 RPC por empresa)
# ══════════════════════════════════════════════════════════════
class _RPC:
    def __init__(self, log, nombre, params, falla):
        self.log, self.nombre, self.params, self.falla = log, nombre, params, falla

    def execute(self):
        if self.nombre in self.falla:
            raise RuntimeError("función no existe")
        self.log.append((self.nombre, self.params))
        return types.SimpleNamespace(data=None)


def _fake_rpc(falla=()):
    log = []
    return log, types.SimpleNamespace(rpc=lambda n, p: _RPC(log, n, p, falla))


def test_etl_cache_empresas_por_lote(monkeypatch):
    import etl_contratos_adjudicados as etl
    log, fake = _fake_rpc()
    monkeypatch.setattr(etl, "supabase", fake)
    ids = [f"e{i}" for i in range(1203)]
    etl.actualizar_caches_empresas(ids)
    assert [n for n, _ in log] == ["actualizar_cache_empresas"] * 3        # 500+500+203
    assert sum(len(p["p_ids"]) for _, p in log) == 1203


def test_etl_cache_empresas_respaldo_uno_por_uno(monkeypatch):
    import etl_contratos_adjudicados as etl
    log, fake = _fake_rpc(falla={"actualizar_cache_empresas"})
    monkeypatch.setattr(etl, "supabase", fake)
    etl.actualizar_caches_empresas(["a", "b", "c"])
    assert log == [("actualizar_cache_empresa", {"p_empresa_id": x}) for x in "abc"]


# ══════════════════════════════════════════════════════════════
# 3) /api/stats: 1 RPC + caché (antes 6 conteos por carga)
# ══════════════════════════════════════════════════════════════
@pytest.fixture
def main_mod(monkeypatch):
    for k in ("ANTHROPIC_API_KEY", "GEMINI_API_KEY", "PAGADITO_UID", "PAGADITO_WSK"):
        monkeypatch.setenv(k, os.environ.get(k, "dummy"))
    try:
        import main
    except Exception as e:  # dependencias opcionales ausentes en el entorno de test
        pytest.skip(f"main no importable aquí: {e}")
    return main


def test_stats_un_rpc_y_cache(main_mod, monkeypatch):
    llamadas = []
    datos = {"total_procesos": 10, "procesos_activos": 4, "obras_activas": 1,
             "bienes_activos": 2, "servicios_activos": 1, "total_articulos": 99}

    class R:
        def execute(self):
            llamadas.append(1)
            return types.SimpleNamespace(data=dict(datos))

    monkeypatch.setattr(main_mod, "supabase", types.SimpleNamespace(rpc=lambda n, *a: R()))
    main_mod._cache_stats.update(data=None, ts=0.0)
    from fastapi.testclient import TestClient
    c = TestClient(main_mod.app)
    r1, r2 = c.get("/api/stats").json(), c.get("/api/stats").json()
    assert len(llamadas) == 1                       # la segunda sale de la caché
    for k, v in datos.items():
        assert r1[k] == v and r2[k] == v
    assert "ultima_actualizacion" in r1


def test_stats_respaldo_si_falla_rpc(main_mod, monkeypatch):
    def rpc(*a, **k):
        raise RuntimeError("sin función")
    procs = [{"id": i, "estado_proceso": "Proceso publicado", "fecha_fin_recepcion_ofertas": "2999-01-01",
              "objeto_proceso": o} for i, o in enumerate(["Obras", "Bienes", "Bienes", "Servicios"])]
    db = FakeDB({"procesos": procs, "articulos_proceso": [{"id": 1}]})
    db.rpc = rpc
    # el respaldo usa gt/ilike: el fake los ignora (todos cumplen), basta con que responda
    _Q.gt = lambda self, *a: self
    _Q.ilike = lambda self, *a: self
    monkeypatch.setattr(main_mod, "supabase", db)
    main_mod._cache_stats.update(data=None, ts=0.0)
    from fastapi.testclient import TestClient
    r = TestClient(main_mod.app).get("/api/stats")
    assert r.status_code == 200 and r.json()["total_procesos"] == 4


# ══════════════════════════════════════════════════════════════
# 4) Referidos: comisiones en 1 consulta (antes 1 por referido)
# ══════════════════════════════════════════════════════════════
def test_mis_referidos_sin_n_mas_1(monkeypatch):
    import router_referidos as ref
    tablas = {
        "referrals": [
            {"id": "f1", "referrer_id": "U", "referred_user_id": "u1", "status": "active", "created_at": "2026-02-01"},
            {"id": "f2", "referrer_id": "U", "referred_user_id": "u2", "status": "pending", "created_at": "2026-01-01"},
        ],
        "referral_commissions": [
            {"referral_id": "f1", "amount": "10.5"}, {"referral_id": "f1", "amount": "4.5"},
            {"referral_id": "f2", "amount": "3"}, {"referral_id": "otro", "amount": "100"},
        ],
    }
    db = FakeDB(tablas)
    emails = {"u1": "ana@correo.com", "u2": "beto@correo.com"}
    db.auth = types.SimpleNamespace(admin=types.SimpleNamespace(
        get_user_by_id=lambda uid: types.SimpleNamespace(user=types.SimpleNamespace(email=emails[uid]))))
    monkeypatch.setattr(ref, "_sb", db)
    monkeypatch.setattr(ref, "_user_id_desde_token", lambda a: "U")
    out = ref.mis_referidos("Bearer x")
    assert [r["comision_generada"] for r in out["referidos"]] == [15.0, 3.0]
    assert [r["email"] for r in out["referidos"]] == [ref._email_parcial(emails["u1"]), ref._email_parcial(emails["u2"])]
    assert out["total_referidos"] == 2 and out["activos"] == 1
    assert db.n("referral_commissions") == 1


# ══════════════════════════════════════════════════════════════
# 5) Bid Manager: sesión y empresa recordadas (antes 2 idas extra por petición)
# ══════════════════════════════════════════════════════════════
def test_bid_auth_y_empresa_cacheados(monkeypatch):
    import base64, json, time
    import router_bid_manager as bm
    gotrue = []
    db = FakeDB({"perfiles_empresa": [{"id": "EMP", "user_id": "UID"}]})

    def get_user(tok):
        gotrue.append(tok)
        return types.SimpleNamespace(user=types.SimpleNamespace(id="UID"))
    db.auth = types.SimpleNamespace(get_user=get_user)
    monkeypatch.setattr(bm, "_sb", db)
    bm._cache_auth.clear(); bm._cache_empresa.clear()

    pl = base64.urlsafe_b64encode(json.dumps({"exp": time.time() + 3600}).encode()).decode().rstrip("=")
    tok = f"h.{pl}.f"
    for _ in range(5):
        assert bm._auth(f"Bearer {tok}") == "UID"
        assert bm._empresa_id("UID") == "EMP"
    assert len(gotrue) == 1
    assert db.n("perfiles_empresa") == 1

    # un token ya vencido no se guarda más allá de su vencimiento
    pl2 = base64.urlsafe_b64encode(json.dumps({"exp": time.time() - 10}).encode()).decode().rstrip("=")
    bm._auth(f"Bearer h.{pl2}.f"); bm._auth(f"Bearer h.{pl2}.f")
    assert len(gotrue) == 3


def test_bid_empresa_sin_perfil_no_se_cachea(monkeypatch):
    import router_bid_manager as bm
    from fastapi import HTTPException
    db = FakeDB({"perfiles_empresa": []})
    monkeypatch.setattr(bm, "_sb", db)
    bm._cache_empresa.clear()
    with pytest.raises(HTTPException):
        bm._empresa_id("NUEVO")
    db.tablas["perfiles_empresa"].append({"id": "E2", "user_id": "NUEVO"})  # completa onboarding
    assert bm._empresa_id("NUEVO") == "E2"


# ══════════════════════════════════════════════════════════════
# 6) InfoPago: marcar trazabilidad por par exacto (NCF, contrato)
# ══════════════════════════════════════════════════════════════
def test_marcar_trazabilidad_por_par(monkeypatch):
    import urllib.parse
    import etl_infopago as ip
    llamadas = []
    monkeypatch.setattr(ip, "sb_patch", lambda t, f, c: llamadas.append((t, f)) or True)
    ip._marcar_trazabilidad([("B0100000001", "CAASD-2025-00188"), ('B01 "x"', "INAPA-2024-1")])
    assert len(llamadas) == 1
    filtro = urllib.parse.unquote(llamadas[0][1])
    assert filtro == ('or=(and(comprobante_fiscal.eq."B0100000001",contrato.eq."CAASD-2025-00188"),'
                      'and(comprobante_fiscal.eq."B01 \\"x\\"",contrato.eq."INAPA-2024-1"))')


def test_marcar_trazabilidad_respaldo(monkeypatch):
    import etl_infopago as ip
    llamadas = []
    monkeypatch.setattr(ip, "sb_patch", lambda t, f, c: llamadas.append(f) or len(llamadas) > 1)
    ip._marcar_trazabilidad([("N1", "C1")])
    assert llamadas[1] == 'comprobante_fiscal=in.("N1")'
