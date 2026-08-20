"""
Tests Fase 4 — Generar Sobre A (asíncrono) + formularios del portal.

Corren local sin red: Supabase mockeado, auth monkeypatcheada.
    SUPABASE_URL=https://dummy.supabase.co SUPABASE_KEY=dummy \
    pytest tests/test_expediente_fase4.py -v
"""

import io
import os
import sys
import zipfile

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.environ.setdefault("SUPABASE_URL", "https://dummy.supabase.co")
os.environ.setdefault("SUPABASE_KEY", "dummy")

from fastapi import FastAPI
from fastapi.testclient import TestClient

import router_sobre_a as mod
import sobre_a_adjuntos as adj
import sobre_a_core as core

app = FastAPI()
app.include_router(mod.sobre_a_router)
client = TestClient(app, raise_server_exceptions=False)

JID = "11111111-1111-1111-1111-111111111111"
FID = "22222222-2222-2222-2222-222222222222"

TODOS_LOS_ENDPOINTS = [
    ("GET",  "/api/bid/expediente/sobre-a/piezas?referencia=REF-1"),
    ("POST", "/api/bid/expediente/sobre-a/generar"),
    ("GET",  f"/api/bid/expediente/sobre-a/jobs/{JID}"),
    ("GET",  "/api/bid/expediente/sobre-a/jobs"),
    ("POST", "/api/bid/expediente/sobre-a/sincronizar-formularios?referencia=REF-1"),
    ("POST", f"/api/bid/expediente/sobre-a/formularios/{FID}/llenado"),
]


# ══════════════════════════════════════════════════════════════
# Fakes
# ══════════════════════════════════════════════════════════════

class FakeQuery:
    def __init__(self, data=None):
        self.data = data or []
        self.count = len(self.data)

    def __getattr__(self, name):
        return lambda *a, **k: self

    def execute(self):
        return self


class FakeStorage:
    subidos = {}

    def from_(self, bucket):
        return self

    def upload(self, path, contenido, file_options=None):
        FakeStorage.subidos[path] = contenido

    def create_signed_url(self, path, ttl, opts=None):
        return {"signedURL": f"https://firmada/{path}"}

    def download(self, path):
        return FakeStorage.subidos.get(path, b"%PDF-1.7 fake")


class FakeSB:
    """Supabase mínimo: tablas en memoria con filtro por eq()."""

    def __init__(self, tablas=None):
        self.tablas = tablas or {}
        self.storage = FakeStorage()
        self.inserts = []
        self.updates = []

    def table(self, nombre):
        sb = self

        class Q:
            def __init__(q):
                q.filtros = {}
                q._data_override = None

            def select(q, *a, **k):
                return q

            def insert(q, fila):
                fila = dict(fila)
                fila.setdefault("id", JID)
                fila.setdefault("creado_en", "2026-08-20T00:00:00+00:00")
                sb.inserts.append((nombre, fila))
                sb.tablas.setdefault(nombre, []).append(fila)
                q._data_override = [fila]
                return q

            def update(q, campos):
                sb.updates.append((nombre, dict(campos)))
                for fila in sb.tablas.get(nombre, []):
                    if all(fila.get(k) == v for k, v in q.filtros.items()):
                        fila.update(campos)
                q._data_override = []
                return q

            def upsert(q, fila, **k):
                sb.tablas.setdefault(nombre, []).append(dict(fila))
                q._data_override = [fila]
                return q

            def eq(q, campo, valor):
                q.filtros[campo] = valor
                return q

            def __getattr__(q, n):
                return lambda *a, **k: q

            def execute(q):
                if q._data_override is not None:
                    q.data = q._data_override
                else:
                    q.data = [f for f in sb.tablas.get(nombre, [])
                              if all(f.get(k) == v for k, v in q.filtros.items())]
                q.count = len(q.data)
                return q

        return Q()


@pytest.fixture
def como_empresa(monkeypatch):
    """Auth ok, empresa 'emp-1', Supabase fake vacío."""
    fake = FakeSB()
    FakeStorage.subidos = {}
    monkeypatch.setattr(mod, "_auth", lambda a: "user-1")
    monkeypatch.setattr(mod, "_empresa_id", lambda uid: "emp-1")
    monkeypatch.setattr(mod, "_sb", fake)
    return fake


HDR = {"Authorization": "Bearer token-valido"}


# ══════════════════════════════════════════════════════════════
# 1) Sin token → 401 en TODOS los endpoints
# ══════════════════════════════════════════════════════════════

@pytest.mark.parametrize("metodo,ruta", TODOS_LOS_ENDPOINTS)
def test_sin_token_401(metodo, ruta):
    kwargs = {}
    if metodo == "POST":
        if "llenado" in ruta:
            # multipart: sin archivo FastAPI corta en 422 antes de la auth;
            # con archivo dummy la validación pasa y se prueba la auth real
            kwargs["files"] = {"archivo": ("f.pdf", b"%PDF-1.7 x")}
        else:
            kwargs["json"] = {"referencia": "R"}
    r = client.request(metodo, ruta, **kwargs)
    assert r.status_code == 401, f"{metodo} {ruta} → {r.status_code}, esperaba 401"


# ══════════════════════════════════════════════════════════════
# 2) Cross-tenant: job/formulario ajeno → 404 (nunca 403)
# ══════════════════════════════════════════════════════════════

def test_job_ajeno_404(como_empresa):
    como_empresa.tablas["bid_sobre_jobs"] = [
        {"id": JID, "empresa_id": "OTRA-empresa", "estado": "listo",
         "referencia": "R", "formato": "docx",
         "creado_en": "x", "resultado": {}}]
    r = client.get(f"/api/bid/expediente/sobre-a/jobs/{JID}", headers=HDR)
    assert r.status_code == 404


def test_llenado_formulario_ajeno_404(como_empresa):
    como_empresa.tablas["bid_formularios_proceso"] = [
        {"id": FID, "empresa_id": "OTRA-empresa", "referencia": "R"}]
    r = client.post(f"/api/bid/expediente/sobre-a/formularios/{FID}/llenado",
                    headers=HDR, files={"archivo": ("f.pdf", b"%PDF-1.7 x")})
    assert r.status_code == 404


def test_job_id_invalido_404(como_empresa):
    r = client.get("/api/bid/expediente/sobre-a/jobs/no-es-uuid", headers=HDR)
    assert r.status_code == 404


# ══════════════════════════════════════════════════════════════
# 3) Contrato del job: 202, persistencia, worker, signed URL
# ══════════════════════════════════════════════════════════════

def test_generar_202_y_job_persistido(como_empresa, monkeypatch):
    mod.rate_limiter._hits.clear()
    # worker no corre de verdad (TestClient sí ejecuta background tasks,
    # así que lo apagamos para probar solo el contrato HTTP)
    monkeypatch.setattr(mod, "_worker_generar", lambda *a, **k: None)
    r = client.post("/api/bid/expediente/sobre-a/generar", headers=HDR,
                    json={"referencia": "REF-1", "piezas": "todas"})
    assert r.status_code == 202
    body = r.json()
    assert body["job_id"] and body["estado"] == "en_cola"
    tablas = [t for t, _f in como_empresa.inserts]
    assert "bid_sobre_jobs" in tablas, "el job DEBE persistirse en BD"
    assert "bid_auditoria" in tablas, "genero_sobre_a debe auditarse"


def test_generar_formato_invalido_422(como_empresa):
    mod.rate_limiter._hits.clear()
    r = client.post("/api/bid/expediente/sobre-a/generar", headers=HDR,
                    json={"referencia": "R", "formato": "exe"})
    assert r.status_code == 422


def test_generar_rate_limit_429(como_empresa, monkeypatch):
    mod.rate_limiter._hits.clear()
    monkeypatch.setattr(mod, "_worker_generar", lambda *a, **k: None)
    for _ in range(10):  # LIMITE_SOBRE_A = 10/hora
        assert client.post("/api/bid/expediente/sobre-a/generar", headers=HDR,
                           json={"referencia": "R"}).status_code == 202
    r = client.post("/api/bid/expediente/sobre-a/generar", headers=HDR,
                    json={"referencia": "R"})
    assert r.status_code == 429


def test_worker_cierra_job_listo(como_empresa, monkeypatch):
    como_empresa.tablas["bid_sobre_jobs"] = [{
        "id": JID, "empresa_id": "emp-1", "referencia": "REF-1",
        "piezas": ["f034"], "formato": "docx", "estado": "en_cola"}]
    monkeypatch.setattr(core, "construir_paquete",
                        lambda *a, **k: {"zip_bytes": b"PK\x03\x04zip",
                                         "nombre": "Sobre_A_REF-1.zip",
                                         "indice": [], "pendientes": [],
                                         "omitidas": []})
    monkeypatch.setattr(mod, "_notificar_push", lambda *a, **k: None)
    mod._worker_generar(JID, "emp-1", "user-1")
    job = como_empresa.tablas["bid_sobre_jobs"][0]
    assert job["estado"] == "listo"
    assert job["resultado"]["nombre"] == "Sobre_A_REF-1.zip"
    assert any(p.startswith("sobre_a/emp-1/REF-1/") for p in FakeStorage.subidos)


def test_worker_error_no_revienta(como_empresa, monkeypatch):
    como_empresa.tablas["bid_sobre_jobs"] = [{
        "id": JID, "empresa_id": "emp-1", "referencia": "REF-1",
        "piezas": ["todas"], "formato": "docx", "estado": "en_cola"}]

    def explota(*a, **k):
        raise RuntimeError("bum")
    monkeypatch.setattr(core, "construir_paquete", explota)
    monkeypatch.setattr(mod, "_notificar_push", lambda *a, **k: None)
    mod._worker_generar(JID, "emp-1", "user-1")   # no debe lanzar
    job = como_empresa.tablas["bid_sobre_jobs"][0]
    assert job["estado"] == "error" and "bum" in job["error"]


def test_job_listo_da_url_firmada_sin_storage_path(como_empresa):
    como_empresa.tablas["bid_sobre_jobs"] = [{
        "id": JID, "empresa_id": "emp-1", "referencia": "R", "formato": "docx",
        "estado": "listo", "creado_en": "x",
        "resultado": {"storage_path": "sobre_a/emp-1/R/z.zip", "nombre": "z.zip"},
        "pendientes": [], "progreso": {}}]
    r = client.get(f"/api/bid/expediente/sobre-a/jobs/{JID}", headers=HDR)
    assert r.status_code == 200
    res = r.json()["resultado"]
    assert res["url_firmada"].startswith("https://firmada/")
    assert "storage_path" not in res, "la ruta interna del bucket no sale al cliente"


def test_llenado_contenido_falso_415(como_empresa):
    como_empresa.tablas["bid_formularios_proceso"] = [
        {"id": FID, "empresa_id": "emp-1", "referencia": "R"}]
    r = client.post(f"/api/bid/expediente/sobre-a/formularios/{FID}/llenado",
                    headers=HDR,
                    files={"archivo": ("malo.pdf", b"MZ\x90\x00ejecutable")})
    assert r.status_code == 415


# ══════════════════════════════════════════════════════════════
# 4) D2 en el paquete: vencido JAMÁS entra al ZIP
# ══════════════════════════════════════════════════════════════

def _fake_sb_catalogo(monkeypatch, piezas):
    monkeypatch.setattr(core, "listar_piezas", lambda *a, **k: list(piezas))
    monkeypatch.setattr(core, "datos_contexto", lambda *a, **k: {
        "empresa": {"razon_social": "X"}, "proceso": {}, "firmante": {},
        "firma_png": None, "referencia": "R", "institucion": "INST"})


def test_paquete_omite_vencidos(monkeypatch):
    piezas = [
        {"id": "doc:certificaciones:1:a", "tipo": "documento",
         "nombre": "TSS", "estado": "err",
         "motivo": "Documento vencido — bloqueado, no entra al ZIP",
         "entidad": "certificaciones", "registro_id": "1", "archivo_id": "a"},
        {"id": "f034", "tipo": "formulario", "nombre": "F.034",
         "estado": "ok", "motivo": None},
    ]
    _fake_sb_catalogo(monkeypatch, piezas)
    r = core.construir_paquete(FakeSB(), "emp-1", "R", "todas", formato="docx")
    nombres = zipfile.ZipFile(io.BytesIO(r["zip_bytes"])).namelist()
    assert not any("TSS" in n for n in nombres), "vencido JAMÁS entra al ZIP"
    assert any(o["nombre"] == "TSS" for o in r["omitidas"])
    assert any(p["nombre"] == "TSS" for p in r["pendientes"])


def test_paquete_doc_faltante_solo_separador(monkeypatch):
    piezas = [{"id": "doc:certificaciones:1:falta", "tipo": "documento",
               "nombre": "Carta compromiso", "estado": "err",
               "motivo": "Sin archivo adjunto — entrará solo la hoja separadora",
               "entidad": "certificaciones", "registro_id": "1"}]
    _fake_sb_catalogo(monkeypatch, piezas)
    r = core.construir_paquete(FakeSB(), "emp-1", "R", "todas", formato="docx")
    nombres = zipfile.ZipFile(io.BytesIO(r["zip_bytes"])).namelist()
    assert any("Separador" in n for n in nombres)


# ══════════════════════════════════════════════════════════════
# 5) Fase 4b: detección y matching de formularios del portal
# ══════════════════════════════════════════════════════════════

def test_deteccion_ignora_generables_y_docs_normales():
    class SB(FakeSB):
        def __init__(self):
            super().__init__({"bid_matching_pliegos": [{
                "empresa_id": "e", "referencia": "R",
                "requisitos": {"otros": [
                    {"nombre": "Formulario SNCC.F.034 de Presentación de Oferta"},
                    {"nombre": "Formulario debida diligencia FR-CYC-002"},
                    {"nombre": "Certificación TSS al día"},
                ]}}]})
    det = adj.detectar_formularios_requeridos(SB(), "e", "R")
    assert [d["nombre"] for d in det] == ["Formulario debida diligencia FR-CYC-002"]


def test_matching_por_codigo_gana_y_pliego_no_matchea():
    req = [{"nombre": "Formulario debida diligencia FR-CYC-002", "texto_original": ""}]
    inv = [
        {"nombre": "Pliego LPN-2025-0069 LA PAVA.pdf", "file_id": "9", "mkey": "z"},
        {"nombre": "FR-CYC-002 -V4- Formulario Debida Diligencia.docx",
         "file_id": "1", "mkey": "a"},
    ]
    parejas = adj._matchear(req, inv)
    assert len(parejas) == 1 and parejas[0][1]["file_id"] == "1"


def test_pieza_formulario_proceso_prefiere_llenado():
    class SB(FakeSB):
        def __init__(self):
            super().__init__({"bid_formularios_proceso": [{
                "id": "x", "empresa_id": "e", "referencia": "R",
                "requisito": "CEI", "nombre_archivo": "cei.pdf",
                "storage_path": "p/cei.pdf",
                "llenado_storage_path": "p/cei_lleno.pdf"}]})
    p = adj.piezas_formularios_proceso(SB(), "e", "R")[0]
    assert p["estado"] == "ok" and p["_legacy_path"] == "p/cei_lleno.pdf"
