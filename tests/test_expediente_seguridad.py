"""
Tests de seguridad Fase 0 — el test que falla el build si un endpoint queda abierto.

- TODO endpoint sin token → 401 (nunca 200, nunca 500).
- Token de otra empresa → 404 (no 403: no revelar existencia).
- Entidad fuera del enum → 422. Contenido falso → 415. Muy grande → 413.
- Cron de purga sin X-Admin-Key → 403.

Corre local sin red: Supabase se mockea, la auth se monkeypatchea.
    SUPABASE_URL=https://dummy.supabase.co SUPABASE_KEY=dummy \
    pytest tests/test_expediente_seguridad.py -v
"""

import os, sys
import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.environ.setdefault("SUPABASE_URL", "https://dummy.supabase.co")
os.environ.setdefault("SUPABASE_KEY", "dummy")

from fastapi import FastAPI
from fastapi.testclient import TestClient

import router_bid_archivos as mod
from expediente_core import MAX_BYTES_ARCHIVO

app = FastAPI()
app.include_router(mod.bid_archivos_router)
client = TestClient(app, raise_server_exceptions=False)

RID = "11111111-1111-1111-1111-111111111111"
AID = "22222222-2222-2222-2222-222222222222"

TODOS_LOS_ENDPOINTS = [
    ("POST",   f"/api/bid/expediente/personal/{RID}/archivos"),
    ("GET",    f"/api/bid/expediente/personal/{RID}/archivos"),
    ("GET",    "/api/bid/expediente/archivos/conteos?entidad=personal"),
    ("GET",    f"/api/bid/expediente/archivos/{AID}/descargar"),
    ("PATCH",  f"/api/bid/expediente/archivos/{AID}"),
    ("DELETE", f"/api/bid/expediente/archivos/{AID}"),
    ("GET",    "/api/bid/expediente/actividad"),
]


# ── 1) Sin token → 401 en TODOS los endpoints ──────────────

@pytest.mark.parametrize("metodo,ruta", TODOS_LOS_ENDPOINTS)
def test_sin_token_401(metodo, ruta):
    r = client.request(metodo, ruta)
    assert r.status_code == 401, f"{metodo} {ruta} devolvió {r.status_code}, esperaba 401"


def test_token_malformado_401():
    r = client.get("/api/bid/expediente/actividad",
                   headers={"Authorization": "no-es-bearer"})
    assert r.status_code == 401


# ── Fixtures: auth mockeada + Supabase falso ───────────────

class _FakeQuery:
    """Query builder mínimo que devuelve siempre vacío (registro ajeno)."""
    def __getattr__(self, _):
        return lambda *a, **k: self
    def execute(self):
        class R: data = []
        return R()

class _FakeSB:
    def table(self, _): return _FakeQuery()
    class storage:
        @staticmethod
        def from_(_): raise AssertionError("no debería tocar storage")


@pytest.fixture
def como_usuario(monkeypatch):
    monkeypatch.setattr(mod, "_auth", lambda a: "user-uuid")
    monkeypatch.setattr(mod, "_empresa_id", lambda u: "empresa-uuid")
    monkeypatch.setattr(mod, "_sb", _FakeSB())
    yield


AUTH = {"Authorization": "Bearer token-de-prueba"}


# ── 2) Cross-tenant: registro/archivo ajeno → 404 ──────────

def test_registro_de_otra_empresa_404(como_usuario):
    r = client.get(f"/api/bid/expediente/personal/{RID}/archivos", headers=AUTH)
    assert r.status_code == 404

def test_archivo_de_otra_empresa_404(como_usuario):
    assert client.get(f"/api/bid/expediente/archivos/{AID}/descargar", headers=AUTH).status_code == 404
    assert client.delete(f"/api/bid/expediente/archivos/{AID}", headers=AUTH).status_code == 404
    assert client.patch(f"/api/bid/expediente/archivos/{AID}",
                        json={"estado": "validado"}, headers=AUTH).status_code == 404


# ── 3) Validaciones ────────────────────────────────────────

def test_entidad_invalida_422(como_usuario):
    r = client.get(f"/api/bid/expediente/hackers/{RID}/archivos", headers=AUTH)
    assert r.status_code == 422
    assert r.json()["detail"]["code"] == "ENTIDAD_INVALIDA"

def test_estado_invalido_422(como_usuario, monkeypatch):
    monkeypatch.setattr(mod, "_archivo_de_empresa",
                        lambda i, e: {"entidad": "personal", "registro_id": RID, "nombre": "x"})
    r = client.patch(f"/api/bid/expediente/archivos/{AID}",
                     json={"estado": "aprobadisimo"}, headers=AUTH)
    assert r.status_code == 422
    assert r.json()["detail"]["code"] == "ESTADO_INVALIDO"


@pytest.fixture
def con_registro_valido(como_usuario, monkeypatch):
    monkeypatch.setattr(mod, "_validar_registro", lambda e, r, i: None)
    yield

def test_exe_disfrazado_de_pdf_415(con_registro_valido):
    r = client.post(f"/api/bid/expediente/personal/{RID}/archivos", headers=AUTH,
                    files={"files": ("virus.pdf", b"MZ\x90\x00basura", "application/pdf")})
    assert r.status_code == 415
    assert r.json()["detail"]["code"] == "CONTENIDO_NO_RECONOCIDO"

def test_archivo_gigante_413(con_registro_valido):
    grande = b"%PDF" + b"0" * MAX_BYTES_ARCHIVO
    r = client.post(f"/api/bid/expediente/personal/{RID}/archivos", headers=AUTH,
                    files={"files": ("acta.pdf", grande, "application/pdf")})
    assert r.status_code == 413
    assert r.json()["detail"]["code"] == "ARCHIVO_MUY_GRANDE"

def test_demasiados_archivos_422(con_registro_valido):
    files = [("files", (f"a{i}.pdf", b"%PDF ok", "application/pdf")) for i in range(6)]
    r = client.post(f"/api/bid/expediente/personal/{RID}/archivos", headers=AUTH, files=files)
    assert r.status_code == 422
    assert r.json()["detail"]["code"] == "DEMASIADOS_ARCHIVOS"


# ── 4) Cron de purga protegido ─────────────────────────────

def test_purga_sin_admin_key_403():
    r = client.post("/api/bid/expediente/cron/purgar-archivos")
    assert r.status_code == 403

def test_purga_con_key_equivocada_403(monkeypatch):
    monkeypatch.setenv("ADMIN_SECRET", "clave-real")
    r = client.post("/api/bid/expediente/cron/purgar-archivos",
                    headers={"X-Admin-Key": "clave-falsa"})
    assert r.status_code == 403
