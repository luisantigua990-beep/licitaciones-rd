"""
Tests Fase 0 — Expediente Digital v2
Corren sin red ni Supabase: lógica pura de expediente_core.

    pytest tests/test_expediente_fase0.py -v
"""

import sys, os
import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from expediente_core import (
    resolver_estado, validar_contenido,
    MAX_BYTES_ARCHIVO, EXT_PERMITIDAS,
    ENTIDADES_ADJUNTOS, TABLA_POR_ENTIDAD,
    ESTADO_VERIFICACION, ESTADO_PIEZA,
    error_body, RateLimiter,
)


# ── D2: tabla de verdad completa de resolver_estado ────────

@pytest.mark.parametrize("verificacion,vencido,esperado", [
    ("validado",  False, "ok"),
    ("pendiente", False, "warn"),
    ("rechazado", False, "err"),
    (None,        False, "err"),
    ("cualquier_basura", False, "err"),
    # vencido gana SIEMPRE — un doc vencido jamás entra al Sobre A
    ("validado",  True,  "err"),
    ("pendiente", True,  "err"),
    ("rechazado", True,  "err"),
    (None,        True,  "err"),
])
def test_resolver_estado(verificacion, vencido, esperado):
    assert resolver_estado(verificacion, vencido) == esperado


def test_resolver_estado_solo_devuelve_estados_del_enum():
    for v in list(ESTADO_VERIFICACION) + [None, "x"]:
        for venc in (True, False):
            assert resolver_estado(v, venc) in ESTADO_PIEZA


# ── Magic bytes: el contenido manda, no la extensión ───────

PDF = b"%PDF-1.7 contenido"
JPG = b"\xff\xd8\xff\xe0" + b"x" * 10
PNG = b"\x89PNG\r\n\x1a\n" + b"x" * 10
OOXML = b"PK\x03\x04" + b"x" * 10          # docx / xlsx
OLE2 = b"\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1" + b"x" * 10  # doc / xls
EXE = b"MZ\x90\x00" + b"x" * 10


def test_pdf_valido():
    ext, mime = validar_contenido("acta.pdf", PDF)
    assert (ext, mime) == ("pdf", "application/pdf")


def test_docx_y_xlsx_comparten_firma_zip():
    assert validar_contenido("cv.docx", OOXML)[1].endswith("wordprocessingml.document")
    assert validar_contenido("cubicacion.xlsx", OOXML)[1].endswith("spreadsheetml.sheet")


def test_doc_y_xls_legacy():
    assert validar_contenido("cv.doc", OLE2)[0] == "doc"
    assert validar_contenido("apu.xls", OLE2)[0] == "xls"


def test_imagenes():
    assert validar_contenido("cedula.jpg", JPG)[1] == "image/jpeg"
    assert validar_contenido("foto.jpeg", JPG)[1] == "image/jpeg"
    assert validar_contenido("plano.png", PNG)[1] == "image/png"


def test_exe_renombrado_a_pdf_rechazado():
    with pytest.raises(ValueError, match="CONTENIDO_NO_RECONOCIDO"):
        validar_contenido("factura.pdf", EXE)


def test_jpg_renombrado_a_pdf_rechazado():
    # contenido reconocible pero que NO coincide con la extensión
    with pytest.raises(ValueError, match="CONTENIDO_NO_COINCIDE"):
        validar_contenido("factura.pdf", JPG)


def test_extension_no_permitida():
    with pytest.raises(ValueError, match="TIPO_NO_PERMITIDO"):
        validar_contenido("script.exe", EXE)
    with pytest.raises(ValueError, match="TIPO_NO_PERMITIDO"):
        validar_contenido("sin_extension", PDF)


def test_archivo_vacio():
    with pytest.raises(ValueError, match="ARCHIVO_VACIO"):
        validar_contenido("acta.pdf", b"")


def test_archivo_muy_grande():
    grande = b"%PDF" + b"x" * MAX_BYTES_ARCHIVO
    with pytest.raises(ValueError, match="ARCHIVO_MUY_GRANDE"):
        validar_contenido("acta.pdf", grande)


# ── Enums y mapeos coherentes ──────────────────────────────

def test_toda_entidad_tiene_tabla():
    assert set(ENTIDADES_ADJUNTOS) == set(TABLA_POR_ENTIDAD.keys())
    for tabla in TABLA_POR_ENTIDAD.values():
        assert tabla.startswith("bid_")


def test_error_body_envelope():
    body = error_body("ARCHIVO_MUY_GRANDE", "files")
    assert body["error"]["code"] == "ARCHIVO_MUY_GRANDE"
    assert "15 MB" in body["error"]["message"]
    assert body["error"]["field"] == "files"


def test_toda_extension_permitida_tiene_firma_cubierta():
    """Cada extensión de la whitelist debe ser validable por alguna firma."""
    muestras = {"pdf": PDF, "jpg": JPG, "jpeg": JPG, "png": PNG,
                "docx": OOXML, "xlsx": OOXML, "doc": OLE2, "xls": OLE2}
    assert set(muestras) == set(EXT_PERMITIDAS)
    for ext, contenido in muestras.items():
        assert validar_contenido(f"a.{ext}", contenido)[0] == ext


# ── Rate limiter ───────────────────────────────────────────

def test_rate_limiter_bloquea_y_libera():
    rl = RateLimiter()
    for _ in range(3):
        assert rl.permitir("u1", 3, 60)
    assert not rl.permitir("u1", 3, 60)        # cuarta en la ventana → bloqueada
    assert rl.permitir("u2", 3, 60)            # otra clave no se afecta
