"""
obras_import.py — Lectura automática de Excel y facturas para Control de Obra
=============================================================================
Funciones puras (sin FastAPI) para poder probarlas solas:

- leer_presupuesto(bytes, hoja=None, mapeo=None)
    Detecta la hoja del listado, la fila de encabezados y las columnas
    (código, descripción, unidad, cantidad, P.U. contratado, P.U. de costo),
    arma los capítulos y separa los gastos indirectos.

- leer_gastos_excel(bytes, hoja=None, mapeo=None)
    Relación de gastos / caja chica: fecha, proveedor, RNC, NCF, concepto,
    monto, ITBIS, total y, si existe, el código de partida.

- sugerir_partidas(textos, partidas)
    Sugiere la partida de cada gasto por coincidencia de palabras.
    El router puede mejorar esto con Gemini cuando hay clave.

Nada aquí guarda en la base: devuelve vistas previas que el usuario confirma.
"""

from __future__ import annotations

import csv
import io
import re
import unicodedata
from datetime import date, datetime

import openpyxl

# ══════════════════════════════════════════════════════════════
# Utilidades
# ══════════════════════════════════════════════════════════════

def _norm(s) -> str:
    """Mayúsculas sin acentos y con espacios simples."""
    if s is None:
        return ""
    s = unicodedata.normalize("NFKD", str(s)).encode("ascii", "ignore").decode()
    return re.sub(r"\s+", " ", s).strip().upper()


def _es_num(v) -> bool:
    return isinstance(v, (int, float)) and not isinstance(v, bool)


def _num(v):
    """Número desde celda (acepta '1,234.50', 'RD$ 1,234', '(500)')."""
    if v is None or v == "":
        return None
    if _es_num(v):
        return float(v)
    s = str(v).strip()
    neg = s.startswith("(") and s.endswith(")")
    s = re.sub(r"[^\d.,\-]", "", s)
    if not s or s in "-.,":
        return None
    # 1.234,50 (formato europeo) -> 1234.50
    if s.count(",") == 1 and s.count(".") >= 1 and s.rfind(",") > s.rfind("."):
        s = s.replace(".", "").replace(",", ".")
    else:
        s = s.replace(",", "")
    try:
        n = float(s)
    except ValueError:
        return None
    return -n if neg else n


def _texto(v) -> str:
    if v is None:
        return ""
    if isinstance(v, (datetime, date)):
        return v.strftime("%Y-%m-%d")
    return re.sub(r"\s+", " ", str(v)).strip()


def _codigo(v, fmt: str = "") -> str:
    """Código de partida legible. 2.0199999 -> '2.02'; 4.1 con formato 0.00 -> '4.10'."""
    if v is None:
        return ""
    if isinstance(v, bool):
        return ""
    if isinstance(v, int) or (isinstance(v, float) and float(v).is_integer()):
        return str(int(v))
    if isinstance(v, float):
        m = re.search(r"0\.(0+)", fmt or "")
        dec = len(m.group(1)) if m else 2
        dec = max(dec, 2)
        s = f"{v:.{dec}f}"
        # Si el redondeo a 2 decimales pierde información real (1.005), usar más
        if abs(float(s) - v) > 1e-6 and dec == 2:
            s = f"{v:.3f}"
        return s
    return _texto(v)


def _fecha(v):
    if v is None or v == "":
        return None
    if isinstance(v, datetime):
        return v.date().isoformat()
    if isinstance(v, date):
        return v.isoformat()
    if _es_num(v) and 20000 < v < 80000:          # serial de Excel
        try:
            return openpyxl.utils.datetime.from_excel(v).date().isoformat()
        except Exception:
            return None
    s = str(v).strip()
    for f in ("%d/%m/%Y", "%d-%m-%Y", "%Y-%m-%d", "%d/%m/%y", "%d.%m.%Y", "%Y/%m/%d"):
        try:
            return datetime.strptime(s[:10], f).date().isoformat()
        except ValueError:
            continue
    return None


def _letra(i: int) -> str:
    return openpyxl.utils.get_column_letter(i + 1)


def _abrir(contenido: bytes, nombre: str = ""):
    """Devuelve lista de (nombre_hoja, filas[list[list]], formatos[list[list]]) visibles."""
    nombre = (nombre or "").lower()
    if nombre.endswith(".csv") or (not contenido[:4] == b"PK\x03\x04" and b"," in contenido[:2000]):
        txt = contenido.decode("utf-8-sig", errors="replace")
        dialecto = csv.Sniffer().sniff(txt[:5000], delimiters=",;\t") if txt.strip() else csv.excel
        filas = [list(r) for r in csv.reader(io.StringIO(txt), dialecto)]
        return [("CSV", filas, [[""] * len(r) for r in filas])]
    if contenido[:4] != b"PK\x03\x04":
        raise ValueError("El archivo no es un Excel .xlsx. Si es .xls viejo, ábrelo y guárdalo como .xlsx.")
    wb = openpyxl.load_workbook(io.BytesIO(contenido), data_only=True, read_only=False)
    hojas = []
    for ws in wb.worksheets:
        if ws.sheet_state != "visible":
            continue
        filas, fmts = [], []
        maxc = min(ws.max_column or 0, 40)
        for row in ws.iter_rows(min_row=1, max_row=min(ws.max_row or 0, 5000), max_col=maxc):
            filas.append([c.value for c in row])
            fmts.append([c.number_format or "" for c in row])
        hojas.append((ws.title, filas, fmts))
    return hojas


# ══════════════════════════════════════════════════════════════
# 1. PRESUPUESTO / LISTADO DE PARTIDAS
# ══════════════════════════════════════════════════════════════

# Palabras clave de cada columna (sobre texto normalizado)
_K_PRES = {
    "codigo":      [r"^NO\.?$", r"^N[O°º]\.?$", r"^ITEM", r"^COD", r"^#$", r"^PARTIDA NO", r"^NUM"],
    "descripcion": [r"DESCRIP", r"^CONCEPTO", r"^PARTIDAS?$", r"^ACTIVIDAD", r"^DETALLE"],
    "unidad":      [r"^UD\.?S?$", r"^UND", r"^UNID", r"^U\.?M\.?$", r"^U$", r"^MEDIDA"],
    "cantidad":    [r"^CANT", r"^VOLUMEN", r"^METRADO"],
    "pu":          [r"P\.? ?U\.?", r"PRECIO", r"UNITARIO", r"^P/U", r"COSTO UNIT", r"^C\.? ?U\.?$"],
    "total":       [r"^COSTO", r"^TOTAL", r"IMPORTE", r"^MONTO", r"^VALOR", r"^PARCIAL"],
}
# Si el grupo de encabezado de arriba dice esto, ese P.U. es costo (no precio de contrato)
_GRUPO_COSTO = re.compile(r"EJEC|COSTO|APU|REAL|INTERNO|CONSTRUCTOR|PRESUPUESTO DE OBRA")

_FIN_DIRECTOS = re.compile(r"SUB ?-?TOTAL GENERAL|^SUB ?-?TOTAL$|TOTAL COSTO DIRECTO|TOTAL DIRECTO|COSTO DIRECTO TOTAL")
_INICIO_IND = re.compile(r"GASTOS? INDIRECTOS?|COSTOS? INDIRECTOS?")
_FIN_TOTAL = re.compile(r"^TOTAL GENERAL|^TOTAL PRESUPUESTO|^TOTAL OFERTA|^GRAN TOTAL|^MONTO TOTAL|^TOTAL A PAGAR|^TOTAL$")


def _match(celda: str, pats) -> bool:
    return any(re.search(p, celda) for p in pats)


def _detectar_encabezado_pres(filas):
    """Busca en las primeras 40 filas la que más parece encabezado. Devuelve (fila, puntaje, cols)."""
    mejor = (None, 0, {})
    for i, fila in enumerate(filas[:40]):
        celdas = [_norm(c) for c in fila]
        cols = {}
        pus = []
        for j, c in enumerate(celdas):
            if not c or len(c) > 40:
                continue
            for campo in ("descripcion", "cantidad", "unidad", "codigo", "total"):
                if campo not in cols and _match(c, _K_PRES[campo]):
                    # "COSTO UNITARIO" es P.U., no total
                    if campo == "total" and _match(c, _K_PRES["pu"]):
                        continue
                    cols[campo] = j
                    break
            else:
                pass
            if _match(c, _K_PRES["pu"]) and j not in cols.values():
                pus.append(j)
        if pus:
            cols["pu_lista"] = pus
        puntaje = sum(k in cols for k in ("descripcion", "cantidad", "unidad")) * 2 + ("pu_lista" in cols) * 2 + ("codigo" in cols) + ("total" in cols)
        if "descripcion" in cols and puntaje > mejor[1]:
            mejor = (i, puntaje, cols)
    return mejor


def _columnas_pres(filas, fila_enc, cols):
    """Resuelve P.U. de contrato vs. P.U. de costo usando la fila de grupos de arriba."""
    pus = cols.get("pu_lista", [])
    grupos = {}
    if fila_enc and fila_enc > 0:
        # Grupos tipo "CLIENTE" / "EJECUCION" en la fila anterior (celdas combinadas => solo la primera tiene texto)
        arriba = filas[fila_enc - 1]
        actual = ""
        for j in range(len(arriba)):
            t = _norm(arriba[j])
            if t:
                actual = t
            grupos[j] = actual
    res = {k: cols[k] for k in ("codigo", "descripcion", "unidad", "cantidad", "total") if k in cols}
    pu_contrato = pu_costo = None
    for j in pus:
        g = grupos.get(j, "")
        if _GRUPO_COSTO.search(g) or _GRUPO_COSTO.search(_norm(filas[fila_enc][j])):
            if pu_costo is None:
                pu_costo = j
        elif pu_contrato is None:
            pu_contrato = j
        elif pu_costo is None:
            pu_costo = j
    if pu_contrato is None and pu_costo is not None and len(pus) == 1:
        pu_contrato, pu_costo = pu_costo, None
    if pu_contrato is not None:
        res["precio_unitario"] = pu_contrato
    if pu_costo is not None:
        res["costo_unitario"] = pu_costo
    # Total: el que queda a la derecha del P.U. de contrato y antes del de costo
    if "total" in res and pu_costo is not None and res["total"] > pu_costo:
        res.pop("total")
    # Sin columna de código: la de la izquierda de la descripción, si tiene valores
    if "codigo" not in res and res.get("descripcion", 0) > 0:
        res["codigo"] = res["descripcion"] - 1
    # Columna de subtotales de capítulo (a la derecha del total, sin encabezado de P.U.)
    return res


def _celda(fila, j):
    return fila[j] if j is not None and j < len(fila) else None


def leer_presupuesto(contenido: bytes, nombre: str = "", hoja: str | None = None, mapeo: dict | None = None) -> dict:
    hojas = _abrir(contenido, nombre)
    if not hojas:
        raise ValueError("El archivo no tiene hojas visibles")

    # Elegir hoja: la indicada o la de mejor encabezado
    candidatas = []
    for nom, filas, fmts in hojas:
        i, puntaje, cols = _detectar_encabezado_pres(filas)
        # Desempate: filas con cantidad numérica debajo del encabezado
        datos = 0
        if i is not None and "cantidad" in cols:
            datos = sum(1 for f in filas[i + 1:i + 400] if _es_num(_num(_celda(f, cols["cantidad"]))) if _num(_celda(f, cols["cantidad"])))
        candidatas.append((nom, puntaje, datos, i, cols, filas, fmts))
    if hoja:
        sel = next((c for c in candidatas if c[0] == hoja), None)
        if not sel:
            raise ValueError(f"No existe la hoja '{hoja}'")
    else:
        def _puntaje_hoja(c):
            n = _norm(c[0])
            bono = 0
            if re.search(r"PRESUP|LISTADO|PARTIDAS|CANTIDADES|OFERTA|LOTE|SNCC.?F.?033", n):
                bono += 40
            if re.search(r"ANALISIS|APU|ACU|INSUMO|MATERIAL|MANO|AUX|MAT-|EQUIPO|RESUMEN|CRONO|FLUJO", n):
                bono -= 60
            return c[1] * 10 + bono + min(c[2], 100) / 10
        sel = max(candidatas, key=_puntaje_hoja)
    nom, puntaje, _, fila_enc, cols_raw, filas, fmts = sel

    avisos = []
    if fila_enc is None:
        fila_enc = 0
        cols = {}
        avisos.append("No encontré la fila de encabezados; indica las columnas a mano.")
    else:
        cols = _columnas_pres(filas, fila_enc, cols_raw)

    if mapeo:
        if "fila_encabezado" in mapeo and mapeo["fila_encabezado"]:
            fila_enc = int(mapeo["fila_encabezado"]) - 1
        for k, v in mapeo.items():
            if k == "fila_encabezado":
                continue
            if v is None or v == "":
                cols.pop(k, None)
            else:
                cols[k] = int(v)

    encabezados = []
    if 0 <= fila_enc < len(filas):
        ancho = max(len(f) for f in filas[fila_enc:fila_enc + 50]) if filas else 0
        for j in range(ancho):
            t = _texto(_celda(filas[fila_enc], j))
            g = _texto(_celda(filas[fila_enc - 1], j)) if fila_enc > 0 else ""
            encabezados.append({"col": j, "letra": _letra(j), "texto": t, "grupo": g})

    cD, cC, cU, cP = cols.get("descripcion"), cols.get("cantidad"), cols.get("unidad"), cols.get("precio_unitario")
    cK, cT, cX = cols.get("codigo"), cols.get("total"), cols.get("costo_unitario")

    partidas, indirectos = [], []
    capitulo = None
    grupo = None
    modo = "directos"
    subtotal_excel = total_excel = None
    sin_total = []
    monto_sin_total = 0.0

    for r in range(fila_enc + 1, len(filas)):
        f = filas[r]
        desc = _texto(_celda(f, cD))
        if not desc:
            continue
        dn = _norm(desc)
        cod_raw = _celda(f, cK)
        cod = _codigo(cod_raw, fmts[r][cK] if cK is not None and cK < len(fmts[r]) else "")
        cant = _num(_celda(f, cC))
        pu = _num(_celda(f, cP))
        tot = _num(_celda(f, cT))
        nums_der = [n for n in (_num(x) for x in f[(cD or 0) + 1:]) if n is not None]

        if _FIN_TOTAL.search(dn):
            total_excel = max((n for n in nums_der), default=None, key=abs) if nums_der else None
            break
        if _FIN_DIRECTOS.search(dn):
            subtotal_excel = max(nums_der, key=abs) if nums_der else None
            modo = "indirectos_pend"
            continue
        if _INICIO_IND.search(dn):
            modo = "indirectos"
            continue

        if modo.startswith("indirectos"):
            # "Dirección Técnica  10%  ...  4,623,061.51" -> monto = mayor número de la fila
            grandes = [n for n in nums_der if abs(n) >= 1]
            if not grandes:
                continue
            monto = max(grandes, key=abs)
            pct = next((n for n in nums_der if 0 < abs(n) < 1), None)
            indirectos.append({
                "fila": r + 1, "codigo": cod or None, "descripcion": desc, "unidad": "PA",
                "cantidad": 1, "precio_unitario": round(monto, 2), "costo_unitario_apu": None,
                "porcentaje": pct, "capitulo": "GASTOS INDIRECTOS",
            })
            continue

        es_partida = (cant is not None and cant != 0) or (pu is not None and pu != 0 and cC is None)
        if not es_partida:
            # Capítulo: código entero o letra; o texto sin cantidad
            if re.fullmatch(r"[A-Z]{1,2}", cod or "") and not (cant or pu):
                grupo = f"{cod}. {desc}"
                capitulo = grupo
            else:
                capitulo = (f"{cod} {desc}".strip() if cod else desc).rstrip(":. ").strip()
            continue

        if pu is None and tot is not None and cant:
            pu = tot / cant
        if pu is None:
            pu = 0.0
        if cT is not None and tot is None and pu:
            sin_total.append(cod or desc[:30])
            monto_sin_total += (cant or 0) * pu
        partidas.append({
            "fila": r + 1,
            "codigo": cod or None,
            "capitulo": capitulo,
            "descripcion": desc,
            "unidad": _texto(_celda(f, cU)) or None,
            "cantidad": round(cant or 0, 4),
            "precio_unitario": round(pu, 2),
            "costo_unitario_apu": round(_num(_celda(f, cX)), 2) if cX is not None and _num(_celda(f, cX)) else None,
        })

    total_partidas = round(sum(p["cantidad"] * p["precio_unitario"] for p in partidas), 2)
    total_ind = round(sum(p["precio_unitario"] for p in indirectos), 2)
    costo_directo = round(sum(p["cantidad"] * (p["costo_unitario_apu"] or 0) for p in partidas), 2)

    if not partidas:
        avisos.append("No se reconoció ninguna partida con cantidad. Revisa las columnas.")
    if sin_total:
        avisos.append(f"{len(sin_total)} partida(s) tienen cantidad y P.U. pero el Excel no les calcula importe "
                      f"({', '.join(sin_total[:5])}). Las incluí; desmárcalas si no van.")
    if subtotal_excel:
        dif = total_partidas - monto_sin_total - subtotal_excel
        tol = max(50.0, abs(subtotal_excel) * 0.0005)      # redondeo de P.U. a 2 decimales
        if abs(dif) > tol:
            avisos.append(f"El subtotal del Excel es {subtotal_excel:,.2f} y las partidas leídas suman "
                          f"{total_partidas - monto_sin_total:,.2f} (diferencia {dif:,.2f}). Revisa que no falte ninguna.")
        elif abs(dif) > 1:
            avisos.append(f"Diferencia de {dif:,.2f} con el subtotal del Excel por redondear los P.U. a 2 decimales.")

    return {
        "hojas": [h[0] for h in hojas],
        "hoja": nom,
        "fila_encabezado": fila_enc + 1,
        "columnas": cols,
        "encabezados": encabezados,
        "partidas": partidas,
        "indirectos": indirectos,
        "resumen": {
            "partidas": len(partidas),
            "capitulos": len({p["capitulo"] for p in partidas}),
            "total_directo": total_partidas,
            "total_indirectos": total_ind,
            "total_general": round(total_partidas + total_ind, 2),
            "costo_directo": costo_directo if costo_directo else None,
            "subtotal_excel": subtotal_excel,
            "total_excel": total_excel,
        },
        "avisos": avisos,
    }


# ══════════════════════════════════════════════════════════════
# 2. RELACIÓN DE GASTOS EN EXCEL
# ══════════════════════════════════════════════════════════════

_K_GAS = {
    "fecha":       [r"FECHA"],
    "proveedor":   [r"PROVEEDOR", r"SUPLIDOR", r"BENEFICIARIO", r"RAZON SOCIAL", r"^NOMBRE", r"PAGADO A", r"EMPRESA"],
    "rnc":         [r"^RNC", r"CEDULA", r"RNC ?/ ?CED"],
    "documento":   [r"NCF", r"FACTURA", r"^NO\.? ?DOC", r"DOCUMENTO", r"CONDUCE", r"RECIBO", r"COMPROBANTE", r"^REF"],
    "descripcion": [r"CONCEPTO", r"DESCRIP", r"DETALLE", r"ARTICULO", r"MATERIAL", r"^GASTO$"],
    "subtotal":    [r"SUB ?-?TOTAL", r"MONTO SIN", r"^BASE", r"^MONTO$", r"^IMPORTE$", r"^VALOR$", r"MONTO FACTURADO"],
    "itbis":       [r"ITBIS", r"IMPUESTO", r"^IVA"],
    "total":       [r"^TOTAL", r"MONTO TOTAL", r"TOTAL PAGADO", r"^PAGADO$", r"^NETO"],
    "partida":     [r"PARTIDA", r"^COD\.? ?PART", r"^ITEM"],
    "tipo":        [r"^TIPO", r"CATEGORIA", r"RUBRO"],
}


def _detectar_encabezado_gas(filas):
    mejor = (None, 0, {})
    for i, fila in enumerate(filas[:40]):
        cols = {}
        for j, c in enumerate(_norm(x) for x in fila):
            if not c or len(c) > 40:
                continue
            for campo, pats in _K_GAS.items():
                if campo in cols:
                    continue
                if campo == "total" and _match(c, _K_GAS["subtotal"]):
                    continue
                if campo == "subtotal" and _match(c, _K_GAS["itbis"]):
                    continue
                if _match(c, pats):
                    cols[campo] = j
                    break
        puntaje = sum(k in cols for k in ("fecha", "descripcion", "proveedor", "documento")) + 2 * any(k in cols for k in ("subtotal", "total"))
        if puntaje > mejor[1] and any(k in cols for k in ("subtotal", "total")):
            mejor = (i, puntaje, cols)
    return mejor


_TIPOS_PALABRAS = {
    "mano_obra":   ["MANO DE OBRA", "AJUSTE", "AJUSTERO", "NOMINA", "JORNAL", "MAESTRO", "OBRERO", "PEON", "ALBANIL", "CARPINTERO", "VARILLERO", "PLOMERO", "ELECTRICISTA", "PINTOR", "CUADRILLA"],
    "equipo":      ["ALQUILER", "RETRO", "EXCAVADORA", "COMPACTADOR", "MEZCLADORA", "VIBRADOR", "GRUA", "MOTONIVELADORA", "RODILLO", "ANDAMIO", "EQUIPO"],
    "transporte":  ["TRANSPORTE", "FLETE", "CAMION", "BOTE", "ACARREO", "COMBUSTIBLE", "GASOIL", "GASOLINA", "PEAJE"],
    "subcontrato": ["SUBCONTRAT", "CONTRATISTA"],
    "indirecto":   ["SEGURO", "FIANZA", "POLIZA", "OFICINA", "PAPELERIA", "TELEFONO", "INTERNET", "VIATICO", "COMIDA", "ALMUERZO", "DIRECCION TECNICA", "HONORARIO"],
}


def tipo_por_texto(texto: str) -> str:
    t = _norm(texto)
    for tipo, pals in _TIPOS_PALABRAS.items():
        if any(p in t for p in pals):
            return tipo
    return "material"


def leer_gastos_excel(contenido: bytes, nombre: str = "", hoja: str | None = None, mapeo: dict | None = None) -> dict:
    hojas = _abrir(contenido, nombre)
    if not hojas:
        raise ValueError("El archivo no tiene hojas visibles")
    cand = []
    for nom, filas, fmts in hojas:
        i, p, cols = _detectar_encabezado_gas(filas)
        cand.append((nom, p, i, cols, filas))
    if hoja:
        sel = next((c for c in cand if c[0] == hoja), None)
        if not sel:
            raise ValueError(f"No existe la hoja '{hoja}'")
    else:
        sel = max(cand, key=lambda c: c[1])
    nom, puntaje, fila_enc, cols, filas = sel
    avisos = []
    if fila_enc is None:
        fila_enc, cols = 0, {}
        avisos.append("No encontré la fila de encabezados; indica las columnas a mano.")
    if mapeo:
        if mapeo.get("fila_encabezado"):
            fila_enc = int(mapeo["fila_encabezado"]) - 1
        for k, v in mapeo.items():
            if k == "fila_encabezado":
                continue
            if v is None or v == "":
                cols.pop(k, None)
            else:
                cols[k] = int(v)

    encabezados = []
    if 0 <= fila_enc < len(filas):
        ancho = max((len(f) for f in filas[fila_enc:fila_enc + 50]), default=0)
        encabezados = [{"col": j, "letra": _letra(j), "texto": _texto(_celda(filas[fila_enc], j))} for j in range(ancho)]

    g = lambda f, k: _celda(f, cols.get(k))
    gastos = []
    for r in range(fila_enc + 1, len(filas)):
        f = filas[r]
        if not any(x not in (None, "") for x in f):
            continue
        desc = _texto(g(f, "descripcion"))
        prov = _texto(g(f, "proveedor"))
        sub, itb, tot = _num(g(f, "subtotal")), _num(g(f, "itbis")), _num(g(f, "total"))
        if _norm(desc).startswith("TOTAL") or _norm(prov).startswith("TOTAL") or _norm(_texto(_celda(f, 0))).startswith("TOTAL"):
            continue
        itb = itb or 0.0
        if sub is None and tot is not None:
            sub = tot - itb
        if not sub:
            continue
        gastos.append({
            "fila": r + 1,
            "fecha": _fecha(g(f, "fecha")),
            "proveedor": prov or None,
            "rnc": _texto(g(f, "rnc")) or None,
            "documento": _texto(g(f, "documento")) or None,
            "descripcion": desc or prov or "(sin concepto)",
            "monto": round(sub, 2),
            "itbis": round(itb, 2),
            "partida_codigo": _codigo(g(f, "partida")) or None,
            "tipo": tipo_por_texto(_texto(g(f, "tipo")) + " " + desc + " " + prov),
        })
    if not gastos:
        avisos.append("No se reconoció ningún gasto con monto. Revisa las columnas.")
    return {
        "hojas": [h[0] for h in hojas],
        "hoja": nom,
        "fila_encabezado": fila_enc + 1,
        "columnas": cols,
        "encabezados": encabezados,
        "gastos": gastos,
        "resumen": {"gastos": len(gastos), "monto": round(sum(x["monto"] for x in gastos), 2),
                    "itbis": round(sum(x["itbis"] for x in gastos), 2)},
        "avisos": avisos,
    }


# ══════════════════════════════════════════════════════════════
# 3. SUGERENCIA DE PARTIDA POR PALABRAS
# ══════════════════════════════════════════════════════════════

_STOP = set("""DE DEL LA LAS LOS EL EN Y CON PARA POR A AL O U UN UNA SUM SUMINISTRO COLOCACION INSTALACION
SUMINISTRO E INCLUYE INCL SEGUN CONSIDERAR TIPO MATERIAL MATERIALES TRABAJO TRABAJOS ML M2 M3 UD UDS PA GL
DE: SUM. COLOC. COLOC Y/O""".split())

# Sinónimos de compras -> palabras que aparecen en las partidas
_SINON = {
    "CEMENTO": ["HORMIGON", "MORTERO", "PANETE", "FINO", "TORTA", "BLOQUE"],
    "ARENA": ["HORMIGON", "MORTERO", "PANETE", "BLOQUE"],
    "GRAVA": ["HORMIGON"], "PIEDRA": ["HORMIGON"],
    "VARILLA": ["ACERO", "HORMIGON", "COLUMNA", "VIGA", "ZAPATA", "LOSA"],
    "ALAMBRE": ["ACERO", "HORMIGON"],
    "BLOCK": ["BLOQUE", "MURO"], "BLOCKS": ["BLOQUE", "MURO"], "BLOQUES": ["BLOQUE", "MURO"],
    "TUBO": ["TUBERIA"], "TUBOS": ["TUBERIA"], "CODO": ["ACCESORIOS", "TUBERIA"], "TEE": ["ACCESORIOS"],
    "CERAMICA": ["CERAMICA", "REVESTIMIENTO", "PISO"], "PORCELANATO": ["PORCELANATO", "PISO", "REVESTIMIENTO"],
    "PINTURA": ["PINTURA"], "SELLADOR": ["PINTURA"],
    "INODORO": ["INODORO"], "LAVAMANOS": ["LAVAMANOS"],
    "PUERTA": ["PUERTA"], "CABLE": ["ALAMBRADO", "CABLE", "ELECTRIC"], "BREAKER": ["PANEL", "BREAKER", "ELECTRIC"],
    "VOLTEO": ["BOTE"], "CAMION": ["BOTE"], "BOTE": ["BOTE"],
    "EPOXICO": ["EPOXICO"], "IMPERMEABILIZANTE": ["IMPERMEABILIZANTE", "IMPERMEABILIZACION"],
    "YESO": ["PLAFON", "YESO"], "SHEETROCK": ["PLAFON", "SHEETROCK", "MURO"], "PLAFON": ["PLAFON"],
}


def _tokens(s: str) -> list[str]:
    t = re.findall(r"[A-Z0-9Ø/\"]+", _norm(s))
    return [x for x in t if len(x) > 2 and x not in _STOP and not x.isdigit()]


def sugerir_partidas(textos: list[str], partidas: list[dict], top: int = 3) -> list[list[dict]]:
    """Para cada texto devuelve hasta `top` partidas [{partida_id, codigo, descripcion, score}]."""
    idx = []
    df = {}
    for p in partidas:
        toks = set(_tokens(f"{p.get('descripcion', '')} {p.get('capitulo') or ''}"))
        idx.append((p, toks))
        for t in toks:
            df[t] = df.get(t, 0) + 1
    n = max(len(partidas), 1)
    salida = []
    for texto in textos:
        q = set(_tokens(texto))
        extra = set()
        for t in q:
            for k, vs in _SINON.items():
                if t.startswith(k[:6]):
                    extra.update(vs)
        res = []
        for p, toks in idx:
            s = 0.0
            for t in q | extra:
                peso = 1.0 if t in q else 0.5
                hit = t in toks or any(len(t) >= 5 and (x.startswith(t[:5])) for x in toks)
                if hit:
                    s += peso * (1.0 + (n / (1 + df.get(t, 1))) ** 0.5 / 3)
            if s > 0:
                res.append((s, p))
        res.sort(key=lambda x: -x[0])
        salida.append([{"partida_id": p["id"], "codigo": p.get("codigo"), "descripcion": p.get("descripcion"),
                        "score": round(s, 2)} for s, p in res[:top]])
    return salida
