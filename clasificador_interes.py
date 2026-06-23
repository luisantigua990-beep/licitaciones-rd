# -*- coding: utf-8 -*-
"""
clasificador_interes.py
=======================
Motor de clasificación de procesos por INTERÉS del usuario (modelo híbrido).

Dos formas de definir interés (se combinan con OR):
  1. CATEGORÍAS predefinidas  → match determinístico por UNSPSC + palabras clave.
                                 Instantáneo, sin IA. Se calcula UNA vez por proceso.
  2. TEXTO LIBRE              → match semántico con Gemini. Solo se evalúa cuando
                                 algún usuario tiene texto libre y el proceso no
                                 matcheó ya por categoría. UNA llamada por proceso.

Regla de oro:
  - Usuario SIN interés (ni categorías ni texto)  → recibe TODO.
  - Usuario CON interés                            → solo lo que matchea.

El scraper de 3 minutos ya guarda en articulos_proceso:
  familia_unspsc (4 díg + 0000), clase_unspsc (6 díg), subclase_unspsc (8 díg).
Aquí matcheamos por PREFIJO de 2 díg (segmento) y/o 4-6 díg para mayor precisión.
"""

import re
import unicodedata

# ──────────────────────────────────────────────────────────────────────────
#  CATÁLOGO DE CATEGORÍAS
#  Cada categoría amigable apunta a:
#    - "unspsc": lista de PREFIJOS UNSPSC (2, 4 o 6 dígitos). Match por prefijo.
#    - "keywords": palabras/raíces que, si aparecen en el título, también matchean.
#  El match de la categoría = (UNSPSC coincide)  OR  (alguna keyword aparece).
#  Ajusta libremente: agrega/quita categorías, prefijos o palabras.
# ──────────────────────────────────────────────────────────────────────────
CATEGORIAS = {
    "salud_farmacia": {
        "nombre": "Salud, farmacia y laboratorio",
        "unspsc": ["51", "42", "41", "85"],
        "keywords": [
            "medicamento", "farmaco", "farmaceutic", "reactivo", "insumo medico",
            "jeringa", "suero", "antibiotic", "hospital", "clinic", "laboratorio",
            "hemocultivo", "vacuna", "gasa", "guante quirurgic", "sonda", "cateter",
            "material gastable medico", "equipo medico", "odontolog",
        ],
    },
    "construccion_obras": {
        "nombre": "Construcción y obras",
        "unspsc": ["30", "72", "95", "22"],
        "keywords": [
            "construccion", "remodelacion", "reparacion", "rehabilitacion",
            "edificacion", "asfalto", "bacheo", "acueducto", "alcantarillado",
            "cemento", "varilla", "hormigon", "obra", "terminacion", "ampliacion",
            "pavimento", "drenaje", "muro", "puente", "carretera",
        ],
    },
    "vehiculos_taller": {
        "nombre": "Vehículos, talleres y lubricantes",
        "unspsc": ["25", "15", "7818"],
        "keywords": [
            "vehiculo", "camion", "neumatic", "goma", "lubricante", "aceite",
            "repuesto", "taller", "mecanic", "bateria", "mantenimiento de vehicul",
            "motor", "filtro", "freno", "llanta", "automotriz",
        ],
    },
    "combustibles": {
        "nombre": "Combustibles",
        "unspsc": ["1510", "1511", "1512"],
        "keywords": ["combustible", "gasoil", "gas oil", "gasolina", "diesel", "glp", "propano"],
    },
    "tecnologia": {
        "nombre": "Tecnología e informática",
        "unspsc": ["43", "32", "8111"],
        "keywords": [
            "computadora", "laptop", "servidor", "software", "licencia", "impresora",
            "red de datos", "switch", "cableado estructurado", "sistema informatic",
            "hosting", "ciberseguridad", "data center", "tableta", "monitor",
            "ups", "scanner", "escaner",
        ],
    },
    "alimentos": {
        "nombre": "Alimentos y catering",
        "unspsc": ["50", "9010"],
        "keywords": [
            "alimento", "racion", "comida", "catering", "almuerzo", "desayuno",
            "vivere", "bebida", "agua", "refrigerio", "merienda",
        ],
    },
    "mobiliario": {
        "nombre": "Mobiliario y equipos de oficina",
        "unspsc": ["56", "44"],
        "keywords": [
            "mueble", "escritorio", "silla", "archivero", "mobiliario",
            "estanteria", "butaca", "gabinete", "anaquel",
        ],
    },
    "uniformes_textil": {
        "nombre": "Uniformes y textiles",
        "unspsc": ["53"],
        "keywords": [
            "uniforme", "ropa", "calzado", "zapato", "tela", "textil",
            "camiseta", "chaleco", "bata", "vestimenta",
        ],
    },
    "limpieza": {
        "nombre": "Limpieza e higiene",
        "unspsc": ["47", "76", "1210"],
        "keywords": [
            "limpieza", "desinfectante", "detergente", "jabon", "papel higienic",
            "fumigacion", "recogida de basura", "aseo", "higiene", "cloro",
        ],
    },
    "papeleria": {
        "nombre": "Papelería y útiles de oficina",
        "unspsc": ["14", "4412", "55"],
        "keywords": [
            "papeleria", "utiles de oficina", "toner", "tinta", "boligrafo",
            "papel bond", "carpeta", "impresion", "fotocopia", "resma",
        ],
    },
    "servicios_profesionales": {
        "nombre": "Servicios profesionales y consultoría",
        "unspsc": ["80", "81", "84", "86"],
        "keywords": [
            "consultoria", "asesoria", "auditoria", "capacitacion", "estudio",
            "diseño", "supervision de obra", "servicio profesional", "honorario",
            "levantamiento", "diagnostico",
        ],
    },
    "seguridad": {
        "nombre": "Seguridad y vigilancia",
        "unspsc": ["46", "92"],
        "keywords": [
            "seguridad", "vigilancia", "camara", "cctv", "alarma", "guardian",
            "monitoreo", "control de acceso",
        ],
    },
    "energia_electricidad": {
        "nombre": "Energía y electricidad",
        "unspsc": ["26", "39", "40"],
        "keywords": [
            "electric", "transformador", "planta electric", "luminaria",
            "aire acondicionado", "generador", "inversor", "panel solar",
            "fotovoltaic", "iluminacion", "cableado electric",
        ],
    },
}


# ──────────────────────────────────────────────────────────────────────────
#  Utilidades de normalización
# ──────────────────────────────────────────────────────────────────────────
def _normalizar(texto: str) -> str:
    """minúsculas + sin tildes, para comparar sin sorpresas."""
    if not texto:
        return ""
    t = unicodedata.normalize("NFKD", str(texto))
    t = "".join(c for c in t if not unicodedata.combining(c))
    return t.lower().strip()


def _familias_del_proceso(articulos: list) -> set:
    """Junta todos los códigos UNSPSC del proceso (familia/clase/subclase)."""
    codigos = set()
    for a in (articulos or []):
        for campo in ("subclase_unspsc", "clase_unspsc", "familia_unspsc"):
            v = a.get(campo)
            if v:
                codigos.add(str(v).strip())
    return codigos


def _matchea_unspsc(codigos_proceso: set, prefijos_categoria: list) -> bool:
    """True si algún código del proceso empieza con algún prefijo de la categoría."""
    for codigo in codigos_proceso:
        for pref in prefijos_categoria:
            if codigo.startswith(pref):
                return True
    return False


# ──────────────────────────────────────────────────────────────────────────
#  PASO 1 — Clasificar el PROCESO en categorías (determinístico, 1 vez por proceso)
# ──────────────────────────────────────────────────────────────────────────
def clasificar_proceso(proceso: dict, articulos: list) -> set:
    """
    Devuelve el conjunto de claves de categoría en las que cae el proceso.
    Ej: {"salud_farmacia"} o {"vehiculos_taller", "combustibles"}.
    Cachéalo en el proceso para no recalcular por usuario.
    """
    codigos = _familias_del_proceso(articulos)
    texto = _normalizar(
        f"{proceso.get('titulo','')} {proceso.get('objeto_proceso','')} "
        f"{proceso.get('descripcion','')}"
    )

    encontradas = set()
    for clave, cat in CATEGORIAS.items():
        # a) por código UNSPSC
        if codigos and _matchea_unspsc(codigos, cat["unspsc"]):
            encontradas.add(clave)
            continue
        # b) por palabra clave en el título/objeto
        for kw in cat["keywords"]:
            if _normalizar(kw) in texto:
                encontradas.add(clave)
                break
    return encontradas


# ──────────────────────────────────────────────────────────────────────────
#  PASO 2 — Texto libre con IA (opcional, 1 llamada por proceso)
#  El integrador conecta su cliente Gemini en `_llamar_gemini`.
# ──────────────────────────────────────────────────────────────────────────
def evaluar_texto_libre(proceso: dict, intereses_libres: list, _llamar_gemini) -> set:
    """
    Recibe la lista de textos-libres DISTINTOS de los usuarios y devuelve el
    subconjunto que matchea este proceso, en UNA sola llamada a Gemini.

    `_llamar_gemini(prompt)` debe devolver el texto crudo de la respuesta.
    Pásale tu wrapper de Gemini ya configurado (modelo, api key, etc.).
    """
    intereses_libres = [i for i in (intereses_libres or []) if i and i.strip()]
    if not intereses_libres:
        return set()

    titulo = proceso.get("titulo", "")
    objeto = proceso.get("objeto_proceso", "")
    lista = "\n".join(f'- "{x.strip()}"' for x in set(intereses_libres))

    prompt = f"""Eres un clasificador de licitaciones públicas dominicanas.
Proceso de compra:
  Título: {titulo}
  Tipo:   {objeto}

Lista de intereses de usuarios:
{lista}

Devuelve SOLO un JSON array con los intereses (texto exacto) que SÍ corresponden
a este proceso. Si ninguno corresponde, devuelve []. Sin explicaciones, sin markdown.
Ejemplo de salida: ["talleres y lubricantes"]"""

    try:
        raw = _llamar_gemini(prompt) or ""
        raw = raw.strip().replace("```json", "").replace("```", "").strip()
        import json
        matched = json.loads(raw) if raw else []
        return {str(m).strip() for m in matched if str(m).strip()}
    except Exception as e:
        print(f"⚠️ evaluar_texto_libre: fallo IA ({e}) — se omite texto libre este ciclo")
        return set()


# ──────────────────────────────────────────────────────────────────────────
#  PASO 3 — ¿Le mando este proceso a ESTE usuario?
# ──────────────────────────────────────────────────────────────────────────
def usuario_interesado(
    usuario_categorias: list,
    usuario_texto_libre: str,
    proceso_categorias: set,
    libres_que_matchearon: set,
) -> bool:
    """
    Decisión final por usuario (rápida, sin IA: todo se precalculó arriba).

    - Sin categorías Y sin texto libre  → True (quiere TODO).
    - Con interés                       → True solo si cae en su categoría
                                          o su texto libre matcheó este proceso.
    """
    cats = set(usuario_categorias or [])
    libre = (usuario_texto_libre or "").strip()

    # Sin ningún interés definido → recibe todo
    if not cats and not libre:
        return True

    # Match por categoría
    if cats & proceso_categorias:
        return True

    # Match por texto libre
    if libre and libre in libres_que_matchearon:
        return True

    return False


# ──────────────────────────────────────────────────────────────────────────
#  Helper de conveniencia para el scraper: filtra una lista de usuarios
# ──────────────────────────────────────────────────────────────────────────
def filtrar_usuarios_para_proceso(proceso, articulos, usuarios, _llamar_gemini=None):
    """
    usuarios: lista de dicts con al menos:
        {"user_id", "email", "categorias": [...], "texto_libre": "..."}
    Devuelve la sublista de usuarios a los que SÍ les corresponde el proceso.

    Hace: clasifica el proceso 1 vez, evalúa texto libre 1 vez (si hay y si se
    pasó _llamar_gemini), y luego decide por usuario sin más llamadas.
    """
    proceso_cats = clasificar_proceso(proceso, articulos)

    # Recolectar textos-libres distintos de usuarios con interés libre
    libres = {u.get("texto_libre", "").strip() for u in usuarios if u.get("texto_libre")}
    libres_match = set()
    if libres and _llamar_gemini is not None:
        libres_match = evaluar_texto_libre(proceso, list(libres), _llamar_gemini)

    seleccionados = []
    for u in usuarios:
        if usuario_interesado(
            u.get("categorias"), u.get("texto_libre"), proceso_cats, libres_match
        ):
            seleccionados.append(u)
    return seleccionados
