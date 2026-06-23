# -*- coding: utf-8 -*-
"""
notificador_interes_profundo.py
===============================
Puente entre el clasificador de interés y el análisis profundo automático.

QUÉ HACE
--------
Cuando el scraper detecta un proceso nuevo, este módulo:
  1. Busca usuarios que DEFINIERON un interés (categorías o texto libre) y tienen
     alertas_email activas.
  2. Clasifica el proceso (UNSPSC + keywords) y filtra quién lo quiere.
  3. Para cada interesado, lo inserta en `seguimiento_procesos` (auto-seguimiento).
  4. Dispara UNA vez el endpoint /api/procesos/{codigo}/analizar.

A partir de ahí, TU código existente hace el resto sin cambios:
  - El endpoint genera el análisis profundo y lo cachea en `analisis_pliego`
    (queda disponible por demanda).
  - enviar_email_analisis() manda el análisis por correo a los de
    seguimiento_procesos (con su guard anti-duplicado intacto).

REGLA DE ORO (respetada aquí)
-----------------------------
  - Usuario SIN interés definido  → este módulo lo ignora. Sigue recibiendo sus
    notificaciones push/email normales como hasta ahora. NO se le genera análisis
    profundo de todo (eso quemaría Gemini y sería spam).
  - Usuario CON interés que matchea → recibe el análisis profundo por email.

SEGURIDAD
---------
  - Todo va dentro de try/except: si algo falla aquí, el scraper sigue su curso
    normal. Este módulo NUNCA debe tumbar el ciclo de notificaciones.
  - No modifica ninguna función existente. Solo INSERTA filas y hace un POST.
"""

import os
import requests

try:
    from clasificador_interes import filtrar_usuarios_para_proceso
except Exception as _e:
    filtrar_usuarios_para_proceso = None
    print(f"⚠️ [interes_profundo] No se pudo importar clasificador_interes: {_e}")

# URL interna del propio servicio (scraper y API corren en el mismo contenedor).
# Se puede override con env var si hiciera falta.
BASE_URL_INTERNA = os.getenv("BASE_URL_INTERNA", "http://localhost:8080")


def _traer_usuarios_con_interes(supabase_admin):
    """
    Devuelve la lista de usuarios que DEFINIERON interés y tienen email activo.
    Cada item: {"user_id", "categorias", "texto_libre"}.
    Si nadie tiene interés definido, devuelve [].
    """
    try:
        resp = supabase_admin.table("perfiles_empresa") \
            .select("user_id, categorias_interes, texto_libre_interes, alertas_email") \
            .eq("alertas_email", True) \
            .execute()
    except Exception as e:
        print(f"⚠️ [interes_profundo] No se pudieron leer perfiles: {e}")
        return []

    usuarios = []
    for p in (resp.data or []):
        cats = p.get("categorias_interes") or []
        libre = (p.get("texto_libre_interes") or "").strip()
        # Solo nos interesan los que DEFINIERON algo (regla de oro)
        if not cats and not libre:
            continue
        if not p.get("user_id"):
            continue
        usuarios.append({
            "user_id": p["user_id"],
            "categorias": cats,
            "texto_libre": libre,
        })
    return usuarios


def _ya_sigue(supabase_admin, user_id, codigo):
    try:
        r = supabase_admin.table("seguimiento_procesos") \
            .select("id") \
            .eq("user_id", user_id) \
            .eq("proceso_codigo", codigo) \
            .limit(1).execute()
        return bool(r.data)
    except Exception:
        return False


def _auto_seguir(supabase_admin, user_id, codigo):
    """Inserta el auto-seguimiento si no existe. Silencioso ante errores."""
    try:
        if _ya_sigue(supabase_admin, user_id, codigo):
            return False
        supabase_admin.table("seguimiento_procesos").insert({
            "user_id": user_id,
            "proceso_codigo": codigo,
            "estado": "analizando",
        }).execute()
        return True
    except Exception as e:
        print(f"⚠️ [interes_profundo] No se pudo auto-seguir {codigo} para {user_id}: {e}")
        return False


def _disparar_analisis(codigo):
    """Llama al endpoint interno que genera análisis + manda email a seguidores."""
    try:
        url = f"{BASE_URL_INTERNA}/api/procesos/{codigo}/analizar"
        resp = requests.post(url, timeout=15)
        if resp.status_code in (200, 201):
            print(f"   🧠 [interes_profundo] Análisis profundo disparado para {codigo}")
            return True
        print(f"   ⚠️ [interes_profundo] Endpoint análisis devolvió {resp.status_code} para {codigo}: {resp.text[:120]}")
        return False
    except Exception as e:
        print(f"   ⚠️ [interes_profundo] No se pudo disparar análisis para {codigo}: {e}")
        return False


def procesar_interes_profundo(proceso, articulos, supabase_admin, llamar_gemini=None):
    """
    Punto de entrada. Llamar desde el scraper para CADA proceso nuevo,
    justo después de notificar_proceso_inmediato.

    Parámetros:
      proceso        : dict del proceso (titulo, objeto_proceso, codigo_proceso, ...)
      articulos      : lista de artículos con familia_unspsc/clase/subclase
      supabase_admin : cliente Supabase con service_role (el `supabase` del scraper sirve)
      llamar_gemini  : (opcional) wrapper Gemini para evaluar texto libre.
                       Si es None, solo se usa el match por CATEGORÍAS (sin IA),
                       que cubre la mayoría. El texto libre se ignora hasta
                       conectar Gemini.

    Devuelve: cantidad de usuarios a los que se les disparó análisis (int).
    No lanza excepciones hacia afuera: ante cualquier fallo, devuelve 0.
    """
    if filtrar_usuarios_para_proceso is None:
        return 0

    try:
        codigo = proceso.get("codigo_proceso")
        if not codigo:
            return 0

        usuarios = _traer_usuarios_con_interes(supabase_admin)
        if not usuarios:
            return 0  # Nadie con interés definido → nada que hacer

        interesados = filtrar_usuarios_para_proceso(
            proceso, articulos, usuarios, _llamar_gemini=llamar_gemini
        )
        if not interesados:
            return 0  # Este proceso no le interesa a ningún usuario con filtro

        # Auto-seguir a cada interesado (para que el email les llegue)
        nuevos_seguidores = 0
        for u in interesados:
            if _auto_seguir(supabase_admin, u["user_id"], codigo):
                nuevos_seguidores += 1

        # Disparar el análisis UNA vez (genera + cachea + email a seguidores).
        # Se dispara aunque ya hubiera seguidores previos: el endpoint maneja
        # cache-hit y el email tiene guard anti-duplicado.
        _disparar_analisis(codigo)

        print(f"   📨 [interes_profundo] {codigo}: {len(interesados)} interesado(s), "
              f"{nuevos_seguidores} nuevo(s) seguidor(es) por interés")
        return len(interesados)

    except Exception as e:
        # Nunca tumbar el scraper por un fallo aquí
        print(f"⚠️ [interes_profundo] Error no fatal procesando interés: {e}")
        return 0
