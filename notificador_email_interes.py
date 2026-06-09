"""
Notificador de Emails por Interés — LicitacionLab
==================================================
Cuando el monitor detecta procesos nuevos:
  1. Busca usuarios cuyo perfil (perfiles_empresa) matchea el proceso
     (provincias, rubros/categorías UNSPSC, rango de montos, alertas_email).
  2. Si hay al menos UN interesado → genera el resumen con Gemini UNA SOLA VEZ
     y lo guarda en `resumenes_email` (caché). Si ya existe, lo reusa.
  3. Envía el email (Resend) con el resumen a TODOS los interesados.

Regla de oro: cero llamadas a Gemini si nadie matchea, y nunca más de
una llamada por proceso aunque haya 100 interesados.
"""

import os
import time
import requests
from datetime import datetime
from dotenv import load_dotenv
from supabase import create_client

load_dotenv()

SUPABASE_URL         = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_KEY = os.getenv("SUPABASE_SERVICE_KEY", os.getenv("SUPABASE_KEY"))
GEMINI_API_KEY       = os.getenv("GEMINI_API_KEY", "")
RESEND_API_KEY       = os.getenv("RESEND_API_KEY", "")
FROM_EMAIL           = os.getenv("RESEND_FROM", "LicitacionLab <notificaciones@licitacionlab.com>")
APP_URL              = os.getenv("APP_URL", "https://app.licitacionlab.com")

supabase_admin = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)


# ─────────────────────────────────────────────────────────────
# 1. MATCHING: ¿a quién le interesa este proceso?
# ─────────────────────────────────────────────────────────────

def _normalizar_codigos_unspsc(categorias_unspsc) -> set:
    """Extrae códigos UNSPSC del jsonb del perfil (tolera lista de strings,
    lista de dicts {codigo,...}, o dicts {codigo: nombre})."""
    codigos = set()
    if not categorias_unspsc:
        return codigos
    try:
        items = categorias_unspsc if isinstance(categorias_unspsc, list) else list(categorias_unspsc.keys())
        for it in items:
            if isinstance(it, dict):
                c = str(it.get("codigo") or it.get("code") or it.get("id") or "")
            else:
                c = str(it)
            c = "".join(ch for ch in c if ch.isdigit())
            if len(c) >= 4:
                codigos.add(c[:4])  # familia UNSPSC (4 dígitos)
    except Exception:
        pass
    return codigos


def _obtener_unspsc_proceso(codigo_proceso: str) -> set:
    """Familias UNSPSC (4 dígitos) de los artículos del proceso."""
    try:
        r = supabase_admin.table("articulos_proceso") \
            .select("codigo_unspsc") \
            .eq("codigo_proceso", codigo_proceso).limit(50).execute()
        fams = set()
        for a in (r.data or []):
            c = "".join(ch for ch in str(a.get("codigo_unspsc") or "") if ch.isdigit())
            if len(c) >= 4:
                fams.add(c[:4])
        return fams
    except Exception:
        return set()


def _email_de_usuario(user_id: str) -> str | None:
    """Email del usuario desde auth.users (vía RPC de PostgREST no aplica;
    usamos la tabla auth con service key)."""
    try:
        r = supabase_admin.auth.admin.get_user_by_id(user_id)
        return getattr(r.user, "email", None)
    except Exception:
        return None


def encontrar_interesados(proceso: dict) -> list[dict]:
    """Devuelve [{email, razon_social, user_id}] de usuarios que matchean."""
    try:
        perfiles = supabase_admin.table("perfiles_empresa") \
            .select("user_id, razon_social, email_empresa, provincias, rubros, categorias_unspsc, "
                    "monto_minimo, monto_maximo, umbral_monto_min, umbral_monto_max, alertas_email") \
            .eq("onboarding_completado", True).execute().data or []
    except Exception as e:
        print(f"   ⚠️ Error leyendo perfiles: {e}")
        return []

    if not perfiles:
        return []

    provincia_proc = (proceso.get("provincia") or "").strip().lower()
    monto          = proceso.get("monto_estimado")
    try:
        monto = float(monto) if monto is not None else None
    except (TypeError, ValueError):
        monto = None

    fams_proceso = None  # lazy: solo consultar artículos si algún perfil tiene categorías
    interesados = []

    for p in perfiles:
        if p.get("alertas_email") is False:
            continue  # el usuario apagó los emails

        # ── Provincia ──
        provs = [str(x).strip().lower() for x in (p.get("provincias") or [])]
        if provs and provincia_proc and provincia_proc not in provs:
            continue

        # ── Monto ──
        mmin = p.get("umbral_monto_min") or p.get("monto_minimo")
        mmax = p.get("umbral_monto_max") or p.get("monto_maximo")
        if monto is not None:
            if mmin and monto < float(mmin):
                continue
            if mmax and monto > float(mmax):
                continue

        # ── Categorías UNSPSC (familia, 4 dígitos) ──
        fams_perfil = _normalizar_codigos_unspsc(p.get("categorias_unspsc"))
        if fams_perfil:
            if fams_proceso is None:
                fams_proceso = _obtener_unspsc_proceso(proceso["codigo_proceso"])
            if fams_proceso and not (fams_perfil & fams_proceso):
                continue
            # Si el proceso no tiene artículos aún, no descartamos por UNSPSC

        email = (p.get("email_empresa") or "").strip() or _email_de_usuario(p["user_id"])
        if not email:
            continue

        interesados.append({
            "email":        email,
            "razon_social": p.get("razon_social") or "",
            "user_id":      p["user_id"],
        })

    # Dedupe por email
    vistos, unicos = set(), []
    for i in interesados:
        if i["email"].lower() not in vistos:
            vistos.add(i["email"].lower())
            unicos.append(i)
    return unicos


# ─────────────────────────────────────────────────────────────
# 2. RESUMEN GEMINI CON CACHÉ (1 análisis por proceso, siempre)
# ─────────────────────────────────────────────────────────────

def obtener_o_generar_resumen(proceso: dict) -> str:
    codigo = proceso["codigo_proceso"]

    # ¿Ya está cacheado?
    try:
        r = supabase_admin.table("resumenes_email") \
            .select("resumen").eq("codigo_proceso", codigo).execute()
        if r.data and r.data[0].get("resumen"):
            print(f"   ♻️  Resumen cacheado para {codigo} — sin gasto de crédito")
            return r.data[0]["resumen"]
    except Exception:
        pass

    resumen = _generar_resumen_gemini(proceso)

    # Guardar en caché (aunque falle Gemini guardamos el fallback para no reintentar)
    try:
        supabase_admin.table("resumenes_email").upsert({
            "codigo_proceso": codigo,
            "resumen": resumen,
            "generado_en": datetime.utcnow().isoformat(),
        }).execute()
    except Exception as e:
        print(f"   ⚠️ No se pudo cachear resumen de {codigo}: {e}")

    return resumen


def _generar_resumen_gemini(proceso: dict) -> str:
    fallback = (proceso.get("descripcion") or proceso.get("titulo") or "")[:400]
    if not GEMINI_API_KEY:
        return fallback
    try:
        from google import genai
        cliente = genai.Client(api_key=GEMINI_API_KEY)
        monto = proceso.get("monto_estimado")
        monto_txt = f"RD${float(monto):,.0f}" if monto else "No especificado"
        prompt = f"""Eres un consultor experto en licitaciones públicas dominicanas (Ley 47-25).
Resume este proceso de compra pública para un email de alerta a empresas interesadas.

Título: {proceso.get('titulo', '')}
Institución: {proceso.get('unidad_compra', '')}
Modalidad: {proceso.get('modalidad', '')}
Monto estimado: {monto_txt}
Provincia: {proceso.get('provincia', 'No especificada')}
Fecha límite presentación de ofertas: {proceso.get('fecha_fin_recepcion_ofertas', 'Por definir')}
Descripción: {(proceso.get('descripcion') or '')[:2000]}

Responde en español, máximo 120 palabras, con este formato exacto (sin markdown):

QUÉ SE LICITA: [1-2 oraciones claras]
PARA QUIÉN ES IDEAL: [tipo de empresa que debería participar]
DATO CLAVE: [lo más importante a saber: requisito, plazo corto, monto atractivo, etc.]
"""
        resp = cliente.models.generate_content(model="gemini-2.0-flash", contents=prompt)
        texto = (resp.text or "").strip()
        return texto if texto else fallback
    except Exception as e:
        print(f"   ⚠️ Gemini falló, usando fallback: {e}")
        return fallback


# ─────────────────────────────────────────────────────────────
# 3. ENVÍO (Resend) con retry ante rate limit
# ─────────────────────────────────────────────────────────────

def _resend_send(subject: str, html: str, to_email: str) -> bool:
    if not RESEND_API_KEY:
        print("   ⚠️ RESEND_API_KEY no configurada — email omitido")
        return False
    for intento in range(3):
        try:
            resp = requests.post(
                "https://api.resend.com/emails",
                headers={"Authorization": f"Bearer {RESEND_API_KEY}", "Content-Type": "application/json"},
                json={"from": FROM_EMAIL, "to": [to_email], "subject": subject, "html": html},
                timeout=20,
            )
            if resp.status_code in (200, 201):
                return True
            if resp.status_code == 429:
                espera = 2 ** (intento + 1)
                print(f"   ⚠️ Resend rate limit — esperando {espera}s")
                time.sleep(espera)
                continue
            print(f"   ⚠️ Resend error {resp.status_code}: {resp.text[:100]}")
            return False
        except Exception as e:
            print(f"   ⚠️ Resend error: {e}")
            time.sleep(2)
    return False


def _html_email(proceso: dict, resumen: str, razon_social: str) -> str:
    codigo = proceso["codigo_proceso"]
    monto  = proceso.get("monto_estimado")
    monto_txt = f"RD${float(monto):,.0f}" if monto else "No especificado"
    fecha_limite = proceso.get("fecha_fin_recepcion_ofertas") or "Por definir"
    resumen_html = "".join(
        f'<p style="margin:0 0 10px;color:#374151;font-size:14px;line-height:1.6;">{linea}</p>'
        for linea in resumen.split("\n") if linea.strip()
    )
    saludo = f"Hola {razon_social}," if razon_social else "Hola,"
    return f"""
<!DOCTYPE html><html><body style="margin:0;padding:0;background:#f4f7f5;font-family:Arial,Helvetica,sans-serif;">
<table width="100%" cellpadding="0" cellspacing="0" style="padding:24px 12px;"><tr><td align="center">
<table width="560" cellpadding="0" cellspacing="0" style="background:#ffffff;border-radius:12px;overflow:hidden;border:1px solid #e5e7eb;">
  <tr><td style="background:#16A34A;padding:18px 24px;">
    <span style="color:#ffffff;font-size:17px;font-weight:bold;">🎯 Nueva licitación de tu interés</span>
  </td></tr>
  <tr><td style="padding:24px;">
    <p style="margin:0 0 14px;color:#111827;font-size:14px;">{saludo}</p>
    <p style="margin:0 0 6px;color:#111827;font-size:16px;font-weight:bold;line-height:1.4;">{proceso.get('titulo','')}</p>
    <p style="margin:0 0 16px;color:#6b7280;font-size:13px;">{proceso.get('unidad_compra','')} · {codigo}</p>
    <table width="100%" cellpadding="0" cellspacing="0" style="background:#f0fdf4;border:1px solid #bbf7d0;border-radius:8px;margin-bottom:16px;"><tr>
      <td style="padding:12px 16px;"><span style="color:#6b7280;font-size:11px;text-transform:uppercase;">Monto estimado</span><br>
        <span style="color:#16A34A;font-size:16px;font-weight:bold;">{monto_txt}</span></td>
      <td style="padding:12px 16px;"><span style="color:#6b7280;font-size:11px;text-transform:uppercase;">Presentación de ofertas</span><br>
        <span style="color:#111827;font-size:14px;font-weight:bold;">{fecha_limite}</span></td>
    </tr></table>
    <div style="border-left:3px solid #16A34A;padding-left:14px;margin-bottom:20px;">
      <p style="margin:0 0 8px;color:#16A34A;font-size:12px;font-weight:bold;text-transform:uppercase;">Análisis LicitacionLab IA</p>
      {resumen_html}
    </div>
    <table cellpadding="0" cellspacing="0"><tr><td style="background:#16A34A;border-radius:8px;">
      <a href="{APP_URL}/?proceso={codigo}" style="display:inline-block;padding:12px 28px;color:#ffffff;font-size:14px;font-weight:bold;text-decoration:none;">Ver proceso completo →</a>
    </td></tr></table>
  </td></tr>
  <tr><td style="padding:16px 24px;background:#f9fafb;border-top:1px solid #e5e7eb;">
    <p style="margin:0;color:#9ca3af;font-size:11px;">Recibes este correo porque tu perfil en LicitacionLab coincide con este proceso.
    Ajusta tus preferencias en la app · <a href="{APP_URL}" style="color:#16A34A;">app.licitacionlab.com</a></p>
  </td></tr>
</table></td></tr></table></body></html>"""


# ─────────────────────────────────────────────────────────────
# 4. ORQUESTADOR — llamar desde monitor.py tras detectar nuevos
# ─────────────────────────────────────────────────────────────

def enviar_emails_procesos_nuevos(procesos_nuevos: list[dict]):
    """Para cada proceso nuevo: matching → resumen cacheado → email a interesados."""
    if not procesos_nuevos:
        return
    print(f"\n📧 Evaluando emails de interés para {len(procesos_nuevos)} proceso(s) nuevo(s)...")
    total_enviados = 0

    for proceso in procesos_nuevos:
        codigo = proceso.get("codigo_proceso")
        if not codigo:
            continue
        try:
            interesados = encontrar_interesados(proceso)
            if not interesados:
                continue  # nadie matchea → CERO gasto de Gemini

            resumen = obtener_o_generar_resumen(proceso)  # 1 sola llamada, cacheada

            enviados = 0
            for u in interesados:
                ok = _resend_send(
                    subject=f"🎯 Nueva licitación: {(proceso.get('titulo') or '')[:70]}",
                    html=_html_email(proceso, resumen, u["razon_social"]),
                    to_email=u["email"],
                )
                if ok:
                    enviados += 1
                time.sleep(0.6)  # respetar rate limit de Resend (~2/seg)

            total_enviados += enviados
            print(f"   ✉️  {codigo}: {enviados}/{len(interesados)} emails enviados")

            try:
                supabase_admin.table("resumenes_email") \
                    .update({"emails_enviados": enviados}) \
                    .eq("codigo_proceso", codigo).execute()
            except Exception:
                pass

        except Exception as e:
            print(f"   ❌ Error en emails de {codigo}: {e}")

    if total_enviados:
        print(f"📧 Total: {total_enviados} email(s) de interés enviados\n")
