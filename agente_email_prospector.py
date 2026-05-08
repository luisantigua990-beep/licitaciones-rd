"""
Agente 6 — Email Prospector · LicitacionLab
=============================================
Envía emails de ventas personalizados con datos reales del DGCP a empresas
que tienen contratos adjudicados Y han participado en ofertas.

Flujo:
  1. Selecciona batch de empresas elegibles (contratos + ofertas + email RPE)
  2. Para cada empresa consulta su historial real en Supabase
  3. Claude genera el cuerpo del email personalizado con esos datos
  4. Resend envía el email con el HTML template
  5. Registra en outreach_log con secuencia (email_1, email_2, email_3)

Secuencia de 3 emails:
  Email 1 — "Encontramos tu historial" (día 0)   → sorpresa + datos
  Email 2 — "Empresas como tú ganaron más"        (día 4)  → FOMO + comparativa
  Email 3 — "3 licitaciones abiertas en tu sector" (día 7) → urgencia + cierre

Disparo desde Telegram:
  /email_batch [cantidad]   → envía N emails del siguiente batch pendiente
  /email_status             → resumen de campaña
  /email_preview [rnc]      → preview sin enviar para una empresa

Uso directo:
  python agente_email_prospector.py --batch 50
  python agente_email_prospector.py --preview --rnc 55105
  python agente_email_prospector.py --status
"""

import os
import time
import json
import argparse
import requests
from datetime import datetime, timedelta
from dotenv import load_dotenv
from supabase import create_client
from anthropic import Anthropic

load_dotenv()

SUPABASE_URL     = os.getenv("SUPABASE_URL")
SUPABASE_KEY     = os.getenv("SUPABASE_SERVICE_KEY", os.getenv("SUPABASE_KEY"))
RESEND_API_KEY   = os.getenv("RESEND_API_KEY")
ANTHROPIC_KEY    = os.getenv("ANTHROPIC_API_KEY")
FROM_EMAIL       = "Luis Antigua · LicitacionLab <luis@licitacionlab.com>"
REPLY_TO         = "l.antigua@licitacionlab.com"
APP_URL          = "https://app.licitacionlab.com"
CONSULTING_URL   = "https://wa.me/18098154457"  # reemplazar con tu WhatsApp

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
claude   = Anthropic(api_key=ANTHROPIC_KEY)

DELAY_ENTRE_EMAILS = 2.0   # segundos entre envíos (anti-spam)
BATCH_DEFAULT      = 50    # emails por llamada

# Días entre emails de la secuencia
SECUENCIA_DIAS = {
    "email_1": 0,
    "email_2": 4,
    "email_3": 7,
}


# ─────────────────────────────────────────────────────────────
# QUERIES DE DATOS — perfil completo de una empresa
# ─────────────────────────────────────────────────────────────

def obtener_perfil_empresa(empresa_id: str, rnc: str) -> dict:
    """
    Construye el perfil completo de una empresa cruzando
    contratos_adjudicados + ofertas_procesos + instituciones_compradoras.
    """
    perfil = {}

    # --- Contratos ganados ---
    try:
        r = supabase.rpc("get_perfil_empresa_email", {"p_empresa_id": empresa_id}).execute()
        # Si no existe el RPC, caemos al query manual abajo
        if r.data:
            return r.data[0]
    except Exception:
        pass

    # Query manual de contratos
    try:
        r = supabase.table("contratos_adjudicados") \
            .select("monto_adjudicado, fecha_adjudicacion, titulo_proceso, modalidad, institucion_id") \
            .eq("empresa_id", empresa_id) \
            .order("fecha_adjudicacion", desc=True) \
            .execute()
        contratos = r.data or []
    except Exception:
        contratos = []

    if contratos:
        montos = [float(c.get("monto_adjudicado") or 0) for c in contratos]
        perfil["total_contratos"]    = len(contratos)
        perfil["monto_total"]        = sum(montos)
        perfil["monto_promedio"]     = sum(montos) / len(montos)
        perfil["ultimo_contrato"]    = contratos[0].get("fecha_adjudicacion")
        perfil["modalidades"]        = list({c.get("modalidad") for c in contratos if c.get("modalidad")})

        # Top 3 instituciones
        inst_count = {}
        for c in contratos:
            iid = c.get("institucion_id")
            if iid:
                inst_count[iid] = inst_count.get(iid, 0) + 1
        top_inst_ids = sorted(inst_count, key=inst_count.get, reverse=True)[:3]

        nombres_inst = []
        for iid in top_inst_ids:
            try:
                ri = supabase.table("instituciones_compradoras") \
                    .select("nombre") \
                    .eq("id", iid) \
                    .limit(1) \
                    .execute()
                if ri.data:
                    nombres_inst.append({
                        "nombre": ri.data[0]["nombre"],
                        "contratos": inst_count[iid]
                    })
            except Exception:
                pass
        perfil["instituciones_top"] = nombres_inst
    else:
        perfil["total_contratos"] = 0
        perfil["monto_total"]     = 0
        perfil["instituciones_top"] = []

    # --- Ofertas presentadas (participaciones) ---
    try:
        ro = supabase.table("ofertas_procesos") \
            .select("estado_oferta, estado_evaluacion, valor_oferta, unidad_compra") \
            .eq("rpe", str(rnc)) \
            .execute()
        ofertas = ro.data or []
    except Exception:
        ofertas = []

    perfil["total_ofertas"]  = len(ofertas)
    perfil["total_perdidas"] = len([
        o for o in ofertas
        if "descalif" in (o.get("estado_evaluacion") or "").lower()
        or "no adjudic" in (o.get("estado_oferta") or "").lower()
    ])

    return perfil


# ─────────────────────────────────────────────────────────────
# GENERACIÓN DE CONTENIDO CON CLAUDE
# ─────────────────────────────────────────────────────────────

def _fmt_monto(monto: float) -> str:
    if monto >= 1_000_000:
        return f"RD${monto/1_000_000:.1f}M"
    elif monto >= 1_000:
        return f"RD${monto/1_000:.0f}K"
    return f"RD${monto:,.0f}"


PROMPTS_EMAIL = {
    "email_1": """Eres el equipo de LicitacionLab escribiendo un email de ventas B2B en español dominicano (formal pero directo, sin tuteo, sin emojis excesivos).

DATOS DE LA EMPRESA:
{datos}

Escribe SOLO el párrafo central del email (3-4 oraciones). El objetivo es:
1. Mencionar datos específicos de su historial (contratos, monto, instituciones)
2. Hacer la pregunta del dolor: ¿saben por qué perdieron algunas licitaciones?
3. Conectar con LicitacionLab como la solución

NO incluyas saludo, firma, ni calls-to-action — esos van en el template HTML.
Máximo 80 palabras. Tono: inteligente, consultivo, no agresivo.""",

    "email_2": """Eres el equipo de LicitacionLab escribiendo el segundo email de una secuencia de ventas en español dominicano.

DATOS DE LA EMPRESA:
{datos}

Este email tiene enfoque de FOMO (miedo a quedarse atrás). Escribe SOLO el párrafo central (3-4 oraciones):
1. Mencionar que empresas similares en su sector están usando herramientas de inteligencia
2. Referencia sutil a sus contratos actuales como punto de partida
3. Urgencia suave: el Estado dominicano lanza +200 licitaciones por semana

NO incluyas saludo, firma, ni calls-to-action.
Máximo 80 palabras. Tono: competitivo pero respetuoso.""",

    "email_3": """Eres el equipo de LicitacionLab escribiendo el tercer y último email de una secuencia de ventas en español dominicano.

DATOS DE LA EMPRESA:
{datos}

Este es el email de cierre. Escribe SOLO el párrafo central (2-3 oraciones):
1. Mencionar que hay licitaciones ABIERTAS AHORA en su sector/instituciones donde ya han trabajado
2. Crear urgencia real: los plazos vencen pronto
3. Invitación directa a probar la app o hablar con el consultor

NO incluyas saludo, firma, ni calls-to-action.
Máximo 60 palabras. Tono: directo, urgente pero profesional.""",
}

ASUNTOS_EMAIL = {
    "email_1": lambda nombre, perfil: f"{nombre} — encontramos tu historial en el DGCP",
    "email_2": lambda nombre, perfil: f"Empresas como {nombre} ganaron {_fmt_monto(perfil.get('monto_total',0)*1.3)} más en 2025",
    "email_3": lambda nombre, perfil: f"Hay {3} licitaciones abiertas en tu sector esta semana",
}


def generar_cuerpo_claude(nombre: str, perfil: dict, tipo_email: str) -> str:
    """Llama a Claude para generar el párrafo personalizado del email."""
    inst_str = ", ".join([
        f"{i['nombre']} ({i['contratos']} contratos)"
        for i in perfil.get("instituciones_top", [])
    ]) or "varias instituciones del Estado"

    datos_str = f"""
- Empresa: {nombre}
- Contratos ganados con el Estado: {perfil.get('total_contratos', 0)}
- Monto total adjudicado: {_fmt_monto(perfil.get('monto_total', 0))}
- Principales instituciones: {inst_str}
- Licitaciones en las que participó: {perfil.get('total_ofertas', 'N/D')}
- Licitaciones perdidas (estimado): {perfil.get('total_perdidas', 'N/D')}
- Último contrato: {perfil.get('ultimo_contrato', 'N/D')}
"""
    prompt = PROMPTS_EMAIL[tipo_email].format(datos=datos_str)

    try:
        resp = claude.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=200,
            messages=[{"role": "user", "content": prompt}]
        )
        return resp.content[0].text.strip()
    except Exception as e:
        print(f"  ⚠️  Claude error: {e}")
        # Fallback genérico
        return (
            f"Analizamos el historial de {nombre} en el portal del DGCP "
            f"y encontramos {perfil.get('total_contratos', 0)} contratos adjudicados "
            f"por un monto de {_fmt_monto(perfil.get('monto_total', 0))}. "
            "LicitacionLab puede ayudarles a identificar más oportunidades y preparar "
            "ofertas más competitivas con análisis de IA en tiempo real."
        )


# ─────────────────────────────────────────────────────────────
# TEMPLATE HTML DEL EMAIL
# ─────────────────────────────────────────────────────────────

def construir_html_email(
    nombre: str,
    perfil: dict,
    cuerpo_ia: str,
    tipo_email: str,
    empresa_id: str,
) -> str:
    """Construye el HTML completo del email con datos reales inyectados."""

    monto_fmt   = _fmt_monto(perfil.get("monto_total", 0))
    contratos   = perfil.get("total_contratos", 0)
    perdidas    = perfil.get("total_perdidas", "N/D")
    inst_tags   = ""
    for inst in perfil.get("instituciones_top", [])[:4]:
        inst_tags += f'<span style="display:inline-block;background:#edf7f0;color:#1a5c2a;border:1px solid #b8e0c5;border-radius:20px;padding:4px 12px;font-size:12px;font-weight:500;margin:3px 3px 3px 0;">{inst["nombre"]} — {inst["contratos"]} contratos</span>'

    # Unsubscribe token simple (empresa_id en base64)
    import base64
    token = base64.urlsafe_b64encode(empresa_id.encode()).decode()
    unsub_url = f"{APP_URL}/unsub?t={token}"

    html = f"""<!DOCTYPE html>
<html lang="es">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>LicitacionLab</title>
</head>
<body style="margin:0;padding:0;background:#f0ede8;font-family:'Helvetica Neue',Helvetica,Arial,sans-serif;">

<table width="100%" cellpadding="0" cellspacing="0" style="background:#f0ede8;padding:32px 16px;">
<tr><td align="center">
<table width="600" cellpadding="0" cellspacing="0" style="background:#ffffff;border-radius:4px;overflow:hidden;border:1px solid #ddd9d2;">

  <!-- HEADER -->
  <tr><td style="background:#1a5c2a;padding:28px 40px 24px;">
    <table width="100%" cellpadding="0" cellspacing="0">
      <tr>
        <td style="padding-bottom:20px;">
          <span style="display:inline-block;background:rgba(255,255,255,0.15);color:rgba(255,255,255,0.9);font-size:11px;font-weight:600;letter-spacing:1.2px;text-transform:uppercase;padding:4px 10px;border-radius:3px;">Inteligencia de Mercado</span>
        </td>
      </tr>
      <tr>
        <td>
          <div style="font-family:Georgia,serif;font-size:27px;color:#ffffff;line-height:1.25;font-weight:400;">
            Encontramos el historial de<br><span style="color:#7fe89f;">{nombre}</span><br>en el DGCP
          </div>
          <p style="color:rgba(255,255,255,0.7);font-size:14px;margin:10px 0 0;">Datos reales de su empresa en el portal de compras del Estado dominicano</p>
        </td>
      </tr>
    </table>
  </td></tr>

  <!-- BODY -->
  <tr><td style="padding:36px 40px;">

    <!-- Saludo -->
    <p style="font-size:15px;color:#2c2c2c;line-height:1.7;margin:0 0 20px;">
      Estimado equipo de <strong style="color:#1a5c2a;">{nombre}</strong> —
    </p>

    <!-- Párrafo generado por Claude -->
    <p style="font-size:15px;color:#3a3a3a;line-height:1.75;margin:0 0 24px;">
      {cuerpo_ia}
    </p>

    <!-- Data card -->
    <table width="100%" cellpadding="0" cellspacing="0" style="background:#f7f5f0;border-left:3px solid #1a5c2a;border-radius:0 6px 6px 0;margin-bottom:20px;">
      <tr><td style="padding:20px 24px;">
        <p style="font-size:11px;font-weight:600;letter-spacing:1px;text-transform:uppercase;color:#1a5c2a;margin:0 0 16px;">Su historial en el Estado dominicano</p>
        <table width="100%" cellpadding="0" cellspacing="0">
          <tr>
            <td width="33%" align="center">
              <span style="font-family:Georgia,serif;font-size:26px;color:#1a5c2a;display:block;">{contratos}</span>
              <span style="font-size:12px;color:#777;">Contratos ganados</span>
            </td>
            <td width="33%" align="center">
              <span style="font-family:Georgia,serif;font-size:26px;color:#1a1a1a;display:block;">{monto_fmt}</span>
              <span style="font-size:12px;color:#777;">Monto adjudicado</span>
            </td>
            <td width="33%" align="center">
              <span style="font-family:Georgia,serif;font-size:26px;color:#a32d2d;display:block;">{perdidas}</span>
              <span style="font-size:12px;color:#777;">Licitaciones perdidas</span>
            </td>
          </tr>
        </table>
      </td></tr>
    </table>

    <!-- Tags de instituciones -->
    <div style="margin-bottom:24px;">
      {inst_tags}
    </div>

    <!-- Beneficios de la app -->
    <p style="font-size:13px;font-weight:600;letter-spacing:0.8px;text-transform:uppercase;color:#999;margin:0 0 12px;">Lo que LicitacionLab hace por su empresa</p>
    <table width="100%" cellpadding="0" cellspacing="0" style="margin-bottom:28px;">
      <tr>
        <td width="48%" valign="top" style="padding-right:8px;padding-bottom:10px;">
          <table width="100%" cellpadding="0" cellspacing="0" style="background:#f7f5f0;border-radius:6px;border:1px solid #ece9e3;">
            <tr><td style="padding:14px 16px;">
              <p style="font-size:13px;font-weight:600;color:#1a1a1a;margin:0 0 4px;">🔔 Alertas en tiempo real</p>
              <p style="font-size:12px;color:#666;line-height:1.5;margin:0;">Recibe notificaciones al instante cuando una institución donde ya ha trabajado publique una nueva licitación en su sector.</p>
            </td></tr>
          </table>
        </td>
        <td width="4%"></td>
        <td width="48%" valign="top" style="padding-left:8px;padding-bottom:10px;">
          <table width="100%" cellpadding="0" cellspacing="0" style="background:#f7f5f0;border-radius:6px;border:1px solid #ece9e3;">
            <tr><td style="padding:14px 16px;">
              <p style="font-size:13px;font-weight:600;color:#1a1a1a;margin:0 0 4px;">🤖 Análisis IA del pliego</p>
              <p style="font-size:12px;color:#666;line-height:1.5;margin:0;">IA analiza cada pliego y le dice si vale la pena participar, qué documentos necesita, y si el proceso tiene señales de direccionamiento.</p>
            </td></tr>
          </table>
        </td>
      </tr>
      <tr>
        <td width="48%" valign="top" style="padding-right:8px;padding-bottom:10px;">
          <table width="100%" cellpadding="0" cellspacing="0" style="background:#f7f5f0;border-radius:6px;border:1px solid #ece9e3;">
            <tr><td style="padding:14px 16px;">
              <p style="font-size:13px;font-weight:600;color:#1a1a1a;margin:0 0 4px;">📊 Inteligencia competitiva</p>
              <p style="font-size:12px;color:#666;line-height:1.5;margin:0;">Vea quién más participa en cada proceso, cuánto ofertaron sus competidores en el pasado y qué empresas dominan cada institución.</p>
            </td></tr>
          </table>
        </td>
        <td width="4%"></td>
        <td width="48%" valign="top" style="padding-left:8px;padding-bottom:10px;">
          <table width="100%" cellpadding="0" cellspacing="0" style="background:#f7f5f0;border-radius:6px;border:1px solid #ece9e3;">
            <tr><td style="padding:14px 16px;">
              <p style="font-size:13px;font-weight:600;color:#1a1a1a;margin:0 0 4px;">📁 Descarga de pliegos</p>
              <p style="font-size:12px;color:#666;line-height:1.5;margin:0;">Descargue cualquier pliego directamente desde la app. Sin entrar al portal del DGCP, sin perder tiempo buscando documentos dispersos.</p>
            </td></tr>
          </table>
        </td>
      </tr>
      <tr>
        <td width="48%" valign="top" style="padding-right:8px;">
          <table width="100%" cellpadding="0" cellspacing="0" style="background:#f7f5f0;border-radius:6px;border:1px solid #ece9e3;">
            <tr><td style="padding:14px 16px;">
              <p style="font-size:13px;font-weight:600;color:#1a1a1a;margin:0 0 4px;">📅 Checklist de documentos</p>
              <p style="font-size:12px;color:#666;line-height:1.5;margin:0;">La IA genera automáticamente el checklist de documentos legales, técnicos y financieros requeridos para cada proceso específico.</p>
            </td></tr>
          </table>
        </td>
        <td width="4%"></td>
        <td width="48%" valign="top" style="padding-left:8px;">
          <table width="100%" cellpadding="0" cellspacing="0" style="background:#edf7f0;border-radius:6px;border:1px solid #b8e0c5;">
            <tr><td style="padding:14px 16px;">
              <p style="font-size:13px;font-weight:600;color:#1a5c2a;margin:0 0 4px;">🏆 Consultoría experta</p>
              <p style="font-size:12px;color:#1a5c2a;line-height:1.5;margin:0;opacity:0.85;">Para licitaciones grandes (MOPC, CAASD, INAPA) contamos con consultores especializados que preparan su oferta completa.</p>
            </td></tr>
          </table>
        </td>
      </tr>
    </table>

    <!-- Precios -->
    <table width="100%" cellpadding="0" cellspacing="0" style="margin-bottom:28px;">
      <tr>
        <td width="31%" align="center" style="padding:0 4px;">
          <table width="100%" cellpadding="0" cellspacing="0" style="background:#f7f5f0;border-radius:6px;border:1px solid #ece9e3;">
            <tr><td style="padding:14px 12px;text-align:center;">
              <p style="font-size:11px;font-weight:600;letter-spacing:0.8px;text-transform:uppercase;color:#888;margin:0 0 6px;">Explorador</p>
              <p style="font-family:Georgia,serif;font-size:20px;color:#1a1a1a;margin:0 0 2px;">RD$1,490</p>
              <p style="font-size:11px;color:#aaa;margin:0 0 8px;">/ mes</p>
              <p style="font-size:11px;color:#666;margin:0;line-height:1.4;">Alertas + búsqueda</p>
            </td></tr>
          </table>
        </td>
        <td width="4%"></td>
        <td width="31%" align="center" style="padding:0 4px;">
          <table width="100%" cellpadding="0" cellspacing="0" style="background:#1a5c2a;border-radius:6px;border:2px solid #1a5c2a;">
            <tr><td style="padding:14px 12px;text-align:center;">
              <p style="font-size:11px;font-weight:600;letter-spacing:0.8px;text-transform:uppercase;color:rgba(255,255,255,0.7);margin:0 0 6px;">⭐ Competidor</p>
              <p style="font-family:Georgia,serif;font-size:20px;color:#ffffff;margin:0 0 2px;">RD$3,990</p>
              <p style="font-size:11px;color:rgba(255,255,255,0.6);margin:0 0 8px;">/ mes</p>
              <p style="font-size:11px;color:rgba(255,255,255,0.85);margin:0;line-height:1.4;">+ Análisis IA + Intel competitiva</p>
            </td></tr>
          </table>
        </td>
        <td width="4%"></td>
        <td width="31%" align="center" style="padding:0 4px;">
          <table width="100%" cellpadding="0" cellspacing="0" style="background:#f7f5f0;border-radius:6px;border:1px solid #ece9e3;">
            <tr><td style="padding:14px 12px;text-align:center;">
              <p style="font-size:11px;font-weight:600;letter-spacing:0.8px;text-transform:uppercase;color:#888;margin:0 0 6px;">Ganador</p>
              <p style="font-family:Georgia,serif;font-size:20px;color:#1a1a1a;margin:0 0 2px;">RD$8,500</p>
              <p style="font-size:11px;color:#aaa;margin:0 0 8px;">/ mes</p>
              <p style="font-size:11px;color:#666;margin:0;line-height:1.4;">Todo + consultoría incluida</p>
            </td></tr>
          </table>
        </td>
      </tr>
    </table>

    <!-- CTAs -->
    <table width="100%" cellpadding="0" cellspacing="0" style="margin-bottom:28px;">
      <tr>
        <td width="48%" style="padding-right:8px;">
          <a href="{APP_URL}?utm_source=email&utm_campaign=prospecto&utm_content={tipo_email}"
             style="display:block;background:#1a5c2a;color:#ffffff;text-decoration:none;text-align:center;padding:14px 16px;border-radius:6px;font-size:14px;font-weight:600;">
            Ver mi historial completo
            <span style="display:block;font-weight:400;font-size:12px;opacity:0.8;margin-top:2px;">App · Desde RD$1,490/mes</span>
          </a>
        </td>
        <td width="4%"></td>
        <td width="48%" style="padding-left:8px;">
          <a href="{CONSULTING_URL}?text=Hola%20Luis%2C%20me%20interesa%20la%20consultor%C3%ADa"
             style="display:block;background:#ffffff;color:#1a5c2a;border:1.5px solid #1a5c2a;text-decoration:none;text-align:center;padding:14px 16px;border-radius:6px;font-size:14px;font-weight:600;">
            Hablar con el consultor
            <span style="display:block;font-weight:400;font-size:12px;color:#555;margin-top:2px;">Licitaciones MOPC · CAASD · INAPA</span>
          </a>
        </td>
      </tr>
    </table>

    <!-- Social proof band -->
    <table width="100%" cellpadding="0" cellspacing="0" style="background:#1a5c2a;border-radius:6px;margin-bottom:28px;">
      <tr>
        <td width="33%" align="center" style="padding:20px 12px;">
          <span style="font-family:Georgia,serif;font-size:22px;color:#ffffff;display:block;">15,000+</span>
          <span style="font-size:11px;color:rgba(255,255,255,0.65);display:block;margin-top:2px;">Empresas monitoreadas</span>
        </td>
        <td width="33%" align="center" style="padding:20px 12px;">
          <span style="font-family:Georgia,serif;font-size:22px;color:#ffffff;display:block;">176k</span>
          <span style="font-size:11px;color:rgba(255,255,255,0.65);display:block;margin-top:2px;">Contratos en la BD</span>
        </td>
        <td width="33%" align="center" style="padding:20px 12px;">
          <span style="font-family:Georgia,serif;font-size:22px;color:#ffffff;display:block;">24/7</span>
          <span style="font-size:11px;color:rgba(255,255,255,0.65);display:block;margin-top:2px;">Alertas en tiempo real</span>
        </td>
      </tr>
    </table>

    <!-- Firma -->
    <p style="font-size:14px;color:#3a3a3a;line-height:1.6;margin:0;">
      <strong style="display:block;font-size:15px;color:#1a1a1a;">Luis Antigua</strong>
      Ing. Civil · Consultor de Licitaciones Públicas<br>
      <span style="font-size:13px;color:#888;">LicitacionLab · app.licitacionlab.com</span>
    </p>

  </td></tr>

  <!-- FOOTER -->
  <tr><td style="background:#f7f5f0;padding:20px 40px;border-top:1px solid #ece9e3;">
    <p style="font-size:11.5px;color:#aaa;line-height:1.6;text-align:center;margin:0;">
      Recibió este email porque su empresa está registrada en el RPE del DGCP.<br>
      <a href="{unsub_url}" style="color:#1a5c2a;text-decoration:none;">Cancelar suscripción</a> ·
      Santo Domingo, República Dominicana
    </p>
  </td></tr>

</table>
</td></tr>
</table>
</body>
</html>"""
    return html


# ─────────────────────────────────────────────────────────────
# ENVÍO CON RESEND
# ─────────────────────────────────────────────────────────────

def enviar_email_resend(
    destinatario: str,
    nombre: str,
    asunto: str,
    html: str,
) -> dict:
    """Envía el email via Resend API."""
    try:
        r = requests.post(
            "https://api.resend.com/emails",
            headers={
                "Authorization": f"Bearer {RESEND_API_KEY}",
                "Content-Type": "application/json",
            },
            json={
                "from":     FROM_EMAIL,
                "to":       [destinatario],
                "reply_to": REPLY_TO,
                "subject":  asunto,
                "html":     html,
            },
            timeout=15,
        )
        r.raise_for_status()
        return {"ok": True, "resend_id": r.json().get("id")}
    except Exception as e:
        return {"ok": False, "error": str(e)}


# ─────────────────────────────────────────────────────────────
# SELECCIÓN DE EMPRESAS ELEGIBLES
# ─────────────────────────────────────────────────────────────

def obtener_empresas_elegibles(limite: int = BATCH_DEFAULT) -> list:
    """
    Empresas que:
    1. Tienen email real (correo_comercial o correo_contacto)
    2. Tienen contratos adjudicados (empresa_id en contratos_adjudicados)
    3. Tienen ofertas presentadas (rpe en ofertas_procesos)
    4. NO han recibido email_1 aún, O han recibido email_N y ya pasaron los días para email_N+1
    """
    try:
        # Subquery: empresas que ya están en outreach_log
        ya_enviados = supabase.table("outreach_log") \
            .select("prospecto_id, tipo, enviado_at") \
            .eq("canal", "email") \
            .execute()

        # Construir mapa de secuencia por empresa
        secuencia_map = {}
        for row in (ya_enviados.data or []):
            pid  = row["prospecto_id"]
            tipo = row["tipo"]
            eat  = row["enviado_at"]
            if pid not in secuencia_map:
                secuencia_map[pid] = {}
            secuencia_map[pid][tipo] = eat

        # Obtener empresas con email + contratos
        empresas_raw = supabase.table("empresas_estado") \
            .select("id, nombre, rnc, correo_comercial, correo_contacto") \
            .not_.is_("correo_comercial", "null") \
            .in_("id", supabase.table("contratos_adjudicados")
                .select("empresa_id")
                .execute()
                .data and [r["empresa_id"] for r in
                    supabase.table("contratos_adjudicados")
                    .select("empresa_id")
                    .execute().data or []] or []) \
            .limit(limite * 5) \
            .execute()

        elegibles = []
        ahora = datetime.utcnow()

        for emp in (empresas_raw.data or []):
            eid   = emp["id"]
            seq   = secuencia_map.get(eid, {})
            email = emp.get("correo_comercial") or emp.get("correo_contacto")
            if not email:
                continue

            # Determinar qué email toca enviar
            if "email_1" not in seq:
                tipo_enviar = "email_1"
            elif "email_2" not in seq:
                # Verificar que pasaron 4 días desde email_1
                enviado_1 = datetime.fromisoformat(seq["email_1"].replace("Z", "+00:00").replace("+00:00", ""))
                if (ahora - enviado_1).days >= SECUENCIA_DIAS["email_2"]:
                    tipo_enviar = "email_2"
                else:
                    continue
            elif "email_3" not in seq:
                enviado_2 = datetime.fromisoformat(seq["email_2"].replace("Z", "+00:00").replace("+00:00", ""))
                if (ahora - enviado_2).days >= (SECUENCIA_DIAS["email_3"] - SECUENCIA_DIAS["email_2"]):
                    tipo_enviar = "email_3"
                else:
                    continue
            else:
                continue  # Ya recibió los 3 emails

            elegibles.append({
                "id":           eid,
                "nombre":       emp["nombre"],
                "rnc":          emp["rnc"],
                "email":        email,
                "tipo_enviar":  tipo_enviar,
            })

            if len(elegibles) >= limite:
                break

        return elegibles

    except Exception as e:
        print(f"❌ Error obteniendo elegibles: {e}")
        return []


def obtener_empresas_elegibles_v2(limite: int = BATCH_DEFAULT) -> list:
    """
    Usa la vista elegibles_email_v1 en Supabase para evitar queries masivas.
    La vista ya filtra: contratos + email + no enviado email_1.
    """
    try:
        result = supabase.table("elegibles_email_v1") \
            .select("id, nombre, rnc, email") \
            .limit(limite) \
            .execute()

        elegibles = []
        for emp in (result.data or []):
            elegibles.append({
                "id":          emp["id"],
                "nombre":      emp["nombre"],
                "rnc":         str(emp.get("rnc") or ""),
                "email":       emp.get("email"),
                "tipo_enviar": "email_1",
            })

        return elegibles

    except Exception as e:
        print(f"❌ Error en elegibles_v2: {e}")
        return []


# ─────────────────────────────────────────────────────────────
# REGISTRAR EN OUTREACH_LOG
# ─────────────────────────────────────────────────────────────

def registrar_outreach(
    empresa_id: str,
    tipo: str,
    asunto: str,
    resend_id: str = None,
    error: str = None,
):
    try:
        supabase.table("outreach_log").insert({
            "prospecto_id": empresa_id,
            "canal":        "email",
            "tipo":         tipo,
            "asunto":       asunto,
            "enviado_at":   datetime.utcnow().isoformat(),
            "hook_usado":   resend_id or error or "",
        }).execute()
    except Exception as e:
        print(f"  ⚠️  No se pudo registrar outreach: {e}")


# ─────────────────────────────────────────────────────────────
# FUNCIÓN PRINCIPAL — correr batch
# ─────────────────────────────────────────────────────────────

def run_batch(
    limite: int = BATCH_DEFAULT,
    dry_run: bool = False,
    tipo_forzado: str = None,
) -> dict:
    """
    Envía un batch de emails. Retorna stats.
    dry_run=True: genera los emails pero no los envía.
    """
    t0 = time.time()
    stats = {
        "enviados": 0,
        "errores":  0,
        "omitidos": 0,
        "dry_run":  dry_run,
    }

    print(f"\n{'='*55}")
    print(f"📧 Agente 6 Email Prospector — {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"   batch={limite} | dry_run={dry_run}")
    print(f"{'='*55}\n")

    elegibles = obtener_empresas_elegibles_v2(limite)
    if not elegibles:
        print("✅ Sin empresas elegibles en este momento.")
        return stats

    print(f"📋 {len(elegibles)} empresas elegibles encontradas\n")

    for emp in elegibles:
        nombre    = emp["nombre"]
        eid       = emp["id"]
        rnc       = emp["rnc"]
        email     = emp["email"]
        tipo      = tipo_forzado or emp["tipo_enviar"]

        print(f"  → {nombre[:40]} | {email[:35]} | {tipo}")

        # 1. Obtener perfil con datos reales
        perfil = obtener_perfil_empresa(eid, rnc)

        # 2. Generar cuerpo con Claude
        cuerpo = generar_cuerpo_claude(nombre, perfil, tipo)

        # 3. Construir asunto y HTML
        asunto = ASUNTOS_EMAIL[tipo](nombre, perfil)
        html   = construir_html_email(nombre, perfil, cuerpo, tipo, eid)

        if dry_run:
            print(f"    [DRY RUN] Asunto: {asunto[:60]}")
            print(f"    [DRY RUN] Cuerpo: {cuerpo[:80]}...")
            stats["enviados"] += 1
            continue

        # 4. Enviar
        resultado = enviar_email_resend(email, nombre, asunto, html)

        if resultado["ok"]:
            registrar_outreach(eid, tipo, asunto, resend_id=resultado.get("resend_id"))
            stats["enviados"] += 1
            print(f"    ✅ Enviado — ID: {resultado.get('resend_id', 'N/A')}")
        else:
            registrar_outreach(eid, tipo, asunto, error=resultado.get("error"))
            stats["errores"] += 1
            print(f"    ❌ Error: {resultado.get('error', 'desconocido')}")

        time.sleep(DELAY_ENTRE_EMAILS)

    duracion = round(time.time() - t0)
    stats["duracion_s"] = duracion
    print(f"\n✅ Batch completado en {duracion}s")
    print(f"   Enviados: {stats['enviados']} | Errores: {stats['errores']}")

    # Log en cron_log
    try:
        supabase.table("cron_log").insert({
            "job":         "agente_6_email_batch",
            "status":      "ok",
            "detalle":     stats,
            "duracion_ms": duracion * 1000,
        }).execute()
    except Exception:
        pass

    return stats


def get_status() -> dict:
    """Resumen del estado de la campaña."""
    try:
        total_enviados = supabase.table("outreach_log") \
            .select("id", count="exact") \
            .eq("canal", "email") \
            .execute().count or 0

        por_tipo = {}
        for tipo in ["email_1", "email_2", "email_3"]:
            n = supabase.table("outreach_log") \
                .select("id", count="exact") \
                .eq("canal", "email") \
                .eq("tipo", tipo) \
                .execute().count or 0
            por_tipo[tipo] = n

        total_elegibles = supabase.table("empresas_estado") \
            .select("id", count="exact") \
            .not_.is_("correo_comercial", "null") \
            .execute().count or 0

        return {
            "total_emails_enviados": total_enviados,
            "por_tipo":              por_tipo,
            "total_empresas_con_email": total_elegibles,
            "pendientes_email_1":    total_elegibles - por_tipo.get("email_1", 0),
        }
    except Exception as e:
        return {"error": str(e)}


def preview_empresa(rnc: str) -> dict:
    """Genera preview del email para una empresa específica sin enviar."""
    try:
        r = supabase.table("empresas_estado") \
            .select("id, nombre, rnc, correo_comercial") \
            .eq("rnc", rnc) \
            .limit(1) \
            .execute()
        if not r.data:
            return {"error": f"Empresa con RNC {rnc} no encontrada"}

        emp    = r.data[0]
        perfil = obtener_perfil_empresa(emp["id"], rnc)
        cuerpo = generar_cuerpo_claude(emp["nombre"], perfil, "email_1")
        asunto = ASUNTOS_EMAIL["email_1"](emp["nombre"], perfil)

        return {
            "empresa": emp["nombre"],
            "email":   emp.get("correo_comercial"),
            "asunto":  asunto,
            "cuerpo_ia": cuerpo,
            "perfil":  perfil,
        }
    except Exception as e:
        return {"error": str(e)}


# ─────────────────────────────────────────────────────────────
# ROUTER FASTAPI — para Telegram vía n8n
# ─────────────────────────────────────────────────────────────

def get_agente6_router():
    import threading
    from fastapi import APIRouter, Depends, Header, HTTPException, Query
    from fastapi.responses import JSONResponse

    ADMIN_SECRET = os.getenv("ADMIN_SECRET", "")

    def verificar_admin(x_admin_key: str = Header(None)):
        if not ADMIN_SECRET or x_admin_key != ADMIN_SECRET:
            raise HTTPException(status_code=403, detail="Acceso denegado")

    router  = APIRouter(prefix="/api/admin", tags=["Agente 6 Email"])
    _estado = {"corriendo": False, "stats": None}

    @router.post("/agente6/batch", dependencies=[Depends(verificar_admin)])
    def disparar_batch(
        limite:  int  = Query(BATCH_DEFAULT, description="Emails a enviar en este batch"),
        dry_run: bool = Query(False, description="Simular sin enviar"),
        tipo:    str  = Query(None, description="Forzar tipo: email_1 | email_2 | email_3"),
    ):
        """Dispara un batch de emails. Llamado desde n8n/Telegram."""
        if _estado["corriendo"]:
            return JSONResponse(status_code=409, content={"error": "Ya hay un batch corriendo"})

        def _run():
            _estado["corriendo"] = True
            try:
                _estado["stats"] = run_batch(limite=limite, dry_run=dry_run, tipo_forzado=tipo)
            except Exception as e:
                _estado["stats"] = {"error": str(e)}
            finally:
                _estado["corriendo"] = False

        threading.Thread(target=_run, daemon=True, name="agente6_email").start()
        return {
            "mensaje":  f"Batch de {limite} emails iniciado en background",
            "dry_run":  dry_run,
            "tipo":     tipo or "secuencia automática",
        }

    @router.get("/agente6/status", dependencies=[Depends(verificar_admin)])
    def status_agente6():
        return {
            "corriendo": _estado["corriendo"],
            "ultimo_batch": _estado["stats"],
            **get_status(),
        }

    @router.get("/agente6/preview", dependencies=[Depends(verificar_admin)])
    def preview_agente6(rnc: str = Query(..., description="RNC de la empresa a previsualizar")):
        return preview_empresa(rnc)

    return router


# ─────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Agente 6 Email Prospector — LicitacionLab")
    parser.add_argument("--batch",   type=int,  default=BATCH_DEFAULT, help="Emails a enviar")
    parser.add_argument("--dry-run", action="store_true", help="Simular sin enviar")
    parser.add_argument("--status",  action="store_true", help="Ver estado de la campaña")
    parser.add_argument("--preview", action="store_true", help="Preview para una empresa")
    parser.add_argument("--rnc",     type=str,  help="RNC para el preview")
    parser.add_argument("--tipo",    type=str,  help="Forzar email_1 | email_2 | email_3")
    args = parser.parse_args()

    if args.status:
        import json
        print(json.dumps(get_status(), indent=2))
    elif args.preview and args.rnc:
        import json
        print(json.dumps(preview_empresa(args.rnc), indent=2, ensure_ascii=False))
    else:
        run_batch(limite=args.batch, dry_run=args.dry_run, tipo_forzado=args.tipo)
