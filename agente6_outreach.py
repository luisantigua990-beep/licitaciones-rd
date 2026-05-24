import os
import json
import math
import httpx
from datetime import datetime, timezone
from fastapi import APIRouter, Header, HTTPException
from supabase import create_client

# ─── Config ────────────────────────────────────────────────────────────────
SUPABASE_URL    = os.getenv("SUPABASE_URL")
SUPABASE_KEY    = os.getenv("SUPABASE_SERVICE_KEY")
RESEND_API_KEY  = os.getenv("RESEND_API_KEY")
GEMINI_API_KEY  = os.getenv("GEMINI_API_KEY")
AGENT_SECRET    = os.getenv("AGENT_SECRET", "licitacionlab-growth-2026")
FROM_EMAIL      = "notificaciones@licitacionlab.com"
APP_URL         = os.getenv("APP_URL", "https://app.licitacionlab.com")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
router   = APIRouter(prefix="/api/agentes/outreach", tags=["Agente 6 Outreach"])

# ─── Helpers ────────────────────────────────────────────────────────────────

def _auth(secret: str):
    if secret != AGENT_SECRET:
        raise HTTPException(status_code=401, detail="No autorizado")


def _fmt_monto(monto) -> str:
    """RD$1,234,567"""
    try:
        return f"RD${int(float(monto)):,}"
    except Exception:
        return "RD$0"


def _tasa(ganadas: int, total: int) -> str:
    if total == 0:
        return "0%"
    return f"{round(ganadas / total * 100)}%"


def _stats_empresa(rnc: str) -> dict:
    """Saca estadísticas reales de ofertas_procesos para una empresa."""
    try:
        res = supabase.rpc("stats_outreach_empresa", {"p_rnc": rnc}).execute()
        if res.data:
            return res.data[0]
    except Exception:
        pass
    # Fallback directo si la RPC no existe aún
    rows = (
        supabase.table("ofertas_procesos")
        .select("estado_oferta, valor_oferta")
        .eq("rpe", rnc)
        .execute()
        .data or []
    )
    total     = len(rows)
    ganadas   = sum(1 for r in rows if r.get("estado_oferta") == "Aprobada")
    monto_gan = sum(float(r.get("valor_oferta") or 0) for r in rows if r.get("estado_oferta") == "Aprobada")
    return {
        "total_ofertas": total,
        "ofertas_ganadas": ganadas,
        "monto_ganado": monto_gan,
    }


def _ya_es_usuario(email: str) -> bool:
    """Verifica si el email ya está registrado en la app."""
    res = supabase.table("usuarios").select("id").eq("email", email).execute()
    return bool(res.data)


def _ultimo_outreach(empresa_id: str) -> dict | None:
    """Retorna el último outreach enviado a esta empresa."""
    res = (
        supabase.table("outreach_log")
        .select("*")
        .eq("prospecto_id", empresa_id)
        .eq("canal", "email")
        .order("enviado_at", desc=True)
        .limit(1)
        .execute()
    )
    return res.data[0] if res.data else None


def _numero_siguiente_email(empresa_id: str) -> int | None:
    """
    Retorna el número del próximo email a enviar (1-4).
    None si ya completó la secuencia o si aún no es tiempo.
    """
    logs = (
        supabase.table("outreach_log")
        .select("tipo, enviado_at")
        .eq("prospecto_id", empresa_id)
        .eq("canal", "email")
        .order("enviado_at", desc=True)
        .execute()
        .data or []
    )
    if not logs:
        return 1  # Primera vez

    tipos_enviados = [l["tipo"] for l in logs]
    ultimo = logs[0]
    dias_desde_ultimo = (
        datetime.now(timezone.utc)
        - datetime.fromisoformat(ultimo["enviado_at"])
    ).days

    secuencia = ["email_1", "email_2", "email_3", "email_4"]
    for i, tipo in enumerate(secuencia):
        if tipo not in tipos_enviados:
            # Verificar espaciado: E1→E2: 2 días, E2→E3: 2 días, E3→E4: 2 días
            if dias_desde_ultimo >= 2:
                return i + 1
            else:
                return None  # Aún no es tiempo

    return None  # Secuencia completa


# ─── Generador de emails con Gemini ────────────────────────────────────────

async def _generar_email(numero: int, empresa: dict, stats: dict) -> dict:
    """Llama a Gemini para personalizar el email según el número en la secuencia."""

    nombre_contacto = (empresa.get("nombre_contacto") or "").strip().title() or "Estimado/a"
    nombre_empresa  = empresa.get("nombre", "su empresa")
    provincia       = empresa.get("provincia") or "República Dominicana"
    total_ofertas   = stats.get("total_ofertas", 0)
    ganadas         = stats.get("ofertas_ganadas", 0)
    monto_ganado    = stats.get("monto_ganado", 0)
    tasa            = _tasa(ganadas, total_ofertas)
    monto_fmt       = _fmt_monto(monto_ganado)

    # Potencial si mejora al 70%
    potencial_ganadas = math.ceil(total_ofertas * 0.70) if total_ofertas > 0 else 0
    mejora_estimada   = max(0, potencial_ganadas - ganadas)

    contextos = {
        1: f"""Eres un ejecutivo de ventas de LicitacionLab escribiendo el PRIMER email frío a una empresa.
Tu objetivo: abrir una conversación, NO vender directamente.
Datos de la empresa:
- Empresa: {nombre_empresa} ({provincia})
- Contacto: {nombre_contacto}
- Participaciones en licitaciones DGCP: {total_ofertas}
- Licitaciones ganadas: {ganadas} (tasa: {tasa})
- Monto ganado histórico: {monto_fmt}

Escribe un email corto (máx 150 palabras) que:
1. Mencione que tienes datos reales de su participación en el DGCP
2. Muestre que conoces su tasa de éxito actual ({tasa}) y que puede mejorar
3. Invite a una conversación de 15 minutos
4. Tono: profesional pero cercano, dominicano

Subject line: algo que llame la atención con datos reales
Formato JSON: {{"subject": "...", "cuerpo_html": "..."}}
El cuerpo_html debe tener HTML simple (p, strong, br) sin estilos inline complejos.
Firma siempre con: Lonny Antigua | LicitacionLab | {APP_URL}""",

        2: f"""Eres un ejecutivo de ventas de LicitacionLab escribiendo el SEGUNDO email de seguimiento.
La empresa NO respondió el primer email hace 2 días.
Datos:
- Empresa: {nombre_empresa}
- Contacto: {nombre_contacto}
- Participaciones: {total_ofertas} | Ganadas: {ganadas} | Tasa: {tasa}
- Monto ganado: {monto_fmt}

Este email debe mostrar licitaciones ACTIVAS AHORA relevantes que podrían interesarle.
Como no tienes datos en tiempo real, menciona que LicitacionLab está monitoreando
procesos activos de instituciones como MOPC, INAPA, CAASD, MIDE, que son las más activas.

Escribe un email (máx 180 palabras) que:
1. Haga seguimiento sin ser insistente
2. Muestre el valor de no perder licitaciones relevantes
3. Incluya un CTA claro para registrarse gratis

Formato JSON: {{"subject": "...", "cuerpo_html": "..."}}
Firma: Lonny Antigua | LicitacionLab | {APP_URL}""",

        3: f"""Eres un ejecutivo de ventas de LicitacionLab escribiendo el TERCER email.
La empresa lleva 4 días sin responder. Este email es el más importante: el argumento de ROI.
Datos:
- Empresa: {nombre_empresa}
- Contacto: {nombre_contacto}
- Tasa actual de éxito: {tasa} ({ganadas} de {total_ofertas})
- Con LicitacionLab, el 70% de las propuestas que preparamos GANAN
- Potencial: si hubieran ganado el 70% → {potencial_ganadas} contratos ganados
- Eso representa aproximadamente {mejora_estimada} contratos adicionales

Escribe un email (máx 200 palabras) que:
1. Muestre el contraste claro: su tasa actual vs 70% con LicitacionLab
2. Mencione que preparamos la propuesta técnica completa (Sobre A + Sobre B)
3. Sea directo sobre el ROI
4. CTA: agendar una llamada de 15 minutos

Formato JSON: {{"subject": "...", "cuerpo_html": "..."}}
Firma: Lonny Antigua | LicitacionLab | {APP_URL}""",

        4: f"""Eres un ejecutivo de ventas de LicitacionLab escribiendo el CUARTO y ÚLTIMO email.
La empresa no respondió ninguno de los 3 anteriores. Este es el cierre.
Datos:
- Empresa: {nombre_empresa}
- Contacto: {nombre_contacto}

Escribe un email corto (máx 120 palabras) que:
1. Sea honesto: diga que es el último email que enviarás
2. Deje la puerta abierta para el futuro
3. Ofrezca registrarse gratis sin compromiso
4. Tono: amigable, sin presión

Formato JSON: {{"subject": "...", "cuerpo_html": "..."}}
Firma: Lonny Antigua | LicitacionLab | {APP_URL}"""
    }

    prompt = contextos[numero]

    async with httpx.AsyncClient(timeout=30) as client:
        resp = await client.post(
            f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash:generateContent?key={GEMINI_API_KEY}",
            json={"contents": [{"parts": [{"text": prompt}]}]}
        )
        resp.raise_for_status()
        text = resp.json()["candidates"][0]["content"]["parts"][0]["text"]

    # Limpiar markdown
    text = text.strip().removeprefix("```json").removeprefix("```").removesuffix("```").strip()
    return json.loads(text)


# ─── Envío por Resend ───────────────────────────────────────────────────────

async def _enviar_resend(to: str, subject: str, html: str) -> bool:
    """Envía el email via Resend. Retorna True si fue exitoso."""
    # Wrapper HTML completo con estilos de LicitacionLab
    html_full = f"""<!DOCTYPE html>
<html lang="es">
<head><meta charset="UTF-8"><meta name="viewport" content="width=device-width, initial-scale=1.0"></head>
<body style="margin:0;padding:0;background:#f1f5f9;font-family:Arial,sans-serif;">
  <table width="100%" cellpadding="0" cellspacing="0" style="background:#f1f5f9;padding:32px 0;">
    <tr><td align="center">
      <table width="600" cellpadding="0" cellspacing="0" style="background:#ffffff;border-radius:12px;overflow:hidden;box-shadow:0 2px 8px rgba(0,0,0,0.08);">
        <!-- Header -->
        <tr>
          <td style="background:#0D1B2A;padding:24px 32px;">
            <span style="color:#2ECC71;font-size:22px;font-weight:800;letter-spacing:-0.5px;">Licitacion<span style="color:#ffffff;">Lab</span></span>
          </td>
        </tr>
        <!-- Body -->
        <tr>
          <td style="padding:32px;color:#1e293b;font-size:14px;line-height:1.7;">
            {html}
          </td>
        </tr>
        <!-- CTA -->
        <tr>
          <td align="center" style="padding:0 32px 32px;">
            <a href="{APP_URL}" style="display:inline-block;background:#2ECC71;color:#0D1B2A;text-decoration:none;padding:14px 32px;border-radius:8px;font-size:14px;font-weight:800;">
              Ver LicitacionLab →
            </a>
          </td>
        </tr>
        <!-- Footer -->
        <tr>
          <td style="background:#f8fafc;padding:20px 32px;border-top:1px solid #e2e8f0;">
            <p style="margin:0;font-size:12px;color:#94a3b8;text-align:center;">
              LicitacionLab · República Dominicana<br>
              Si ya eres cliente o no deseas recibir más emails, responde con <strong>STOP</strong>
            </p>
          </td>
        </tr>
      </table>
    </td></tr>
  </table>
</body>
</html>"""

    async with httpx.AsyncClient(timeout=20) as client:
        resp = await client.post(
            "https://api.resend.com/emails",
            headers={"Authorization": f"Bearer {RESEND_API_KEY}", "Content-Type": "application/json"},
            json={"from": FROM_EMAIL, "to": [to], "subject": subject, "html": html_full}
        )
        return resp.status_code in (200, 201)


# ─── Endpoints ──────────────────────────────────────────────────────────────

@router.post("/disparar")
async def disparar_outreach(
    payload: dict,
    x_agent_secret: str = Header(default="")
):
    """
    Disparo manual desde Telegram.
    Body: { "cantidad": 50 }
    Toma N empresas no contactadas, les envía el email correspondiente en la secuencia.
    """
    _auth(x_agent_secret)
    cantidad = int(payload.get("cantidad", 30))

    # 1. Traer empresas con email que NO hayan completado la secuencia
    #    y que no sean ya usuarios de la app
    empresas_raw = (
        supabase.table("empresas_estado")
        .select("id, rnc, nombre, correo_comercial, nombre_contacto, posicion_contacto, provincia, total_contratos, monto_total")
        .not_.is_("correo_comercial", "null")
        .neq("correo_comercial", "")
        .order("total_contratos", desc=True)
        .limit(cantidad * 3)  # traemos más para filtrar
        .execute()
        .data or []
    )

    # 2. Filtrar las que ya completaron secuencia o aún no es tiempo
    cola = []
    for emp in empresas_raw:
        if len(cola) >= cantidad:
            break
        email = emp.get("correo_comercial", "").strip()
        if not email:
            continue
        if _ya_es_usuario(email):
            continue
        num = _numero_siguiente_email(emp["id"])
        if num is None:
            continue
        cola.append({"empresa": emp, "numero_email": num})

    if not cola:
        return {"ok": False, "mensaje": "No hay empresas disponibles para outreach ahora mismo"}

    # 3. Procesar cada empresa
    enviados = 0
    errores  = 0
    detalle  = []

    for item in cola:
        emp    = item["empresa"]
        num    = item["numero_email"]
        email  = emp["correo_comercial"].strip()
        rnc    = emp["rnc"]

        try:
            stats  = _stats_empresa(rnc)
            result = await _generar_email(num, emp, stats)
            subject = result["subject"]
            html    = result["cuerpo_html"]

            ok = await _enviar_resend(email, subject, html)

            if ok:
                # Registrar en outreach_log
                supabase.table("outreach_log").insert({
                    "prospecto_id": emp["id"],
                    "canal": "email",
                    "tipo": f"email_{num}",
                    "asunto": subject,
                    "cuerpo": html,
                    "enviado_at": datetime.now(timezone.utc).isoformat(),
                    "respondio": False,
                    "convirtio": False,
                    "hook_usado": f"secuencia_email_{num}"
                }).execute()
                enviados += 1
                detalle.append(f"✅ {emp['nombre'][:35]} → E{num}")
            else:
                errores += 1
                detalle.append(f"❌ {emp['nombre'][:35]} → fallo Resend")

        except Exception as e:
            errores += 1
            detalle.append(f"⚠️ {emp['nombre'][:35]} → {str(e)[:60]}")

    return {
        "ok": True,
        "enviados": enviados,
        "errores": errores,
        "detalle": detalle[:50]
    }


@router.post("/marcar_convertido")
async def marcar_convertido(
    payload: dict,
    x_agent_secret: str = Header(default="")
):
    """
    Marca una empresa como convertida para parar la secuencia.
    Body: { "empresa_id": "uuid" }  o  { "email": "xxx@yyy.com" }
    """
    _auth(x_agent_secret)

    empresa_id = payload.get("empresa_id")
    email      = payload.get("email")

    if email and not empresa_id:
        res = supabase.table("empresas_estado").select("id").eq("correo_comercial", email).execute()
        if res.data:
            empresa_id = res.data[0]["id"]

    if not empresa_id:
        return {"ok": False, "mensaje": "No se encontró la empresa"}

    supabase.table("outreach_log").update({"convirtio": True}).eq("prospecto_id", empresa_id).execute()
    return {"ok": True, "mensaje": "Empresa marcada como convertida"}


@router.get("/estado")
async def estado_outreach(x_agent_secret: str = Header(default="")):
    """Resumen del estado del pipeline de outreach para Telegram."""
    _auth(x_agent_secret)

    total_empresas = supabase.table("empresas_estado").select("id", count="exact").not_.is_("correo_comercial", "null").execute().count or 0
    total_logs     = supabase.table("outreach_log").select("id", count="exact").execute().count or 0
    convertidos    = supabase.table("outreach_log").select("id", count="exact").eq("convirtio", True).execute().count or 0

    # Emails por tipo
    por_tipo = {}
    for tipo in ["email_1", "email_2", "email_3", "email_4"]:
        c = supabase.table("outreach_log").select("id", count="exact").eq("tipo", tipo).execute().count or 0
        por_tipo[tipo] = c

    return {
        "total_empresas_con_email": total_empresas,
        "total_enviados": total_logs,
        "convertidos": convertidos,
        "por_tipo": por_tipo,
        "pendientes_sin_contactar": total_empresas - (por_tipo.get("email_1", 0))
    }
