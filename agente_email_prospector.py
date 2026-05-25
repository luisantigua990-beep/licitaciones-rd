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
BATCH_DEFAULT      = 80    # Total por batch: 40 construcción + 40 bienes/servicios
BATCH_CONSTRUCCION = 70
BATCH_BIENES       = 15
EMAILS_EXCLUIDOS   = ['conser@conser.com.do']  # Clientes activos — no contactar
EMPRESAS_EXCLUIDAS = ['conser, srl', 'conser srl']  # por nombre (lowercase)

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
        # Año del primer contrato registrado (los contratos vienen desc, el último es el más antiguo)
        primer_fecha = contratos[-1].get("fecha_adjudicacion") or ""
        perfil["primer_contrato_anio"] = primer_fecha[:4] if primer_fecha else "2024"

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
            .select("estado_oferta, estado_evaluacion, valor_oferta, unidad_compra, fecha_creacion") \
            .eq("rpe", str(rnc)) \
            .execute()
        ofertas = ro.data or []
    except Exception:
        ofertas = []

    # Total histórico de participaciones (desde 2015)
    perfil["total_ofertas"] = len(ofertas)

    # Participaciones recientes desde 2024 (período con datos de adjudicación)
    ofertas_2024 = [
        o for o in ofertas
        if (o.get("fecha_creacion") or "") >= "2024-01-01"
    ]
    perfil["total_ofertas_2024"] = len(ofertas_2024)

    # Año más antiguo de participación
    fechas = [o.get("fecha_creacion","") for o in ofertas if o.get("fecha_creacion")]
    perfil["anio_primera_participacion"] = fechas[-1][:4] if fechas else "2017"

    perfil["total_perdidas"] = len([
        o for o in ofertas
        if "descalif" in (o.get("estado_evaluacion") or "").lower()
        or "no adjudic" in (o.get("estado_oferta") or "").lower()
    ])

    # Instituciones donde participó sin ganar (para email_2)
    perfil["instituciones_sin_ganar"] = list({
        o.get("unidad_compra") for o in ofertas
        if o.get("unidad_compra") and o.get("estado_oferta") != "Aprobada"
    })[:5]

    # Proceso activo relevante para Email 2
    try:
        instituciones_empresa = [
            o.get('unidad_compra') for o in ofertas if o.get('unidad_compra')
        ]
        if instituciones_empresa:
            from datetime import datetime as _dt
            inst_top = max(set(instituciones_empresa), key=instituciones_empresa.count)
            rp = supabase.table('procesos') \
                .select('titulo, unidad_compra, monto_estimado, fecha_fin_recepcion_ofertas') \
                .eq('estado_proceso', 'Proceso publicado') \
                .ilike('unidad_compra', f'%{inst_top[:20]}%') \
                .gte('fecha_fin_recepcion_ofertas', _dt.now().isoformat()) \
                .order('fecha_fin_recepcion_ofertas') \
                .limit(3) \
                .execute()
            perfil['procesos_activos_relevantes'] = rp.data if rp.data else []
            # Mantener compatibilidad con código que usa el singular
            perfil['proceso_activo_relevante'] = rp.data[0] if rp.data else None
        else:
            perfil['proceso_activo_relevante'] = None
    except Exception:
        perfil['proceso_activo_relevante'] = None


    # Competidores que le ganaron (cruce ofertas_procesos vs contratos_adjudicados)
    try:
        rc = supabase.rpc("get_competidores_que_ganaron", {"p_rnc": str(rnc), "p_empresa_id": empresa_id}).execute()
        if rc.data:
            perfil["competidores_que_ganaron"] = rc.data[:3]
        else:
            raise Exception("RPC no disponible")
    except Exception:
        # Query directo
        try:
            sql_result = supabase.table("contratos_adjudicados")                 .select("empresa_id, monto_adjudicado, codigo_proceso")                 .execute()
            # Fallback simplificado sin RPC
            perfil["competidores_que_ganaron"] = []
        except Exception:
            perfil["competidores_que_ganaron"] = []

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
    "email_1": """INSTRUCCIÓN CRÍTICA — OBLIGATORIA: Escribe SIEMPRE el email sin excepción. Nunca opines sobre los datos, nunca expliques limitaciones, nunca preguntes, nunca justifiques. Tu única función es producir el párrafo del email.

IMPORTANTE: Al final de los datos verás una sección "ESTRATEGIA DE COMUNICACIÓN PARA ESTE EMAIL" — úsala para determinar el ángulo del hook. Si dice ALTO RATIO, NO menciones el 70% como ventaja; habla de volumen y capacidad. Si dice BAJO RATIO, enfócate en la brecha de contratos perdidos. Si dice RATIO MEDIO, habla de mejora incremental.

Eres el Ing. Luis Antigua, fundador de LicitacionLab, escribiendo un email de prospección B2B en español formal dominicano.

{datos}

REGLA DE DATOS: Usa ÚNICAMENTE los datos anteriores. Si un dato dice "N/D" o "datos no disponibles", no lo menciones. No inventes nada.

Escribe SOLO el párrafo central (3-4 oraciones):
1. Preséntate: "Soy el Ing. Luis Antigua, fundador de LicitacionLab — plataforma de inteligencia para licitaciones públicas dominicanas que prepara propuestas completas (Sobre A y Sobre B)."
2. Historial real: participaciones desde qué año + desde 2024 contratos ganados con institución y monto exacto
3. Si hay competidor real que les ganó, nómbralo — luego: "Llevamos años preparando propuestas para esas instituciones y sabemos lo que buscan — cosas que no siempre están escritas en el pliego"
4. Sin prometer: 70% de adjudicación + cuántos contratos adicionales habrían representado con ese ratio
5. CTA: "Escríbanos por WhatsApp" — sin precios

Máximo 130 palabras. NUNCA cortes una oración a la mitad — siempre completa el pensamiento. NO incluyas saludo ni firma.
Tono: consultor que ya revisó el expediente, autoridad, genera curiosidad.""",

    "email_1_bienes_servicios": """INSTRUCCIÓN CRÍTICA — OBLIGATORIA: Escribe SIEMPRE el email sin excepción. Nunca opines sobre los datos, nunca expliques limitaciones, nunca preguntes, nunca justifiques. Tu única función es producir el párrafo del email.

IMPORTANTE: Al final de los datos verás una sección "ESTRATEGIA DE COMUNICACIÓN PARA ESTE EMAIL" — úsala para determinar el ángulo del hook. Si dice ALTO RATIO, NO menciones el 70% como ventaja; habla de volumen y capacidad. Si dice BAJO RATIO, enfócate en la brecha de contratos perdidos. Si dice RATIO MEDIO, habla de mejora incremental.

Eres el Ing. Luis Antigua, fundador de LicitacionLab, escribiendo un email de prospección B2B en español formal dominicano a una empresa de bienes o servicios.

{datos}

REGLA DE DATOS: Usa ÚNICAMENTE los datos anteriores. No inventes nada.

Escribe SOLO el párrafo central (3-4 oraciones):
1. Preséntate: "Soy el Ing. Luis Antigua, fundador de LicitacionLab — plataforma de inteligencia para licitaciones públicas dominicanas que prepara propuestas completas (Sobre A y Sobre B)."
2. Historial real: participaciones desde qué año + desde 2024 contratos ganados con institución y monto exacto
3. Si hay competidor real que les ganó, nómbralo — luego: "Conocemos lo que estas instituciones buscan en una propuesta — cosas que no siempre están escritas en el pliego"
4. Sin prometer: 70% de adjudicación + cuántos contratos adicionales habrían representado
5. CTA: "Escríbanos por WhatsApp"

Máximo 130 palabras. NUNCA cortes una oración a la mitad — siempre completa el pensamiento. NO incluyas saludo ni firma.
Tono: consultor con autoridad, directo, genera curiosidad.""",

    "email_1_construccion": """INSTRUCCIÓN CRÍTICA — OBLIGATORIA: Escribe SIEMPRE el email sin excepción. Nunca opines sobre los datos, nunca expliques limitaciones, nunca preguntes, nunca justifiques. Tu única función es producir el párrafo del email.

IMPORTANTE: Al final de los datos verás una sección "ESTRATEGIA DE COMUNICACIÓN PARA ESTE EMAIL" — úsala para determinar el ángulo del hook. Si dice ALTO RATIO, NO menciones el 70% como ventaja; habla de volumen y capacidad. Si dice BAJO RATIO, enfócate en la brecha de contratos perdidos. Si dice RATIO MEDIO, habla de mejora incremental.

Eres el Ing. Luis Antigua, fundador de LicitacionLab, escribiendo un email de prospección B2B a una empresa constructora dominicana.

{datos}

REGLA DE DATOS: Usa ÚNICAMENTE los datos anteriores. No inventes nada.

Escribe SOLO el párrafo central (3-4 oraciones):
1. Preséntate: "Soy el Ing. Luis Antigua, fundador de LicitacionLab — plataforma de inteligencia para licitaciones públicas dominicanas que prepara propuestas completas (Sobre A y Sobre B)."
2. Historial real: participaciones desde qué año + desde 2024 institución exacta (MOPC, CAASD, MIVHED, etc.), tipo de obra y monto
3. Si hay competidor constructor real que les ganó, nómbralo — luego: "El Sobre A y el Sobre B tienen que contar una historia coherente con lo que el pliego realmente busca — y eso es algo que sabemos construir"
4. Sin prometer: 70% de adjudicación + cuántos contratos adicionales de obras habrían representado
5. CTA: "Escríbanos por WhatsApp"

Máximo 130 palabras. NUNCA cortes una oración a la mitad — siempre completa el pensamiento. NO incluyas saludo ni firma.
Tono: colega constructor con autoridad técnica, directo.""",

    "email_2": """INSTRUCCIÓN CRÍTICA — OBLIGATORIA: Escribe SIEMPRE el email sin excepción. Nunca opines sobre los datos, nunca expliques limitaciones, nunca preguntes, nunca justifiques por qué no puedes escribir. Si los datos son limitados, escribe con lo que hay. Tu única función es producir el párrafo del email.

Eres el Ing. Luis Antigua, fundador de LicitacionLab, escribiendo el SEGUNDO email de seguimiento en español formal dominicano.
La empresa no respondió el primer email hace 4 días.

{datos}

REGLA DE DATOS: Usa ÚNICAMENTE los datos anteriores. No inventes nada.

Escribe SOLO el párrafo central (3-4 oraciones):
1. "Le escribí hace unos días sobre el historial de [empresa] en el DGCP."
2. Si hay procesos activos en los datos, menciona 2 o 3 brevemente: institución real, descripción corta, monto estimado y fecha límite real de cada uno
3. "En LicitacionLab preparamos la propuesta completa — el cliente solo firma y entrega. Escríbanos por WhatsApp."

Máximo 100 palabras. NO incluyas saludo ni firma.
Tono: directo, sin presión, enfocado en las oportunidades concretas.""",

    "email_3": """INSTRUCCIÓN CRÍTICA — OBLIGATORIA: Escribe SIEMPRE el email sin excepción. Nunca opines sobre los datos, nunca expliques limitaciones, nunca preguntes, nunca justifiques por qué no puedes escribir. Si los datos son limitados, escribe con lo que hay. Tu única función es producir el párrafo del email.

Eres el Ing. Luis Antigua, fundador de LicitacionLab, escribiendo el TERCER email de seguimiento en español formal dominicano.
La empresa lleva 8 días sin responder.

{datos}

REGLA DE DATOS: Usa ÚNICAMENTE los datos anteriores. No inventes nada.

Escribe SOLO el párrafo central (3 oraciones):
1. Directo al número: cuántas participaciones desde 2024, cuántas ganaron, cuánto promedio por contrato ganado (usa monto promedio real)
2. El contraste: con 70% de adjudicación habrían ganado ~X contratos adicionales — eso es aproximadamente RD$X en ingresos que se fueron a otras empresas (usa el dato "Ingresos estimados dejados sobre la mesa")
3. "Esa brecha es recuperable. Escríbanos por WhatsApp."

Máximo 80 palabras. NO incluyas saludo ni firma.
Tono: números concretos, sin drama, directo.""",

    "email_4": """INSTRUCCIÓN CRÍTICA — OBLIGATORIA: Escribe SIEMPRE el email sin excepción. Nunca opines sobre los datos, nunca expliques limitaciones, nunca preguntes, nunca justifiques por qué no puedes escribir. Si los datos son limitados, escribe con lo que hay. Tu única función es producir el párrafo del email.

Eres el Ing. Luis Antigua, fundador de LicitacionLab, escribiendo el CUARTO y último email en español formal dominicano.
La empresa lleva 14 días sin responder.

{datos}

REGLA DE DATOS: Usa ÚNICAMENTE los datos anteriores. No inventes nada.

Escribe SOLO el párrafo central (2-3 oraciones):
1. "Este es el último correo que le enviaremos."
2. Menciona las instituciones reales donde han participado y que seguirán publicando procesos
3. "Cuando quieran mejorar su ratio de adjudicación en el DGCP, aquí estaremos. Escríbanos por WhatsApp."

Máximo 70 palabras. NO incluyas saludo ni firma.
Tono: amigable, sin presión, genuino, cierre limpio.""",
}

def _asunto_email_1(nombre, perfil):
    total_historico = perfil.get('total_ofertas', 0) or 0
    total_2024      = perfil.get('total_ofertas_2024', 0) or 0
    contratos       = perfil.get('total_contratos', 0) or 0
    anio            = perfil.get('anio_primera_participacion', '2017')
    if total_2024 > 0 and contratos == 0:
        return f"{nombre} — {total_2024} participaciones desde 2024 sin adjudicación"
    elif total_2024 > 0 and contratos > 0:
        tasa = round(contratos / total_2024 * 100)
        if tasa < 20:
            return f"{nombre}: {total_2024} procesos desde 2024 — y solo {contratos} contratos ganados"
        return f"{nombre} — revisamos sus {total_historico} participaciones en el DGCP"
    else:
        return f"{nombre} — revisamos su historial en el DGCP desde {anio}"

def _asunto_email_2(nombre, perfil):
    procesos = perfil.get('procesos_activos_relevantes') or []
    if not procesos and perfil.get('proceso_activo_relevante'):
        procesos = [perfil['proceso_activo_relevante']]
    if procesos:
        n = len(procesos)
        inst = procesos[0].get('unidad_compra', '').split('(')[0].strip()[:30]
        fecha = (procesos[0].get('fecha_fin_recepcion_ofertas') or '')[:10]
        if n >= 2:
            return f"{nombre} — {n} procesos activos en sus instituciones esta semana"
        return f"{nombre} — proceso abierto en {inst} cierra el {fecha}"
    insts = perfil.get('instituciones_top', [])
    if insts:
        inst = insts[0]['nombre'].split('(')[0].strip()[:35]
        return f"{nombre} — hay procesos activos en {inst} ahora mismo"
    return f"{nombre} — hay licitaciones abiertas en sus instituciones"

def _asunto_email_3(nombre, perfil):
    total_2024  = perfil.get('total_ofertas_2024', 0) or 0
    ganadas     = perfil.get('total_contratos', 0) or 0
    monto_total = perfil.get('monto_total', 0) or 0
    if total_2024 > 0 and ganadas > 0:
        monto_prom  = monto_total / ganadas
        adicionales = max(0, round(total_2024 * 0.70) - ganadas)
        dejado      = adicionales * monto_prom
        if dejado > 0:
            return f"{nombre} — ~RD${int(dejado/1_000_000)}M en contratos que se fueron a otras empresas"
    elif total_2024 > 0:
        return f"{nombre} — {total_2024} participaciones desde 2024, calculamos el costo real"
    return f"{nombre} — el costo real de no ganar licitaciones"

def _asunto_email_4(nombre, perfil):
    return f"{nombre} — último mensaje de nuestra parte"

ASUNTOS_EMAIL = {
    "email_1": _asunto_email_1,
    "email_2": _asunto_email_2,
    "email_3": _asunto_email_3,
    "email_4": _asunto_email_4,
}


def generar_cuerpo_claude(nombre: str, perfil: dict, tipo_email: str, sector: str = "bienes_servicios") -> str:
    """Llama a Claude para generar el párrafo personalizado del email."""
    inst_str = ", ".join([
        f"{i['nombre']} ({i['contratos']} contratos)"
        for i in perfil.get("instituciones_top", [])
    ]) or "varias instituciones del Estado"

    # Calcular tasa de éxito real
    total_ofertas  = perfil.get('total_ofertas', 0) or 0
    contratos_gan  = perfil.get('total_contratos', 0) or 0
    monto_total    = perfil.get('monto_total', 0) or 0

    if total_ofertas > 0:
        tasa_real = round(contratos_gan / total_ofertas * 100)
        tasa_str  = f"{tasa_real}% ({contratos_gan} ganadas de {total_ofertas} participaciones)"
    else:
        tasa_str = "sin datos de participaciones recientes"

    # Potencial si usara nuestro servicio (70%)
    if total_ofertas > 0:
        potencial_ganadas  = round(total_ofertas * 0.70)
        mejora_contratos   = max(0, potencial_ganadas - contratos_gan)
        monto_promedio     = monto_total / contratos_gan if contratos_gan > 0 else 150000
        ingreso_potencial  = _fmt_monto(mejora_contratos * monto_promedio)
        potencial_str = (
            f"Con nuestra tasa del 70%, de {total_ofertas} participaciones "
            f"habría ganado ~{potencial_ganadas} contratos "
            f"(+{mejora_contratos} adicionales, ~{ingreso_potencial} en ingresos adicionales estimados)"
        )
    else:
        potencial_str = "potencial de mejora significativo con consultoría especializada"

    # Construir texto de competidores
    competidores = perfil.get("competidores_que_ganaron", [])
    if competidores:
        comp_str = ", ".join([
            f"{c.get('competidor', 'N/D')} (les ganó {c.get('procesos_ganados_contra_mi', 0)} veces)"
            for c in competidores
        ])
    else:
        comp_str = "datos no disponibles"

    # Período de participaciones
    anio_inicio = perfil.get("anio_primera_participacion", "2017")
    total_historico = perfil.get("total_ofertas", 0) or 0
    total_2024 = perfil.get("total_ofertas_2024", 0) or 0

    # Tasa solo sobre el período con datos completos (desde 2024)
    if total_2024 > 0 and contratos_gan > 0:
        tasa_2024 = round(contratos_gan / total_2024 * 100)
        tasa_str = f"{tasa_2024}% ({contratos_gan} ganados de {total_2024} participaciones desde 2024)"
    elif total_2024 > 0:
        tasa_str = f"0% ({total_2024} participaciones desde 2024, ninguna adjudicada en ese período)"
    else:
        tasa_str = "sin participaciones recientes registradas"

    # Potencial calculado sobre participaciones desde 2024
    if total_2024 > 0:
        potencial_ganadas = round(total_2024 * 0.70)
        mejora = max(0, potencial_ganadas - contratos_gan)
        monto_prom = monto_total / contratos_gan if contratos_gan > 0 else 150000
        potencial_str = (
            f"de {total_2024} participaciones recientes con nuestra tasa del 70% "
            f"habrían ganado ~{potencial_ganadas} contratos "
            f"(+{mejora} adicionales, ~{_fmt_monto(mejora * monto_prom)} estimados)"
        )
    else:
        potencial_str = "potencial de mejora significativo con consultoría especializada"

    # Procesos activos relevantes para email_2 (hasta 3)
    procesos_activos = perfil.get('procesos_activos_relevantes') or []
    if not procesos_activos and perfil.get('proceso_activo_relevante'):
        procesos_activos = [perfil['proceso_activo_relevante']]

    if procesos_activos:
        lineas = []
        for i, p in enumerate(procesos_activos[:3], 1):
            fecha_lim  = p.get('fecha_fin_recepcion_ofertas', '')[:10]
            monto_proc = _fmt_monto(p.get('monto_estimado') or 0)
            titulo     = p.get('titulo', 'N/D')[:70]
            inst       = p.get('unidad_compra', 'N/D')
            lineas.append(f"{i}. {titulo} | {inst} | {monto_proc} | Cierra: {fecha_lim}")
        proceso_str = "\n".join(lineas)
    else:
        proceso_str = "datos no disponibles"

    # Monto promedio real por contrato ganado
    monto_prom_real = monto_total / contratos_gan if contratos_gan > 0 else 0
    ingresos_perdidos = max(0, round(total_2024 * 0.70) - contratos_gan) * monto_prom_real

    # Perfil de la empresa según tasa de adjudicación
    if total_2024 > 0 and contratos_gan >= 0:
        tasa_pct = round(contratos_gan / total_2024 * 100) if total_2024 > 0 else 0
        if tasa_pct < 50:
            perfil_hook = (
                f"PERFIL: BAJO RATIO ({tasa_pct}%) — "
                "El hook principal es que están perdiendo contratos y algo en las propuestas está fallando. "
                "NO menciones el 70% como meta — menciona la brecha: cuántos contratos adicionales habrían ganado. "
                "Tono: señalar el problema con autoridad, sin atacar."
            )
        elif tasa_pct < 70:
            perfil_hook = (
                f"PERFIL: RATIO MEDIO ({tasa_pct}%) — "
                "Tienen buen historial pero hay margen de mejora. "
                "El hook es que ya van bien y con nuestra metodología pueden llegar al 70%. "
                "Tono: reconocer su buen desempeño y proponer mejora incremental."
            )
        else:
            perfil_hook = (
                f"PERFIL: ALTO RATIO ({tasa_pct}%) — "
                "Su tasa supera o iguala la nuestra. NO menciones el 70% como ventaja — sería contraproducente. "
                "El hook es VOLUMEN y CAPACIDAD: ya saben ganar, el problema es cuántos procesos pueden preparar al mismo tiempo. "
                "Argumento: con LicitacionLab pueden participar en más procesos sin aumentar su equipo interno. "
                "Tono: reconocer su excelente tasa y hablar de escalar."
            )
    else:
        perfil_hook = "PERFIL: SIN DATOS SUFICIENTES — usa el hook general de preparación de propuestas."

    datos_str = f"""
DATOS REALES DE LA EMPRESA — usar SOLO estos, no inventar nada. Si dice "N/D" o "datos no disponibles", no lo menciones.

EMPRESA:
- Nombre: {nombre}
- Contacto: {perfil.get('nombre_contacto') or nombre}
- Cargo: {perfil.get('posicion_contacto') or 'N/D'}
- Provincia: {perfil.get('provincia') or 'República Dominicana'}

HISTORIAL COMPLETO EN DGCP (desde {anio_inicio}):
- Total de participaciones registradas: {total_historico}
- Primer año de participación: {anio_inicio}

PERÍODO RECIENTE — datos de adjudicación disponibles desde marzo 2024:
- Participaciones desde 2024: {total_2024}
- Contratos adjudicados desde 2024: {contratos_gan}
- Monto total adjudicado desde 2024: {_fmt_monto(monto_total)}
- Monto promedio por contrato ganado: {_fmt_monto(monto_prom_real)}
- Tasa de éxito en período reciente: {tasa_str}
- Instituciones donde ganó desde 2024: {inst_str if inst_str != "varias instituciones del Estado" else "datos no disponibles"}
- Competidores que les ganaron en esos mismos procesos recientes: {comp_str}

PROCESO ACTIVO RELEVANTE (para mencionar en email_2):
- {proceso_str}

OPORTUNIDAD:
- {potencial_str}
- Ingresos estimados dejados sobre la mesa: {_fmt_monto(ingresos_perdidos)} (promedio real × contratos adicionales)

ESTRATEGIA DE COMUNICACIÓN PARA ESTE EMAIL:
{perfil_hook}
"""
    # Seleccionar prompt según sector para email_1
    if tipo_email == "email_1":
        key = f"email_1_{sector}" if f"email_1_{sector}" in PROMPTS_EMAIL else "email_1"
    else:
        key = tipo_email
    prompt = PROMPTS_EMAIL[key].format(datos=datos_str)

    try:
        resp = claude.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=300,
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
    part_2024   = perfil.get("total_ofertas_2024", 0) or 0
    inst_tags   = ""
    for inst in perfil.get("instituciones_top", [])[:4]:
        inst_tags += (
            f'<span style="display:inline-block;background:#edf7f0;color:#1a5c2a;'
            f'border:1px solid #b8e0c5;border-radius:20px;padding:4px 12px;'
            f'font-size:12px;font-weight:500;margin:3px 3px 3px 0;">'
            f'{inst["nombre"]} — {inst["contratos"]} contratos</span>'
        )

    import base64
    token     = base64.urlsafe_b64encode(empresa_id.encode()).decode()
    unsub_url = f"{APP_URL}/unsub?t={token}"

    # Mensaje WhatsApp pre-cargado personalizado
    nombre_enc = nombre.replace(' ', '%20').replace(',', '%2C').replace('&', '%26')[:60]
    ws_text    = f"Hola%20Ing.%20Luis%2C%20recib%C3%AD%20su%20correo%20sobre%20{nombre_enc}%20y%20me%20interesa%20conversar."
    ws_url     = f"{CONSULTING_URL}?text={ws_text}"

    # Email 4 va sin header verde (más personal)
    if tipo_email == "email_4":
        header_html = ""
        saludo_style = "font-size:15px;color:#2c2c2c;line-height:1.7;margin:0 0 20px;"
    else:
        # Header dinámico según perfil
        tasa_header = round(contratos / part_2024 * 100) if part_2024 > 0 else 0
        competidor_top = ""
        comps = perfil.get("competidores_que_ganaron", [])
        if comps:
            competidor_top = comps[0].get("competidor", "").split(",")[0].strip()[:30]

        if tasa_header >= 70:
            # Alto ratio — hook de volumen
            linea1 = f"{nombre}:"
            linea2 = f"<span style='color:#7fe89f;'>excelente tasa</span> —"
            linea3 = "¿cuántos procesos más podrían ganar?"
        elif tasa_header >= 50:
            # Ratio medio — hook de mejora
            comp_str = f"competidores como {competidor_top}" if competidor_top else "otras empresas"
            linea1 = f"{nombre}:"
            linea2 = f"<span style='color:#7fe89f;'>{tasa_header}% de adjudicación</span> —"
            linea3 = f"podría llegar al 70%"
        elif part_2024 > 0 and contratos == 0:
            # Cero ganados
            linea1 = f"{nombre}:"
            linea2 = f"<span style='color:#7fe89f;'>{part_2024} participaciones</span>"
            linea3 = "desde 2024 sin ningún contrato ganado"
        elif competidor_top:
            # Bajo ratio con competidor conocido
            linea1 = f"{nombre}:"
            linea2 = f"<span style='color:#7fe89f;'>¿por qué {competidor_top}</span>"
            linea3 = "les sigue ganando?"
        else:
            # Bajo ratio genérico
            linea1 = f"{nombre}:"
            linea2 = f"<span style='color:#7fe89f;'>{part_2024} procesos desde 2024</span>"
            linea3 = f"y solo {contratos} contratos ganados"

        header_html = f"""
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
            {linea1}<br>{linea2}<br>{linea3}
          </div>
          <p style="color:rgba(255,255,255,0.7);font-size:14px;margin:10px 0 0;">Datos reales de su empresa en el portal de compras del Estado dominicano</p>
        </td>
      </tr>
    </table>
  </td></tr>"""
        saludo_style = "font-size:15px;color:#2c2c2c;line-height:1.7;margin:0 0 20px;"

    # Año para el label del historial
    primer_contrato_anio = perfil.get("primer_contrato_anio") or                            perfil.get("anio_primera_participacion") or "2024"

    # Calcular tasa de adjudicación para la data card
    tasa_card = "N/D"
    if part_2024 > 0:
        tasa_num  = round(contratos / part_2024 * 100)
        tasa_card = f"{tasa_num}%"
        tasa_color = "#a32d2d" if tasa_num < 30 else "#1a5c2a"
    else:
        tasa_color = "#777"

    # Data card y tags solo en email_1 y email_2
    if tipo_email in ("email_1", "email_1_construccion", "email_1_bienes_servicios"):
        data_section = f"""
    <!-- Data card -->
    <table width="100%" cellpadding="0" cellspacing="0" style="background:#f7f5f0;border-left:3px solid #1a5c2a;border-radius:0 6px 6px 0;margin-bottom:20px;">
      <tr><td style="padding:20px 24px;">
        <p style="font-size:11px;font-weight:600;letter-spacing:1px;text-transform:uppercase;color:#1a5c2a;margin:0 0 16px;">Su historial en el Estado dominicano desde {primer_contrato_anio}</p>
        <table width="100%" cellpadding="0" cellspacing="0">
          <tr>
            <td width="25%" align="center">
              <span style="font-family:Georgia,serif;font-size:26px;color:#1a5c2a;display:block;">{contratos}</span>
              <span style="font-size:12px;color:#777;">Contratos ganados</span>
            </td>
            <td width="25%" align="center">
              <span style="font-family:Georgia,serif;font-size:26px;color:#1a1a1a;display:block;">{monto_fmt}</span>
              <span style="font-size:12px;color:#777;">Monto adjudicado</span>
            </td>
            <td width="25%" align="center">
              <span style="font-family:Georgia,serif;font-size:26px;color:#1a5c2a;display:block;">{part_2024}</span>
              <span style="font-size:12px;color:#777;">Participaciones desde 2024</span>
            </td>
            <td width="25%" align="center">
              <span style="font-family:Georgia,serif;font-size:26px;color:{tasa_color};display:block;">{tasa_card}</span>
              <span style="font-size:12px;color:#777;">Tasa de adjudicación</span>
            </td>
          </tr>
        </table>
      </td></tr>
    </table>

    <!-- Tags de instituciones -->
    <div style="margin-bottom:28px;">
      {inst_tags}
    </div>

    <!-- Bloque social proof -->
    <table width="100%" cellpadding="0" cellspacing="0" style="background:#1a5c2a;border-radius:6px;margin-bottom:28px;">
      <tr>
        <td width="33%" align="center" style="padding:20px 12px;">
          <span style="font-family:Georgia,serif;font-size:22px;color:#ffffff;display:block;">70%</span>
          <span style="font-size:11px;color:rgba(255,255,255,0.65);display:block;margin-top:2px;">Tasa de adjudicación</span>
        </td>
        <td width="33%" align="center" style="padding:20px 12px;">
          <span style="font-family:Georgia,serif;font-size:13px;color:#ffffff;display:block;line-height:1.4;">Ing. Civil</span>
          <span style="font-family:Georgia,serif;font-size:13px;color:#ffffff;display:block;line-height:1.4;">Consultor de Licitaciones Públicas</span>
          <span style="font-size:11px;color:rgba(255,255,255,0.65);display:block;margin-top:4px;">Ing. Luis Antigua</span>
        </td>
        <td width="33%" align="center" style="padding:20px 12px;">
          <span style="font-family:Georgia,serif;font-size:13px;color:#ffffff;display:block;line-height:1.4;">MOPC · INAPA · CAASD</span>
          <span style="font-size:11px;color:rgba(255,255,255,0.65);display:block;margin-top:4px;">y otras instituciones</span>
        </td>
      </tr>
    </table>"""
    else:
        data_section = ""

    # Bloque visual de contraste para email_3
    if tipo_email == "email_3" and part_2024 > 0 and contratos > 0:
        tasa_real     = round(contratos / part_2024 * 100)
        adicionales   = max(0, round(part_2024 * 0.70) - contratos)
        t_color       = "#a32d2d" if tasa_real < 30 else "#1a5c2a"
        roi_section   = f"""
    <!-- Bloque contraste ROI -->
    <table width="100%" cellpadding="0" cellspacing="0" style="margin-bottom:28px;">
      <tr>
        <td width="48%" style="padding-right:8px;">
          <table width="100%" cellpadding="0" cellspacing="0" style="background:#fff5f5;border:1px solid #f5c6c6;border-radius:6px;">
            <tr><td style="padding:20px;text-align:center;">
              <p style="font-size:11px;font-weight:600;letter-spacing:1px;text-transform:uppercase;color:{t_color};margin:0 0 8px;">Su tasa actual</p>
              <span style="font-family:Georgia,serif;font-size:40px;color:{t_color};display:block;line-height:1;">{tasa_real}%</span>
              <span style="font-size:12px;color:#888;display:block;margin-top:4px;">{contratos} de {part_2024} procesos</span>
            </td></tr>
          </table>
        </td>
        <td width="4%"></td>
        <td width="48%" style="padding-left:8px;">
          <table width="100%" cellpadding="0" cellspacing="0" style="background:#edf7f0;border:1px solid #b8e0c5;border-radius:6px;">
            <tr><td style="padding:20px;text-align:center;">
              <p style="font-size:11px;font-weight:600;letter-spacing:1px;text-transform:uppercase;color:#1a5c2a;margin:0 0 8px;">Con LicitacionLab</p>
              <span style="font-family:Georgia,serif;font-size:40px;color:#1a5c2a;display:block;line-height:1;">70%</span>
              <span style="font-size:12px;color:#888;display:block;margin-top:4px;">~{adicionales} contratos adicionales</span>
            </td></tr>
          </table>
        </td>
      </tr>
    </table>"""
    else:
        roi_section = ""

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

  {header_html}

  <!-- BODY -->
  <tr><td style="padding:36px 40px;">

    <!-- Saludo -->
    <p style="{saludo_style}">
      Estimado <strong style="color:#1a5c2a;">{perfil.get('nombre_contacto') or ('equipo de ' + nombre)}</strong> —
    </p>

    <!-- Párrafo generado por IA -->
    <p style="font-size:15px;color:#3a3a3a;line-height:1.75;margin:0 0 28px;">
      {cuerpo_ia}
    </p>

    {data_section}

    {roi_section}

    <!-- CTA único WhatsApp -->
    <table width="100%" cellpadding="0" cellspacing="0" style="margin-bottom:28px;">
      <tr>
        <td align="center">
          <a href="{ws_url}"
             style="display:inline-block;background:#1a5c2a;color:#ffffff;text-decoration:none;text-align:center;padding:16px 48px;border-radius:6px;font-size:15px;font-weight:600;">
            Hablar con el consultor
          </a>
          <p style="font-size:12px;color:#999;margin:10px 0 0;">WhatsApp · Sin compromiso</p>
        </td>
      </tr>
    </table>

    <!-- Firma -->
    <table width="100%" cellpadding="0" cellspacing="0">
      <tr><td style="border-top:1px solid #ece9e3;padding-top:20px;">
        <strong style="display:block;font-size:15px;color:#1a1a1a;">Ing. Luis Antigua</strong>
        <span style="font-size:13px;color:#555;">Ing. Civil · Consultor de Licitaciones Públicas</span><br>
        <span style="font-size:13px;color:#888;">República Dominicana · +1 (809) 815-4457</span><br>
        <a href="https://www.instagram.com/licitacionlab/" target="_blank"
           style="display:inline-block;margin-top:10px;text-decoration:none;background:#f0ede8;border:1px solid #ddd9d2;border-radius:20px;padding:5px 14px;">
          <span style="font-size:12px;color:#1a1a1a;font-weight:600;">@licitacionlab</span>
          <span style="font-size:12px;color:#888;"> · Síguenos en Instagram</span>
        </a>
      </td></tr>
    </table>

  </td></tr>

  <!-- FOOTER -->
  <tr><td style="background:#f7f5f0;padding:20px 40px;border-top:1px solid #ece9e3;">
    <p style="font-size:11.5px;color:#aaa;line-height:1.6;text-align:center;margin:0;">
      Recibió este correo porque su empresa está registrada en el RPE de la DGCP.<br>
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


def obtener_empresas_elegibles_v2(limite: int = BATCH_DEFAULT, sector: str = None) -> list:
    """
    Usa la vista elegibles_email_v1. Si sector='construccion' o 'bienes_servicios', filtra.
    """
    try:
        q = supabase.table("elegibles_email_v1").select("id, nombre, rnc, email, sector")
        if sector:
            q = q.eq("sector", sector)
        result = q.limit(limite).execute()

        elegibles = []
        for emp in (result.data or []):
            elegibles.append({
                "id":          emp["id"],
                "nombre":      emp["nombre"],
                "rnc":         str(emp.get("rnc") or ""),
                "email":       emp.get("email"),
                "sector":      emp.get("sector", "bienes_servicios"),
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
    sector_forzado: str = None,
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

    # ── Batch 70/15: 70 construcción + 15 bienes/servicios ──
    if sector_forzado:
        elegibles = obtener_empresas_elegibles_v2(limite, sector=sector_forzado)
    else:
        emp_const  = obtener_empresas_elegibles_v2(BATCH_CONSTRUCCION, sector="construccion")
        emp_bienes = obtener_empresas_elegibles_v2(BATCH_BIENES, sector="bienes_servicios")
        # Intercalar para variedad
        elegibles = []
        for i in range(max(len(emp_const), len(emp_bienes))):
            if i < len(emp_const):
                elegibles.append(emp_const[i])
            if i < len(emp_bienes):
                elegibles.append(emp_bienes[i])
        elegibles = elegibles[:limite]

    # Excluir clientes activos
    elegibles = [
        e for e in elegibles
        if e.get('nombre', '').lower() not in EMPRESAS_EXCLUIDAS
        and e.get('email', '').lower() not in EMAILS_EXCLUIDOS
    ]

    if not elegibles:
        print("✅ Sin empresas elegibles en este momento.")
        return stats

    n_const  = sum(1 for e in elegibles if e.get("sector") == "construccion")
    n_bienes = sum(1 for e in elegibles if e.get("sector") == "bienes_servicios")
    print(f"📋 {len(elegibles)} empresas elegibles: {n_const} construcción | {n_bienes} bienes/servicios\n")

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
        sector_emp = emp.get("sector", "bienes_servicios")
        cuerpo = generar_cuerpo_claude(nombre, perfil, tipo, sector=sector_emp)

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

    @router.post("/agente6/enviar", dependencies=[Depends(verificar_admin)])
    def enviar_manual(
        rnc:    str = Query(None, description="RNC o RPE de la empresa"),
        nombre: str = Query(None, description="Nombre o parte del nombre de la empresa"),
        tipo:   str = Query("email_1", description="email_1 | email_2 | email_3 | email_4"),
        dry_run: bool = Query(False, description="Solo simular, no enviar"),
    ):
        """Envía un email manual a una empresa específica por RNC o nombre."""
        # Buscar empresa
        empresa = None
        if rnc:
            r = supabase.table("empresas_estado") \
                .select("id, nombre, rnc, correo_comercial, sector") \
                .eq("rnc", str(rnc)) \
                .limit(1).execute()
            if r.data:
                empresa = r.data[0]
        elif nombre:
            r = supabase.table("empresas_estado") \
                .select("id, nombre, rnc, correo_comercial, sector") \
                .ilike("nombre", f"%{nombre}%") \
                .limit(1).execute()
            if r.data:
                empresa = r.data[0]

        if not empresa:
            return {"ok": False, "mensaje": f"Empresa no encontrada con {'RNC ' + str(rnc) if rnc else 'nombre ' + nombre}"}

        email_dest = empresa.get("correo_comercial")
        if not email_dest:
            return {"ok": False, "mensaje": f"La empresa {empresa['nombre']} no tiene email registrado"}

        emp_nombre = empresa["nombre"]
        emp_rnc    = str(empresa.get("rnc") or "")
        emp_id     = empresa["id"]
        sector     = empresa.get("sector", "bienes_servicios")

        # Construir perfil y email
        perfil   = obtener_perfil_empresa(emp_id, emp_rnc)
        cuerpo   = generar_cuerpo_claude(emp_nombre, perfil, tipo, sector=sector)
        asunto   = ASUNTOS_EMAIL.get(tipo, ASUNTOS_EMAIL["email_1"])(emp_nombre, perfil)
        html     = construir_html_email(emp_nombre, perfil, cuerpo, tipo, emp_id)

        if dry_run:
            return {
                "ok":      True,
                "dry_run": True,
                "empresa": emp_nombre,
                "email":   email_dest,
                "asunto":  asunto,
                "cuerpo":  cuerpo,
            }

        resultado = enviar_email_resend(email_dest, emp_nombre, asunto, html)

        if resultado.get("ok"):
            # Registrar en outreach_log
            try:
                supabase.table("outreach_log").insert({
                    "prospecto_id": emp_id,
                    "canal":        "email",
                    "tipo":         tipo,
                    "asunto":       asunto,
                    "enviado_at":   datetime.utcnow().isoformat(),
                    "resend_id":    resultado.get("resend_id"),
                    "manual":       True,
                }).execute()
            except Exception:
                pass

        return {
            "ok":      resultado.get("ok", False),
            "empresa": emp_nombre,
            "email":   email_dest,
            "asunto":  asunto,
            "tipo":    tipo,
            "error":   resultado.get("error"),
        }

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
