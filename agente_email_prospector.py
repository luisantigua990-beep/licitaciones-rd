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
                .limit(1) \
                .execute()
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
    "email_1": """Eres Luis Antigua, ingeniero civil con 15+ años preparando propuestas para licitaciones públicas dominicanas.
Escribes un email de prospección B2B en español formal dominicano.

{datos}

REGLA ABSOLUTA: Usa ÚNICAMENTE los datos anteriores. Si un dato dice "N/D" o "datos no disponibles", NO lo menciones. No inventes nombres, cifras ni instituciones.

Escribe SOLO el párrafo central del email (3-4 oraciones):
1. Historial completo: cuántas participaciones desde qué año
2. Período reciente (2024): contratos ganados con institución y monto real — si hay competidor real que les ganó, nómbralo
3. "Llevamos años preparando propuestas para esas instituciones y conocemos lo que buscan — cosas que no siempre están escritas en el pliego"
4. 70% de adjudicación sin prometer nada, cuántos contratos adicionales habrían representado
5. CTA: escríbenos por WhatsApp — sin precios ni app

Máximo 100 palabras. NO incluyas saludo ni firma.
Tono: consultor que revisó el expediente, directo, autoridad, genera curiosidad.""",

    "email_1_bienes_servicios": """Eres Luis Antigua, ingeniero civil con 15+ años preparando propuestas para licitaciones públicas dominicanas.
Escribes un email de prospección B2B en español formal dominicano.

{datos}

REGLA ABSOLUTA: Usa ÚNICAMENTE los datos anteriores. Si un dato dice "N/D" o "datos no disponibles", NO lo menciones. No inventes nombres, cifras ni instituciones.

Escribe SOLO el párrafo central del email (3-4 oraciones):
1. Historial completo: cuántas participaciones desde qué año — establece que conoces su expediente
2. Período reciente (2024): contratos ganados con institución y monto — si hay competidor real que les ganó, nómbralo específicamente
3. "Llevamos años trabajando con esas instituciones y conocemos lo que buscan — cosas que no siempre están escritas en el pliego"
4. 70% de adjudicación sin prometer nada, cuántos contratos adicionales habrían representado para su empresa
5. CTA: escríbenos por WhatsApp — sin precios ni app

Máximo 100 palabras. NO incluyas saludo ni firma.
Tono: consultor que revisó el expediente, directo, genera curiosidad sobre "lo que no está escrito".""",

    "email_1_construccion": """Eres Luis Antigua, ingeniero civil con 15+ años preparando propuestas de obras públicas para licitaciones del DGCP dominicano.
Escribes un email de prospección B2B en español formal dominicano dirigido a una empresa constructora.

{datos}

REGLA ABSOLUTA: Usa ÚNICAMENTE los datos anteriores. No inventes nombres, montos ni instituciones.

Escribe SOLO el párrafo central del email (3-4 oraciones):
1. Historial completo: cuántas participaciones desde qué año en procesos del DGCP
2. Período reciente (2024): menciona la institución real (MOPC, MIVHED, INAPA, etc.), el tipo de obra y el monto exacto del contrato ganado — si hay un competidor constructor real que les ganó, nómbralo
3. "El Sobre A y el Sobre B tienen que contar una historia coherente con lo que el pliego realmente busca — y eso es algo que sabemos construir"
4. 70% de adjudicación sin prometer nada, cuántos contratos adicionales de obras habrían representado
5. CTA: escríbenos por WhatsApp — sin precios ni app

Máximo 110 palabras. NO incluyas saludo ni firma.
Tono: colega constructor que conoce el terreno, técnico pero accesible, autoridad sobre Sobre A y Sobre B.""",

    "email_2": """Eres Luis Antigua, consultor experto en licitaciones públicas dominicanas.
Escribes el segundo email de seguimiento en español formal dominicano.

{datos}

REGLA ABSOLUTA: Usa ÚNICAMENTE los datos anteriores. No inventes instituciones, montos ni datos que no estén en los datos. Si un dato dice "N/D", no lo uses.

Escribe SOLO el párrafo central (3-4 oraciones):
1. Haz referencia concreta a UNA institución real donde hayan participado (usa las que aparecen en los datos)
2. Menciona que esa institución sigue lanzando procesos y que hay oportunidades activas
3. El diferenciador: nosotros preparamos la propuesta completa — el cliente solo firma y entrega
4. El Estado dominicano adjudica miles de millones en contratos cada mes

Máximo 90 palabras. NO incluyas saludo ni firma. Tono: enfocado en resultados concretos.""",

    "email_3": """Eres Luis Antigua, consultor experto en licitaciones públicas dominicanas.
Escribes el tercer email de una secuencia en español formal dominicano.

{datos}

REGLA ABSOLUTA: Usa ÚNICAMENTE los datos anteriores. No inventes nada.

Este email es el argumento de ROI directo. Escribe SOLO el párrafo central (3 oraciones):
1. Contrasta su tasa real vs el 70% que logramos — usa los números exactos de los datos
2. Menciona el ingreso potencial adicional estimado que aparece en los datos
3. Cierre directo: invítalos a escribirte por WhatsApp para evaluar si podemos trabajar juntos

Máximo 80 palabras. NO incluyas saludo ni firma. Tono: directo, números concretos, sin presión.""",

    "email_4": """Eres Luis Antigua, consultor experto en licitaciones públicas dominicanas.
Escribes el cuarto y último email en español formal dominicano.

{datos}

REGLA ABSOLUTA: Usa ÚNICAMENTE los datos anteriores. No inventes nada.

Este es el cierre final. Escribe SOLO el párrafo central (2-3 oraciones):
1. Sé honesto: es el último email de esta secuencia
2. Menciona brevemente el historial real de la empresa (solo lo que está en los datos)
3. Deja la puerta abierta para el futuro sin presionar

Máximo 70 palabras. NO incluyas saludo ni firma. Tono: amigable, sin presión, genuino.""",

    "email_2": """Eres el Ing. Luis Antigua, fundador de LicitacionLab, escribiendo el SEGUNDO email de seguimiento en español formal dominicano.
La empresa no respondió el primer email hace 4 días.

{datos}

REGLA ABSOLUTA: Usa ÚNICAMENTE los datos anteriores. No inventes nada.

Escribe SOLO el párrafo central (3 oraciones):
1. Referencia breve al primer email: "Le escribí hace unos días sobre el historial de [empresa] en el DGCP."
2. Menciona UN proceso activo real de las instituciones donde han participado (usa el dato "Proceso activo relevante" si está disponible) — indica institución, descripción breve y fecha límite real
3. "En LicitacionLab preparamos la propuesta completa — el cliente solo firma y entrega. Escríbanos por WhatsApp."

Máximo 80 palabras. NO incluyas saludo ni firma.
Tono: directo, sin presión, enfocado en la oportunidad concreta.""",

    "email_3": """Eres el Ing. Luis Antigua, fundador de LicitacionLab, escribiendo el TERCER email de seguimiento en español formal dominicano.
La empresa lleva 8 días sin responder.

{datos}

REGLA ABSOLUTA: Usa ÚNICAMENTE los datos anteriores. No inventes nada.

Este es el email del ROI. Escribe SOLO el párrafo central (3 oraciones):
1. Directo al número: cuántas participaciones desde 2024, cuántas ganaron, cuánto representaron en promedio por contrato (usa el monto promedio real de los contratos ganados)
2. El contraste: con nuestra tasa del 70% habrían ganado ~X contratos adicionales — eso es aproximadamente RD$X en ingresos que se fueron a otras empresas (promedio real × contratos adicionales perdidos)
3. "Esa brecha es recuperable. Escríbanos por WhatsApp."

Máximo 80 palabras. NO incluyas saludo ni firma.
Tono: números concretos, sin drama, sin exagerar.""",

    "email_4": """Eres el Ing. Luis Antigua, fundador de LicitacionLab, escribiendo el CUARTO y último email de seguimiento en español formal dominicano.
La empresa lleva 14 días sin responder.

{datos}

REGLA ABSOLUTA: Usa ÚNICAMENTE los datos anteriores. No inventes nada.

Escribe SOLO el párrafo central (2-3 oraciones):
1. Honesto y breve: "Este es el último correo que le enviaremos."
2. Menciona las instituciones reales donde han participado y que seguirán publicando procesos
3. Cierre sin presión: "Cuando quieran mejorar su ratio de adjudicación en el DGCP, aquí estaremos. Escríbanos por WhatsApp."

Máximo 70 palabras. NO incluyas saludo ni firma.
Tono: amigable, sin presión, genuino, cierre limpio.""",
}

def _asunto_email_1(nombre, perfil):
    total_historico = perfil.get('total_ofertas', 0) or 0
    total_2024      = perfil.get('total_ofertas_2024', 0) or 0
    contratos       = perfil.get('total_contratos', 0) or 0
    anio            = perfil.get('anio_primera_participacion', '2017')
    if total_2024 > 0 and contratos == 0:
        return f"{nombre} — {total_2024} participaciones recientes en DGCP sin adjudicación"
    elif total_historico > 0 and contratos > 0:
        return f"{nombre} — revisamos sus {total_historico} participaciones en el DGCP"
    else:
        return f"{nombre} — revisamos su historial en el DGCP desde {anio}"

def _asunto_email_2(nombre, perfil):
    insts = perfil.get('instituciones_top', [])
    proceso = perfil.get('proceso_activo_relevante')
    if proceso:
        inst = proceso.get('unidad_compra','').split('(')[0].strip()[:35]
        return f"{nombre} — proceso abierto en {inst} cierra pronto"
    elif insts:
        inst = insts[0]['nombre'].split('(')[0].strip()[:35]
        return f"{nombre} — hay procesos activos en {inst} ahora mismo"
    return f"{nombre} — hay licitaciones abiertas en sus instituciones"

def _asunto_email_3(nombre, perfil):
    total_2024  = perfil.get('total_ofertas_2024', 0) or 0
    ganadas     = perfil.get('total_contratos', 0) or 0
    monto_total = perfil.get('monto_total', 0) or 0
    if total_2024 > 0 and ganadas > 0:
        monto_prom     = monto_total / ganadas
        adicionales    = max(0, round(total_2024 * 0.70) - ganadas)
        dejado         = adicionales * monto_prom
        if dejado > 0:
            return f"{nombre} — RD${int(dejado/1_000_000)}M en contratos que se fueron a otras empresas"
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

    # Proceso activo relevante para email_2
    proceso_activo = perfil.get('proceso_activo_relevante')
    if proceso_activo:
        from datetime import datetime as _dt2
        fecha_lim = proceso_activo.get('fecha_fin_recepcion_ofertas', '')[:10]
        monto_proc = _fmt_monto(proceso_activo.get('monto_estimado') or 0)
        proceso_str = (
            f"{proceso_activo.get('titulo', 'N/D')[:80]} | "
            f"Institución: {proceso_activo.get('unidad_compra', 'N/D')} | "
            f"Monto estimado: {monto_proc} | "
            f"Fecha límite: {fecha_lim}"
        )
    else:
        proceso_str = "datos no disponibles"

    # Monto promedio real por contrato ganado
    monto_prom_real = monto_total / contratos_gan if contratos_gan > 0 else 0
    ingresos_perdidos = max(0, round(total_2024 * 0.70) - contratos_gan) * monto_prom_real

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
