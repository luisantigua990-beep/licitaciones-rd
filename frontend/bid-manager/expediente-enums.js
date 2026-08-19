/**
 * expediente-enums.js — Espejo de expediente_core.py (regla D3)
 * ==============================================================
 * Fuente de verdad: el backend. Este archivo solo REFLEJA los enums
 * para que el frontend valide y pinte; NUNCA calcula estados por su
 * cuenta (reglas D1-D4 de RECOMENDACIONES_PRODUCCION.md).
 * Si cambias algo aquí, cámbialo primero en expediente_core.py.
 */
window.ExpedienteEnums = Object.freeze({
  ESTADO_REGISTRO: Object.freeze(["completo", "incompleto", "vencido", "por_vencer", "sin_verificar"]),
  ESTADO_VERIFICACION: Object.freeze(["validado", "pendiente", "rechazado"]),
  ESTADO_RENGLON: Object.freeze(["cumple", "en_riesgo", "no_cumple"]),
  ESTADO_PIEZA: Object.freeze(["ok", "warn", "err"]),
  ENTIDADES_ADJUNTOS: Object.freeze([
    "personal", "equipos", "experiencia", "socios",
    "representantes", "certificaciones", "financieros",
  ]),

  // estado_pieza → tono visual (chips/puntos). El tono NO decide reglas.
  TONO: Object.freeze({ ok: "ok", warn: "warn", err: "err" }),

  // Límites (informativos para UX; el backend es quien rechaza)
  MAX_MB_ARCHIVO: 15,
  MAX_MB_REQUEST: 50,
  MAX_ARCHIVOS_POR_SUBIDA: 5,
  EXTENSIONES: Object.freeze(["pdf", "doc", "docx", "xls", "xlsx", "jpg", "jpeg", "png"]),
  ACCEPT: ".pdf,.doc,.docx,.xls,.xlsx,image/jpeg,image/png",
});
