-- ============================================================
-- CONTROL DE OBRA — FASE 0 (equivalente interno a SINCO ADPRO)
-- Fecha: 2026-10-23
-- Patrón: igual que bid_* — RLS activo SIN policies = solo service key.
-- Idempotente (IF NOT EXISTS). Aplicar vía MCP de Supabase.
-- ============================================================

-- 1. OBRA = contrato adjudicado. Nace de una licitación (referencia)
--    o se crea a mano (obra privada / subcontrato).
CREATE TABLE IF NOT EXISTS obras (
  id              uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  empresa_id      uuid NOT NULL,
  referencia      text,                      -- código del proceso DGCP (opcional)
  nombre          text NOT NULL,
  entidad         text,                      -- contratante
  contrato_no     text,
  monto_contrato  numeric(16,2) NOT NULL DEFAULT 0,
  avance_pct_meta numeric(5,2),              -- opcional: meta de avance físico
  fecha_inicio    date,
  fecha_fin       date,
  plazo_dias      integer,
  -- planificada -> en_ejecucion -> suspendida -> terminada -> cerrada
  estado          text NOT NULL DEFAULT 'planificada',
  notas           text,
  creado_en       timestamptz NOT NULL DEFAULT now(),
  actualizado_en  timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_obras_empresa ON obras (empresa_id, creado_en DESC);
ALTER TABLE obras ENABLE ROW LEVEL SECURITY;

-- 2. PARTIDAS CONTRATADAS = el listado de cantidades de la oferta.
--    Mismas columnas que el presupuesto de licitación para importarlo tal cual.
CREATE TABLE IF NOT EXISTS obra_partidas (
  id              uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  empresa_id      uuid NOT NULL,
  obra_id         uuid NOT NULL REFERENCES obras(id) ON DELETE CASCADE,
  orden           integer NOT NULL DEFAULT 0,
  codigo          text,                      -- 1.01, 2.03 ...
  capitulo        text,                      -- "MOVIMIENTO DE TIERRA"
  descripcion     text NOT NULL,
  unidad          text,
  cantidad        numeric(16,4) NOT NULL DEFAULT 0,
  precio_unitario numeric(16,2) NOT NULL DEFAULT 0,
  total           numeric(18,2) GENERATED ALWAYS AS (cantidad * precio_unitario) STORED,
  -- costo directo estimado del APU (sin indirectos ni utilidad); base del margen esperado
  costo_unitario_apu numeric(16,2),
  creado_en       timestamptz NOT NULL DEFAULT now(),
  actualizado_en  timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_obra_partidas_obra ON obra_partidas (obra_id, orden);
ALTER TABLE obra_partidas ENABLE ROW LEVEL SECURITY;

-- 3. CUBICACIONES = avance físico certificado por periodo.
CREATE TABLE IF NOT EXISTS obra_cubicaciones (
  id              uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  empresa_id      uuid NOT NULL,
  obra_id         uuid NOT NULL REFERENCES obras(id) ON DELETE CASCADE,
  numero          integer NOT NULL,          -- Cubicación No. 1, 2, 3...
  periodo_desde   date,
  periodo_hasta   date,
  -- borrador -> presentada -> aprobada -> pagada
  estado          text NOT NULL DEFAULT 'borrador',
  monto_presentado numeric(18,2) NOT NULL DEFAULT 0,
  monto_aprobado  numeric(18,2),
  fecha_pago      date,
  notas           text,
  creado_en       timestamptz NOT NULL DEFAULT now(),
  actualizado_en  timestamptz NOT NULL DEFAULT now(),
  UNIQUE (obra_id, numero)
);
CREATE INDEX IF NOT EXISTS idx_obra_cub_obra ON obra_cubicaciones (obra_id, numero);
ALTER TABLE obra_cubicaciones ENABLE ROW LEVEL SECURITY;

-- Detalle: cantidad ejecutada de cada partida en esa cubicación.
CREATE TABLE IF NOT EXISTS obra_cubicacion_detalle (
  id              uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  cubicacion_id   uuid NOT NULL REFERENCES obra_cubicaciones(id) ON DELETE CASCADE,
  partida_id      uuid NOT NULL REFERENCES obra_partidas(id) ON DELETE CASCADE,
  cantidad        numeric(16,4) NOT NULL DEFAULT 0,
  UNIQUE (cubicacion_id, partida_id)
);
CREATE INDEX IF NOT EXISTS idx_obra_cubdet_partida ON obra_cubicacion_detalle (partida_id);
ALTER TABLE obra_cubicacion_detalle ENABLE ROW LEVEL SECURITY;

-- 4. GASTOS = costo real. Cada gasto se carga a una partida (o queda como
--    indirecto de obra si partida_id es NULL).
CREATE TABLE IF NOT EXISTS obra_gastos (
  id              uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  empresa_id      uuid NOT NULL,
  obra_id         uuid NOT NULL REFERENCES obras(id) ON DELETE CASCADE,
  partida_id      uuid REFERENCES obra_partidas(id) ON DELETE SET NULL,
  fecha           date NOT NULL DEFAULT CURRENT_DATE,
  -- material | mano_obra | equipo | subcontrato | transporte | indirecto
  tipo            text NOT NULL DEFAULT 'material',
  proveedor       text,
  descripcion     text NOT NULL,
  documento       text,                      -- No. factura / conduce / recibo
  monto           numeric(18,2) NOT NULL DEFAULT 0,
  itbis           numeric(18,2) NOT NULL DEFAULT 0,
  pagado          boolean NOT NULL DEFAULT false,
  creado_en       timestamptz NOT NULL DEFAULT now(),
  actualizado_en  timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_obra_gastos_obra ON obra_gastos (obra_id, fecha DESC);
CREATE INDEX IF NOT EXISTS idx_obra_gastos_partida ON obra_gastos (partida_id);
ALTER TABLE obra_gastos ENABLE ROW LEVEL SECURITY;

-- 5. VISTA: estado de cada partida (contratado / ejecutado / gastado / margen).
--    Todo el tablero sale de aquí con un solo SELECT.
CREATE OR REPLACE VIEW v_obra_partida_estado AS
SELECT
  p.id            AS partida_id,
  p.obra_id,
  p.empresa_id,
  p.orden, p.codigo, p.capitulo, p.descripcion, p.unidad,
  p.cantidad      AS cant_contratada,
  p.precio_unitario,
  p.total         AS monto_contratado,
  COALESCE(e.cant_ejecutada, 0)                                   AS cant_ejecutada,
  ROUND(COALESCE(e.cant_ejecutada, 0) * p.precio_unitario, 2)     AS monto_ejecutado,
  CASE WHEN p.cantidad > 0
       THEN ROUND(COALESCE(e.cant_ejecutada, 0) / p.cantidad * 100, 2)
       ELSE 0 END                                                 AS avance_pct,
  COALESCE(g.gastado, 0)                                          AS gastado,
  ROUND(COALESCE(e.cant_ejecutada, 0) * p.precio_unitario - COALESCE(g.gastado, 0), 2) AS margen,
  CASE WHEN COALESCE(e.cant_ejecutada, 0) > 0
       THEN ROUND(COALESCE(g.gastado, 0) / e.cant_ejecutada, 2)
       ELSE NULL END                                              AS costo_unitario_real,
  p.costo_unitario_apu
FROM obra_partidas p
LEFT JOIN (
  SELECT d.partida_id, SUM(d.cantidad) AS cant_ejecutada
  FROM obra_cubicacion_detalle d
  JOIN obra_cubicaciones c ON c.id = d.cubicacion_id
  WHERE c.estado <> 'borrador'           -- solo cubicaciones presentadas o más
  GROUP BY d.partida_id
) e ON e.partida_id = p.id
LEFT JOIN (
  SELECT partida_id, SUM(monto) AS gastado
  FROM obra_gastos WHERE partida_id IS NOT NULL
  GROUP BY partida_id
) g ON g.partida_id = p.id;

-- 6. VISTA: resumen por obra para la lista y los KPIs.
CREATE OR REPLACE VIEW v_obra_resumen AS
SELECT
  o.id AS obra_id, o.empresa_id, o.nombre, o.entidad, o.referencia, o.estado,
  o.monto_contrato, o.fecha_inicio, o.fecha_fin,
  COALESCE(SUM(v.monto_contratado), 0)   AS presupuesto,
  COALESCE(SUM(v.monto_ejecutado), 0)    AS ejecutado,
  COALESCE(SUM(v.gastado), 0)            AS gastado_directo,
  COALESCE(gi.indirectos, 0)             AS gastado_indirecto,
  COALESCE(cb.cobrado, 0)                AS cobrado,
  CASE WHEN SUM(v.monto_contratado) > 0
       THEN ROUND(SUM(v.monto_ejecutado) / SUM(v.monto_contratado) * 100, 2)
       ELSE 0 END                        AS avance_financiero_pct
FROM obras o
LEFT JOIN v_obra_partida_estado v ON v.obra_id = o.id
LEFT JOIN (
  SELECT obra_id, SUM(monto) AS indirectos
  FROM obra_gastos WHERE partida_id IS NULL GROUP BY obra_id
) gi ON gi.obra_id = o.id
LEFT JOIN (
  SELECT obra_id, SUM(COALESCE(monto_aprobado, monto_presentado)) AS cobrado
  FROM obra_cubicaciones WHERE estado = 'pagada' GROUP BY obra_id
) cb ON cb.obra_id = o.id
GROUP BY o.id, gi.indirectos, cb.cobrado;
