-- ============================================================
-- EXPEDIENTE DIGITAL v2 — FASE 4: Generar Sobre A (asíncrono)
-- Fecha: 2026-08-20
-- Aplicada en producción vía MCP el 2026-08-20.
-- Este archivo es la copia versionada. NO re-ejecutar a mano:
-- es idempotente (IF NOT EXISTS) pero ya está aplicada.
-- ============================================================

-- Jobs asíncronos de generación de Sobre A.
-- Un job = una solicitud de generación (paquete completo o pieza individual).
-- El worker (BackgroundTasks) actualiza progreso/resultado/pendientes.
CREATE TABLE IF NOT EXISTS bid_sobre_jobs (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  empresa_id uuid NOT NULL,
  referencia text NOT NULL,

  -- piezas solicitadas: ["f034","f042","doc:certificaciones:<uuid>", ...] o ["todas"]
  piezas jsonb NOT NULL DEFAULT '[]'::jsonb,

  -- 'docx' (formularios editables) | 'pdf' (requiere LibreOffice en el runtime)
  formato text NOT NULL DEFAULT 'docx',

  -- en_cola -> procesando -> listo | error
  estado text NOT NULL DEFAULT 'en_cola',

  -- {"pieza_actual": "SNCC.F.034", "completadas": 3, "total": 12}
  progreso jsonb NOT NULL DEFAULT '{}'::jsonb,

  -- {"url_firmada": "...", "nombre": "Sobre_A_REF_fecha.zip", "bytes": 123, "paginas": 45}
  resultado jsonb NOT NULL DEFAULT '{}'::jsonb,

  -- hoja de pendientes: [{"pieza":"...", "motivo":"...", "campos":[...]}]
  pendientes jsonb NOT NULL DEFAULT '[]'::jsonb,

  error text,
  creado_en timestamptz NOT NULL DEFAULT now(),
  actualizado_en timestamptz NOT NULL DEFAULT now(),

  -- el ZIP en el bucket vence a los 7 días; el cron de purga lo usa
  expira_en timestamptz
);

CREATE INDEX IF NOT EXISTS idx_sobre_jobs_empresa
  ON bid_sobre_jobs (empresa_id, creado_en DESC);

-- parcial: solo jobs vivos, para que el worker/monitor los encuentre barato
CREATE INDEX IF NOT EXISTS idx_sobre_jobs_estado
  ON bid_sobre_jobs (estado) WHERE estado IN ('en_cola','procesando');

-- RLS activo SIN policies = acceso solo con service key (patrón Fase 0).
ALTER TABLE bid_sobre_jobs ENABLE ROW LEVEL SECURITY;

-- NOTA: la firma digital del representante NO necesita DDL:
-- se guarda en bid_archivos (polimórfica) con entidad='empresa', tipo='firma'.

-- ============================================================
-- FASE 4b (2026-08-20, aplicada vía MCP): formularios del portal
-- ============================================================

-- Formularios propios del proceso descargados del portal transaccional
-- (FR-CYC-002, CEI, SGI, etc.). El usuario los llena y sube su versión
-- llenada en llenado_storage_path.
CREATE TABLE IF NOT EXISTS bid_formularios_proceso (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  empresa_id uuid NOT NULL,
  referencia text NOT NULL,
  requisito text,
  nombre_archivo text NOT NULL,
  storage_path text NOT NULL,
  mime text,
  tamano_bytes bigint,
  llenado_storage_path text,
  llenado_nombre text,
  origen text NOT NULL DEFAULT 'portal',
  creado_en timestamptz NOT NULL DEFAULT now(),
  actualizado_en timestamptz NOT NULL DEFAULT now(),
  UNIQUE (empresa_id, referencia, nombre_archivo)
);
CREATE INDEX IF NOT EXISTS idx_form_proceso_ref ON bid_formularios_proceso (empresa_id, referencia);
ALTER TABLE bid_formularios_proceso ENABLE ROW LEVEL SECURITY;

-- Control de intentos de scraping (máx. 1 cada 6 horas por proceso)
CREATE TABLE IF NOT EXISTS bid_formularios_sync (
  empresa_id uuid NOT NULL,
  referencia text NOT NULL,
  ultimo_intento timestamptz NOT NULL DEFAULT now(),
  exito boolean NOT NULL DEFAULT false,
  detalle text,
  PRIMARY KEY (empresa_id, referencia)
);
ALTER TABLE bid_formularios_sync ENABLE ROW LEVEL SECURITY;
