-- ============================================================
-- CONTROL DE OBRA — gastos con factura adjunta y origen
-- Fecha: 2026-11-01. Idempotente.
-- ============================================================
ALTER TABLE obra_gastos ADD COLUMN IF NOT EXISTS rnc          text;
ALTER TABLE obra_gastos ADD COLUMN IF NOT EXISTS archivo_path text;   -- foto/PDF en bucket bid-manager-docs
ALTER TABLE obra_gastos ADD COLUMN IF NOT EXISTS origen       text NOT NULL DEFAULT 'manual';  -- manual | foto | excel
