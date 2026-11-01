-- Semáforo de Pago: búsqueda completa de empresas y cola del ETL InfoPago
-- (aplicada en Supabase el 2026-11-01)

-- 1) Marca de intento del resolver: las empresas sin match ya no bloquean la cola
ALTER TABLE public.empresas_estado ADD COLUMN IF NOT EXISTS infopago_resolver_at timestamptz;
CREATE INDEX IF NOT EXISTS idx_empresas_estado_infopago_pend
  ON public.empresas_estado (total_contratos DESC NULLS LAST)
  WHERE infopago_id IS NULL;

-- 2) Vista del buscador: una fila por empresa, con RNC tomado de las facturas
--    o, si no lo traen, de infopago_proveedor_contratos
DROP MATERIALIZED VIEW IF EXISTS public.infopago_proveedores;
CREATE MATERIALIZED VIEW public.infopago_proveedores AS
WITH f AS (
  SELECT upper(trim(nombre_proveedor)) AS nombre_norm,
         MIN(nombre_proveedor) AS nombre_proveedor,
         MAX(NULLIF(rnc_proveedor, '')) AS rnc_fact,
         count(*) AS total_facturas,
         sum(monto) AS monto_total,
         min(fecha_emision) AS primera_factura,
         max(fecha_emision) AS ultima_factura
  FROM public.infopago_facturas
  WHERE nombre_proveedor IS NOT NULL
  GROUP BY 1
),
pc AS (
  SELECT upper(trim(razon_social)) AS nombre_norm, MAX(rnc) AS rnc
  FROM public.infopago_proveedor_contratos
  WHERE rnc ~ '^[0-9]{9,11}$'
  GROUP BY 1
)
SELECT COALESCE(f.rnc_fact, pc.rnc, '') AS rnc_proveedor,
       f.nombre_proveedor, f.nombre_norm, f.total_facturas, f.monto_total,
       f.primera_factura, f.ultima_factura
FROM f LEFT JOIN pc USING (nombre_norm);

CREATE UNIQUE INDEX idx_infopago_prov_uniq ON public.infopago_proveedores (nombre_proveedor, rnc_proveedor);
CREATE INDEX idx_infopago_prov_trgm  ON public.infopago_proveedores USING gin (nombre_proveedor gin_trgm_ops);
CREATE INDEX idx_infopago_prov_rnc   ON public.infopago_proveedores (rnc_proveedor text_pattern_ops);
CREATE INDEX idx_infopago_prov_total ON public.infopago_proveedores (total_facturas DESC);
GRANT SELECT ON public.infopago_proveedores TO anon, authenticated, service_role;

-- 3) Refresco de la vista (antes nadie la refrescaba)
CREATE OR REPLACE FUNCTION public.refrescar_infopago_proveedores()
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO 'public', 'pg_temp'
AS $$
BEGIN
  REFRESH MATERIALIZED VIEW CONCURRENTLY public.infopago_proveedores;
END;
$$;
REVOKE ALL ON FUNCTION public.refrescar_infopago_proveedores() FROM PUBLIC, anon, authenticated;
GRANT EXECUTE ON FUNCTION public.refrescar_infopago_proveedores() TO service_role;

CREATE EXTENSION IF NOT EXISTS pg_cron;
SELECT cron.schedule('refrescar-infopago-proveedores', '15 * * * *',
                     'SELECT public.refrescar_infopago_proveedores()');
