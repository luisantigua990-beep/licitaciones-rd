-- ═══════════════════════════════════════════════════════════════════════
-- Rendimiento — 2026-10-18
-- YA APLICADO EN PRODUCCIÓN (proyecto yqmwwwqgtkrxtjxaqyxk). Este archivo deja
-- constancia de lo que se hizo. Todo es aditivo: no cambia datos ni borra nada.
-- Los índices se crearon con CONCURRENTLY (sin bloquear escrituras).
-- ═══════════════════════════════════════════════════════════════════════

-- 1) Índices para consultas lentas detectadas en pg_stat_statements
--    (tiempo medio en producción antes del cambio → páginas leídas antes/después)

-- UPDATE infopago_proveedor_contratos ... WHERE contrato = ANY(...)  (etl_infopago, 137 ms medio)
-- El único índice tenía (rnc, contrato): con solo "contrato" recorría el índice entero.
-- 1.137 → 3 páginas
create index concurrently if not exists idx_ipc_contrato
  on public.infopago_proveedor_contratos (contrato);

-- monitor.py: procesos sin enriquecer (932 ms medio; recorría toda la tabla)
-- 2.672 → 1 página
create index concurrently if not exists idx_procesos_sin_enriquecer
  on public.procesos (fecha_publicacion) where enriquecido_api = false;

-- etl_infopago: empresas pendientes de cruzar con InfoPago, ordenadas por contratos (1.175 ms medio)
-- 1.652 → 198 páginas
create index concurrently if not exists idx_empresas_sin_infopago_top
  on public.empresas_estado (total_contratos desc nulls last) where infopago_id is null;

-- etl_infopago: empresas con InfoPago pendientes de traer contratos
-- 1.026 → 1 página
create index concurrently if not exists idx_empresas_pend_contratos
  on public.empresas_estado (total_contratos desc nulls last)
  where infopago_id is not null and infopago_contratos_at is null;

-- NOTA: también quedó creado idx_empresas_pend_infopago (mismo objetivo que
-- idx_empresas_sin_infopago_top pero con "and nombre is not null" en el filtro).
-- Postgres 17 no lo usa porque descarta esa condición (nombre es NOT NULL).
-- No se usa: se puede borrar sin riesgo con
--   drop index concurrently if exists public.idx_empresas_pend_infopago;


-- 2) /api/stats en una sola ida (antes 6 consultas count exact por carga de pantalla)
create or replace function public.estadisticas_home()
returns json
language sql
stable
set search_path = public, pg_temp
as $$
  select json_build_object(
    'total_procesos',    (select count(*) from public.procesos),
    'procesos_activos',  a.activos,
    'obras_activas',     a.obras,
    'bienes_activos',    a.bienes,
    'servicios_activos', a.servicios,
    -- no se muestra en la app; estimado del catálogo en vez de recorrer 700k filas
    'total_articulos',   (select greatest(c.reltuples, 0)::bigint from pg_class c where c.oid = 'public.articulos_proceso'::regclass)
  )
  from (
    select count(*)                                              as activos,
           count(*) filter (where objeto_proceso ilike '%Obras%')     as obras,
           count(*) filter (where objeto_proceso ilike '%Bienes%')    as bienes,
           count(*) filter (where objeto_proceso ilike '%Servicios%') as servicios
    from public.procesos
    where estado_proceso = 'Proceso publicado'
      and fecha_fin_recepcion_ofertas > now()
  ) a;
$$;
grant execute on function public.estadisticas_home() to anon, authenticated, service_role;


-- 3) Caché de totales por empresa en lote (antes 1 RPC por empresa: 3,3 M llamadas)
create or replace function public.actualizar_cache_empresas(p_ids uuid[])
returns integer
language sql
security definer
set search_path = public, pg_temp
as $$
  with agg as (
    select i.id,
           count(c.id)                          as n,
           coalesce(sum(c.monto_adjudicado), 0) as monto,
           min(c.fecha_adjudicacion)            as primero,
           max(c.fecha_adjudicacion)            as ultimo
    from unnest(p_ids) as i(id)
    left join public.contratos_adjudicados c on c.empresa_id = i.id
    group by i.id
  ), upd as (
    update public.empresas_estado e
       set total_contratos = agg.n,
           monto_total     = agg.monto,
           primer_contrato = agg.primero,
           ultimo_contrato = agg.ultimo,
           actualizado_en  = now()
      from agg
     where e.id = agg.id
    returning 1
  )
  select count(*)::int from upd;
$$;
revoke all on function public.actualizar_cache_empresas(uuid[]) from public, anon, authenticated;
grant execute on function public.actualizar_cache_empresas(uuid[]) to service_role;
-- Si el ETL corre con la clave anon, la llamada por lote falla y el código vuelve solo
-- al método anterior (1 por empresa). Para aprovecharla, define SUPABASE_SERVICE_KEY.
