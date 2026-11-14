-- Análisis de pliego: correo por usuario + reintentos automáticos
--
-- 1) El candado del correo pasa de ser uno por proceso (analisis_pliego.email_enviado_at)
--    a uno por seguidor (seguimiento_procesos.analisis_email_at). Así quien sigue un
--    proceso ya analizado, o lo vuelve a seguir, también recibe el análisis.
-- 2) Un análisis fallido se reintenta hasta 3 veces (analisis_pliego.intentos) y se
--    guarda el motivo del último fallo (analisis_pliego.error_detalle).

alter table public.seguimiento_procesos
  add column if not exists analisis_email_at timestamptz;

comment on column public.seguimiento_procesos.analisis_email_at is
  'Cuándo se le envió a este seguidor el correo del análisis del pliego (NULL = pendiente)';

-- Quienes ya seguían el proceso cuando salió el correo, ya lo recibieron
update public.seguimiento_procesos s
   set analisis_email_at = a.email_enviado_at
  from public.analisis_pliego a
 where a.proceso_id = s.proceso_codigo
   and a.email_enviado_at is not null
   and s.analisis_email_at is null
   and s.created_at <= a.email_enviado_at + interval '5 seconds';

alter table public.analisis_pliego
  add column if not exists intentos integer not null default 0;

alter table public.analisis_pliego
  add column if not exists error_detalle text;

comment on column public.analisis_pliego.intentos is
  'Intentos fallidos del análisis; con 3 queda en estado error';
comment on column public.analisis_pliego.error_detalle is
  'Motivo del último fallo del análisis (se borra cuando completa)';
