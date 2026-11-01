-- Seguridad: permisos de funciones y search_path (aplicada en Supabase el 2026-11-01)

-- 1) Funciones SECURITY DEFINER que solo usa el backend (service_role)
DO $$
DECLARE f text;
BEGIN
  FOREACH f IN ARRAY ARRAY[
    'public.actualizar_cache_empresa(uuid)',
    'public.actualizar_cache_empresas(uuid[])',
    'public.buscar_rubros(text, integer)',
    'public.fn_actualizar_estados_certificaciones(uuid)',
    'public.fn_actualizar_estados_certificaciones_global()',
    'public.get_competidores_frecuentes(text, integer)',
    'public.get_dashboard_rubro(text, integer)',
    'public.get_oferentes_proceso(text)',
    'public.get_perfil_winloss(text)',
    'public.get_resumen_mercado()',
    'public.tiene_suscripcion_activa(uuid)',
    'public.handle_new_user()'
  ] LOOP
    EXECUTE format('REVOKE EXECUTE ON FUNCTION %s FROM PUBLIC, anon, authenticated', f);
    EXECUTE format('GRANT EXECUTE ON FUNCTION %s TO service_role', f);
  END LOOP;
END $$;

-- 2) bid_empresa_id_actual(): la usan las políticas de storage del Expediente (authenticated)
REVOKE EXECUTE ON FUNCTION public.bid_empresa_id_actual() FROM PUBLIC, anon;
GRANT EXECUTE ON FUNCTION public.bid_empresa_id_actual() TO authenticated, service_role;

-- 3) search_path fijo
ALTER FUNCTION public.unaccent_immutable(text)                    SET search_path = public, extensions, pg_temp;
ALTER FUNCTION public.trg_set_anio()                              SET search_path = public, extensions, pg_temp;
ALTER FUNCTION public.handle_new_user()                           SET search_path = public, extensions, pg_temp;
ALTER FUNCTION public.fn_actualizar_estado_certificacion()        SET search_path = public, extensions, pg_temp;
ALTER FUNCTION public.fn_reset_flags_alertas_cert()               SET search_path = public, extensions, pg_temp;
ALTER FUNCTION public.ig_es_obra(text, text)                      SET search_path = public, extensions, pg_temp;
ALTER FUNCTION public.ig_datos(text)                              SET search_path = public, extensions, pg_temp;
ALTER FUNCTION public.correo_rubro(text)                          SET search_path = public, extensions, pg_temp;
ALTER FUNCTION public.correo_lote(integer, numeric)               SET search_path = public, extensions, pg_temp;
ALTER FUNCTION public.correo_rubro_bs(text)                       SET search_path = public, extensions, pg_temp;
ALTER FUNCTION public.correo_lote_bs(integer, numeric)            SET search_path = public, extensions, pg_temp;
