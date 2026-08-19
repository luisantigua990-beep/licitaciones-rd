-- Expediente Digital v2 — Fase 0 (YA APLICADA en producción el 18 ago 2026
-- vía Supabase MCP, migración: expediente_v2_fase0_archivos_auditoria).
-- Se versiona aquí como registro. Aditiva: no toca tablas existentes.

create table if not exists bid_archivos (
  id            uuid primary key default gen_random_uuid(),
  empresa_id    uuid not null references perfiles_empresa(id) on delete cascade,
  entidad       text not null check (entidad in
                  ('personal','equipos','experiencia','socios',
                   'representantes','certificaciones','financieros')),
  registro_id   uuid not null,
  nombre        text not null,
  storage_path  text not null,
  mime          text not null,
  tamano_bytes  bigint not null check (tamano_bytes > 0),
  estado        text not null default 'pendiente'
                check (estado in ('validado','pendiente','rechazado')),
  subido_por    uuid,
  creado_en     timestamptz not null default now(),
  actualizado_en timestamptz,
  eliminado_en  timestamptz
);
create index if not exists idx_bid_archivos_registro
  on bid_archivos (empresa_id, entidad, registro_id) where eliminado_en is null;
create index if not exists idx_bid_archivos_purga
  on bid_archivos (eliminado_en) where eliminado_en is not null;

create table if not exists bid_auditoria (
  id          bigint generated always as identity primary key,
  empresa_id  uuid not null references perfiles_empresa(id) on delete cascade,
  actor       text not null,
  accion      text not null,
  entidad     text,
  registro_id uuid,
  detalle     jsonb,
  creado_en   timestamptz not null default now()
);
create index if not exists idx_bid_auditoria_empresa on bid_auditoria (empresa_id, creado_en desc);
create index if not exists idx_bid_auditoria_registro on bid_auditoria (empresa_id, entidad, registro_id, creado_en desc);

create table if not exists bid_propuesta_cache (
  empresa_id       uuid not null references perfiles_empresa(id) on delete cascade,
  referencia       text not null,
  hash_expediente  text not null,
  payload          jsonb not null,
  creado_en        timestamptz not null default now(),
  primary key (empresa_id, referencia)
);

create table if not exists bid_user_prefs (
  user_id     uuid not null,
  clave       text not null,
  valor       jsonb not null,
  actualizado_en timestamptz not null default now(),
  primary key (user_id, clave)
);

alter table bid_archivos        enable row level security;
alter table bid_auditoria       enable row level security;
alter table bid_propuesta_cache enable row level security;
alter table bid_user_prefs      enable row level security;
