-- Mission Control auth tables (callsign mapping + throttle)

create schema if not exists mc_auth;

create table if not exists mc_auth.mc_commanders (
  callsign text primary key,
  email text not null,
  commander_display text not null,
  is_active boolean not null default true,
  created_at timestamptz not null default now()
);

create table if not exists mc_auth.mc_login_throttle (
  throttle_key text primary key,
  callsign text not null,
  ip_hash text not null,
  failed_count int not null default 0,
  cooldown_until timestamptz null,
  window_start timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create index if not exists mc_login_throttle_callsign_idx
  on mc_auth.mc_login_throttle (callsign);

create index if not exists mc_login_throttle_updated_at_idx
  on mc_auth.mc_login_throttle (updated_at);

revoke all on table mc_auth.mc_commanders from anon, authenticated;
revoke all on table mc_auth.mc_login_throttle from anon, authenticated;
