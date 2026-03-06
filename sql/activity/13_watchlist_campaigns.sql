-- Watchlist per-user campaigns (RLS by auth.uid)

create extension if not exists "pgcrypto";

create table if not exists public.watchlist_campaigns (
  id uuid primary key default gen_random_uuid(),
  user_id uuid not null,
  user_email text null,
  commander_callsign text null,
  campaign_key text not null,
  company text not null,
  traffic_type text not null,
  pinned boolean not null default false,
  snoozed_until timestamptz null,
  notes text null,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

alter table public.watchlist_campaigns
  add constraint watchlist_campaigns_user_campaign_key_uniq
  unique (user_id, campaign_key);

create index if not exists watchlist_campaigns_user_id_idx
  on public.watchlist_campaigns (user_id);

create index if not exists watchlist_campaigns_user_id_pinned_idx
  on public.watchlist_campaigns (user_id, pinned);

create index if not exists watchlist_campaigns_user_id_snoozed_until_idx
  on public.watchlist_campaigns (user_id, snoozed_until);

create index if not exists watchlist_campaigns_campaign_key_idx
  on public.watchlist_campaigns (campaign_key);

create or replace function public.set_updated_at()
returns trigger
language plpgsql
as $$
begin
  new.updated_at = now();
  return new;
end;
$$;

drop trigger if exists set_watchlist_campaigns_updated_at on public.watchlist_campaigns;
create trigger set_watchlist_campaigns_updated_at
before update on public.watchlist_campaigns
for each row
execute function public.set_updated_at();

alter table public.watchlist_campaigns enable row level security;

drop policy if exists watchlist_campaigns_select on public.watchlist_campaigns;
create policy watchlist_campaigns_select
on public.watchlist_campaigns
for select
to authenticated
using (auth.uid() = user_id);

drop policy if exists watchlist_campaigns_insert on public.watchlist_campaigns;
create policy watchlist_campaigns_insert
on public.watchlist_campaigns
for insert
to authenticated
with check (auth.uid() = user_id);

drop policy if exists watchlist_campaigns_update on public.watchlist_campaigns;
create policy watchlist_campaigns_update
on public.watchlist_campaigns
for update
to authenticated
using (auth.uid() = user_id)
with check (auth.uid() = user_id);

drop policy if exists watchlist_campaigns_delete on public.watchlist_campaigns;
create policy watchlist_campaigns_delete
on public.watchlist_campaigns
for delete
to authenticated
using (auth.uid() = user_id);
