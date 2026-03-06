-- mv_campaign_activity
-- Purpose: Materialized campaign activity for dashboards + alerting.
create materialized view public.mv_campaign_activity as
with
-- Compute month boundaries in America/Los_Angeles.
month_bounds as (
  select
    date_trunc('month', timezone('America/Los_Angeles', now())) as start_of_current_month_local,
    date_trunc('month', timezone('America/Los_Angeles', now())) - interval '1 month'
      as start_of_previous_month_local,
    date_trunc('month', timezone('America/Los_Angeles', now())) + interval '1 month'
      as start_of_next_month_local
),
-- Aggregate forms events in current + previous month window.
forms_events_window as (
  select
    e.company,
    e.aff_lookup,
    count(*)::bigint as events_last_2_months,
    max(e.date_created) as last_seen_at
  from public.v_forms_events e
  cross join month_bounds b
  where timezone('America/Los_Angeles', e.date_created)
        >= b.start_of_previous_month_local
    and timezone('America/Los_Angeles', e.date_created)
        < b.start_of_next_month_local
  group by e.company, e.aff_lookup
),
-- Aggregate coreg facts in current + previous month window (by pacific_date).
coreg_events_window as (
  select
    f.company,
    f.aff_lookup,
    sum(f.leads)::bigint as events_last_2_months,
    (timezone('America/Los_Angeles', max(f.pacific_date)::timestamp) at time zone 'UTC')
      as last_seen_at
  from public.coreg_facts f
  cross join month_bounds b
  where f.pacific_date >= b.start_of_previous_month_local::date
    and f.pacific_date < b.start_of_next_month_local::date
  group by f.company, f.aff_lookup
),
events_window as (
  select company, aff_lookup, events_last_2_months, last_seen_at
  from forms_events_window
  union all
  select company, aff_lookup, events_last_2_months, last_seen_at
  from coreg_events_window
)
select
  m.company::text as company,
  m.aff_lookup::text as aff_lookup,
  (m.company::text || m.aff_lookup::text) as campaign_key,
  m.publisher::text as publisher,
  m.campaign::text as campaign,
  coalesce(w.events_last_2_months, 0)::bigint as events_last_2_months,
  w.last_seen_at as last_seen_at,
  (coalesce(w.events_last_2_months, 0) > 0) as is_active_computed,
  m.is_active_override as is_active_override,
  coalesce(m.is_active_override, (coalesce(w.events_last_2_months, 0) > 0))
    as is_active_effective,
  m.affiliate_manager::text as affiliate_manager
from public.myprecious m
left join events_window w
  on w.company = m.company
 and w.aff_lookup = m.aff_lookup;

-- Unique index required for CONCURRENTLY refresh (assumes no duplicates).
create unique index if not exists mv_campaign_activity_uniq_idx
  on public.mv_campaign_activity (company, aff_lookup);

-- Scheduled refresh (every 15 minutes).
-- Requires pg_cron extension and runs outside transactions.
select cron.schedule(
  'mv_campaign_activity_refresh_15m',
  '*/15 * * * *',
  $$refresh materialized view concurrently public.mv_campaign_activity;$$
);
