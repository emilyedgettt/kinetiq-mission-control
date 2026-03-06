-- v_all_daily_v2 + mv_lifecycle_averages_v1
-- Purpose: Unified daily rollup + lifecycle averages from daily rollups only.

create or replace view public.v_all_daily_v2 as
select
  company::text as company,
  aff_lookup::text as aff_lookup,
  campaign_key::text as campaign_key,
  pacific_date::date as pacific_date,
  lead_count::bigint as lead_count,
  'forms'::text as traffic_type
from public.v_forms_daily

union all

select
  company::text as company,
  aff_lookup::text as aff_lookup,
  campaign_key::text as campaign_key,
  pacific_date::date as pacific_date,
  lead_count::bigint as lead_count,
  'coreg'::text as traffic_type
from public.v_coreg_daily;


create materialized view public.mv_lifecycle_averages_v1 as
with
time_ctx as (
  select
    timezone('America/Los_Angeles', now())::date as today_pt,
    (timezone('America/Los_Angeles', now())::date - 1) as yesterday_pt,
    date_trunc('week', timezone('America/Los_Angeles', now())::date::timestamp)::date as this_week_start,
    date_trunc('month', timezone('America/Los_Angeles', now())::date::timestamp)::date as this_month_start
),
lifecycle_days as (
  select
    d.company,
    d.aff_lookup,
    d.campaign_key,
    sum(d.lead_count)::bigint as acquired_lifecycle_completed,
    count(*)::int as days_completed
  from public.v_all_daily_v2 d
  cross join time_ctx t
  where d.pacific_date <= t.yesterday_pt
  group by d.company, d.aff_lookup, d.campaign_key
),
weekly_totals as (
  select
    d.company,
    d.campaign_key,
    date_trunc('week', d.pacific_date::timestamp)::date as week_start,
    sum(d.lead_count)::bigint as week_total
  from public.v_all_daily_v2 d
  cross join time_ctx t
  where d.pacific_date < t.this_week_start
  group by d.company, d.campaign_key, week_start
),
weekly_avg as (
  select
    company,
    campaign_key,
    avg(week_total)::numeric as weekly_avg
  from weekly_totals
  group by company, campaign_key
),
monthly_totals as (
  select
    d.company,
    d.campaign_key,
    date_trunc('month', d.pacific_date::timestamp)::date as month_start,
    sum(d.lead_count)::bigint as month_total
  from public.v_all_daily_v2 d
  cross join time_ctx t
  where d.pacific_date < t.this_month_start
  group by d.company, d.campaign_key, month_start
),
monthly_avg as (
  select
    company,
    campaign_key,
    avg(month_total)::numeric as monthly_avg
  from monthly_totals
  group by company, campaign_key
)
select
  l.company::text as company,
  l.aff_lookup::text as aff_lookup,
  l.campaign_key::text as campaign_key,
  l.acquired_lifecycle_completed,
  (l.acquired_lifecycle_completed::numeric / nullif(l.days_completed, 0)) as daily_avg,
  w.weekly_avg as weekly_avg,
  m.monthly_avg as monthly_avg,
  now() as updated_at
from lifecycle_days l
left join weekly_avg w
  on w.company = l.company
 and w.campaign_key = l.campaign_key
left join monthly_avg m
  on m.company = l.company
 and m.campaign_key = l.campaign_key;

-- Unique index required for CONCURRENTLY refresh.
create unique index concurrently if not exists mv_lifecycle_averages_v1_uniq_idx
  on public.mv_lifecycle_averages_v1 (company, campaign_key, aff_lookup);

create index concurrently if not exists mv_lifecycle_averages_v1_company_campaign_key_idx
  on public.mv_lifecycle_averages_v1 (company, campaign_key);

create index concurrently if not exists mv_lifecycle_averages_v1_company_aff_lookup_idx
  on public.mv_lifecycle_averages_v1 (company, aff_lookup);

-- Scheduled refresh (daily shortly after midnight PT; 08:20 UTC).
select cron.schedule(
  'mv_lifecycle_averages_v1_refresh_daily',
  '20 8 * * *',
  $$refresh materialized view concurrently public.mv_lifecycle_averages_v1;$$
);
