-- mv_ytd_yoy_v1
-- Purpose: YTD Year-Over-Year completed metrics using daily rollups only.

create materialized view public.mv_ytd_yoy_v1 as
with
time_ctx as (
  select
    timezone('America/Los_Angeles', now())::date as today_pt,
    (timezone('America/Los_Angeles', now())::date - 1) as yesterday_pt
),
date_ctx as (
  select
    yesterday_pt as this_ytd_end,
    date_trunc('year', yesterday_pt::timestamp)::date as this_ytd_start
  from time_ctx
),
window_ctx as (
  select
    this_ytd_start,
    this_ytd_end,
    (this_ytd_end - this_ytd_start + 1)::int as ytd_days,
    (this_ytd_start - interval '1 year')::date as last_ytd_start
  from date_ctx
),
range_ctx as (
  select
    this_ytd_start,
    this_ytd_end,
    ytd_days,
    last_ytd_start,
    (last_ytd_start + (ytd_days - 1))::date as last_ytd_end
  from window_ctx
),
this_ytd as (
  select
    d.company,
    d.aff_lookup,
    d.campaign_key,
    sum(d.lead_count)::bigint as acquired_ytd_this_year
  from public.v_all_daily_v2 d
  cross join range_ctx r
  where d.pacific_date between r.this_ytd_start and r.this_ytd_end
  group by d.company, d.aff_lookup, d.campaign_key
),
last_ytd as (
  select
    d.company,
    d.aff_lookup,
    d.campaign_key,
    sum(d.lead_count)::bigint as acquired_ytd_last_year
  from public.v_all_daily_v2 d
  cross join range_ctx r
  where d.pacific_date between r.last_ytd_start and r.last_ytd_end
  group by d.company, d.aff_lookup, d.campaign_key
),
combined as (
  select
    coalesce(t.company, l.company) as company,
    coalesce(t.aff_lookup, l.aff_lookup) as aff_lookup,
    coalesce(t.campaign_key, l.campaign_key) as campaign_key,
    t.acquired_ytd_this_year,
    l.acquired_ytd_last_year
  from this_ytd t
  full join last_ytd l
    on l.company = t.company
   and l.aff_lookup = t.aff_lookup
   and l.campaign_key = t.campaign_key
)
select
  c.company::text as company,
  c.aff_lookup::text as aff_lookup,
  c.campaign_key::text as campaign_key,
  r.ytd_days,
  r.this_ytd_start,
  r.this_ytd_end,
  r.last_ytd_start,
  r.last_ytd_end,
  coalesce(c.acquired_ytd_this_year, 0)::bigint as acquired_ytd_this_year,
  c.acquired_ytd_last_year,
  case
    when c.acquired_ytd_last_year is null or c.acquired_ytd_last_year = 0 then null
    else c.acquired_ytd_last_year::numeric / nullif(r.ytd_days, 0)
  end as baseline_yoy_daily_avg,
  c.acquired_ytd_last_year as expected_yoy_total,
  case
    when c.acquired_ytd_last_year is null then null
    else coalesce(c.acquired_ytd_this_year, 0)::bigint - c.acquired_ytd_last_year
  end as delta_yoy,
  case
    when c.acquired_ytd_last_year is null or c.acquired_ytd_last_year = 0 then null
    else coalesce(c.acquired_ytd_this_year, 0)::numeric / c.acquired_ytd_last_year
  end as velocity_yoy,
  now() as updated_at
from combined c
cross join range_ctx r;

create unique index if not exists mv_ytd_yoy_v1_uniq_idx
  on public.mv_ytd_yoy_v1 (company, campaign_key, aff_lookup);

select cron.schedule(
  'mv_ytd_yoy_v1_refresh_daily',
  '30 8 * * *',
  $$refresh materialized view concurrently public.mv_ytd_yoy_v1;$$
);

