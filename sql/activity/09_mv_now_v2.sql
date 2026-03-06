-- mv_now_v2
-- Purpose: Intraday ingredients for Lovable (forms + coreg).
create materialized view public.mv_now_v2 as
with
time_ctx as (
  select
    timezone('America/Los_Angeles', now()) as pacific_now,
    date(timezone('America/Los_Angeles', now())) as today_pacific_date,
    extract(hour from timezone('America/Los_Angeles', now())) as current_hour
),
blocks as (
  select
    today_pacific_date,
    case
      when current_hour < 4 then 0
      when current_hour < 8 then 1
      when current_hour < 12 then 2
      when current_hour < 16 then 3
      when current_hour < 20 then 4
      else 5
    end as current_block_index,
    ((
      case
        when current_hour < 4 then 0
        when current_hour < 8 then 1
        when current_hour < 12 then 2
        when current_hour < 16 then 3
        when current_hour < 20 then 4
        else 5
      end
    ) + 1) / 6.0 as blocks_fraction
  from time_ctx
),
campaigns as (
  select
    case
      when lower(company) in ('tra', 'gt', 'dra') then 'forms'
      else 'coreg'
    end::text as traffic_type,
    company,
    aff_lookup,
    campaign_key,
    publisher,
    campaign,
    affiliate_manager,
    is_active_effective
  from public.mv_campaign_activity
),
daily_union as (
  select
    'forms'::text as traffic_type,
    company,
    aff_lookup,
    campaign_key,
    pacific_date,
    lead_count
  from public.v_forms_daily
  union all
  select
    'coreg'::text as traffic_type,
    company,
    aff_lookup,
    campaign_key,
    pacific_date,
    lead_count
  from public.v_coreg_daily
),
daily_stats as (
  select
    traffic_type,
    company,
    aff_lookup,
    campaign_key,
    min(pacific_date) as first_seen_pacific_date,
    sum(lead_count)::bigint as lifetime_total
  from daily_union
  group by traffic_type, company, aff_lookup, campaign_key
),
blocks_union as (
  select
    'forms'::text as traffic_type,
    company,
    aff_lookup,
    campaign_key,
    pacific_date,
    block_index,
    lead_count
  from public.v_forms_blocks
  union all
  select
    'coreg'::text as traffic_type,
    company,
    aff_lookup,
    campaign_key,
    pacific_date,
    block_index,
    lead_count
  from public.v_coreg_blocks
),
today_rollup as (
  select
    bu.traffic_type,
    bu.company,
    bu.aff_lookup,
    bu.campaign_key,
    sum(
      case when bu.block_index <= b.current_block_index then bu.lead_count else 0 end
    )::bigint as today_so_far,
    sum(
      case when bu.block_index = b.current_block_index then bu.lead_count else 0 end
    )::bigint as current_block_count
  from blocks_union bu
  cross join blocks b
  where bu.pacific_date = b.today_pacific_date
  group by bu.traffic_type, bu.company, bu.aff_lookup, bu.campaign_key
)
select
  c.traffic_type,
  c.company::text as company,
  c.aff_lookup::text as aff_lookup,
  c.campaign_key::text as campaign_key,
  c.publisher::text as publisher,
  c.campaign::text as campaign,
  c.affiliate_manager::text as affiliate_manager,
  c.is_active_effective as is_active_effective,
  b.today_pacific_date as today_pacific_date,
  b.current_block_index as current_block_index,
  b.blocks_fraction as blocks_fraction,
  coalesce(tr.today_so_far, 0)::bigint as today_so_far,
  coalesce(tr.current_block_count, 0)::bigint as current_block_count,
  case
    when ds.first_seen_pacific_date is null then 0::numeric
    else ds.lifetime_total::numeric
      / nullif((b.today_pacific_date - ds.first_seen_pacific_date + 1), 0)
  end as lifetime_daily_avg
from campaigns c
cross join blocks b
left join daily_stats ds
  on ds.traffic_type = c.traffic_type
 and ds.company = c.company
 and ds.aff_lookup = c.aff_lookup
 and ds.campaign_key = c.campaign_key
left join today_rollup tr
  on tr.traffic_type = c.traffic_type
 and tr.company = c.company
 and tr.aff_lookup = c.aff_lookup
 and tr.campaign_key = c.campaign_key
with no data;

create unique index if not exists mv_now_v2_uniq_idx
  on public.mv_now_v2 (traffic_type, company, aff_lookup);

select cron.schedule(
  'mv_now_v2_refresh_10m',
  '*/10 * * * *',
  $$refresh materialized view concurrently public.mv_now_v2;$$
);

refresh materialized view public.mv_now_v2;
