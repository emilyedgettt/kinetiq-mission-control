-- v_coreg_pacing_status
-- Purpose: Dashboard-ready pacing/status for coreg campaigns.
create or replace view public.v_coreg_pacing_status as
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
    case
      when current_hour < 4 then 0
      when current_hour < 8 then 1
      when current_hour < 12 then 2
      when current_hour < 16 then 3
      when current_hour < 20 then 4
      else 5
    end as blocks_elapsed,
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
coreg_campaigns as (
  select
    company,
    aff_lookup,
    campaign_key,
    publisher,
    campaign,
    affiliate_manager,
    is_active_effective
  from public.mv_campaign_activity
  where company in ('DATA', 'DEBT')
),
daily_today as (
  select
    company,
    aff_lookup,
    pacific_date,
    lead_count
  from public.v_coreg_daily
),
block_today as (
  select
    company,
    aff_lookup,
    pacific_date,
    block_index,
    lead_count
  from public.v_coreg_blocks
)
select
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
  coalesce(d.lead_count, 0)::bigint as today_so_far,
  coalesce(bl.lead_count, 0)::bigint as current_block_count,
  coalesce(base.baseline_daily_avg, 0::numeric) as baseline_daily_avg,
  (coalesce(base.baseline_daily_avg, 0::numeric) * b.blocks_fraction) as expected_by_now,
  (coalesce(d.lead_count, 0)::numeric
    - (coalesce(base.baseline_daily_avg, 0::numeric) * b.blocks_fraction)) as delta_vs_expected,
  case
    when (coalesce(base.baseline_daily_avg, 0::numeric) * b.blocks_fraction) > 0
      then coalesce(d.lead_count, 0)::numeric
           / (coalesce(base.baseline_daily_avg, 0::numeric) * b.blocks_fraction)
    else null
  end as pace_ratio,
  case
    when (coalesce(base.baseline_daily_avg, 0::numeric) * b.blocks_fraction) = 0
      then case
        when coalesce(d.lead_count, 0) = 0 then 'no baseline yet'
        else 'exceeding expectations'
      end
    when (coalesce(d.lead_count, 0)::numeric
          / (coalesce(base.baseline_daily_avg, 0::numeric) * b.blocks_fraction)) < 0.50
      then 'dangerously low'
    when (coalesce(d.lead_count, 0)::numeric
          / (coalesce(base.baseline_daily_avg, 0::numeric) * b.blocks_fraction)) < 0.80
      then 'somewhat disappointing'
    when (coalesce(d.lead_count, 0)::numeric
          / (coalesce(base.baseline_daily_avg, 0::numeric) * b.blocks_fraction)) < 1.20
      then 'on track'
    when (coalesce(d.lead_count, 0)::numeric
          / (coalesce(base.baseline_daily_avg, 0::numeric) * b.blocks_fraction)) < 1.60
      then 'exceeding expectations'
    else 'GOAT STATUS'
  end as status_label
from coreg_campaigns c
cross join blocks b
left join daily_today d
  on d.company = c.company
 and d.aff_lookup = c.aff_lookup
 and d.pacific_date = b.today_pacific_date
left join block_today bl
  on bl.company = c.company
 and bl.aff_lookup = c.aff_lookup
 and bl.pacific_date = b.today_pacific_date
 and bl.block_index = b.current_block_index
left join public.v_coreg_baselines base
  on base.company = c.company
 and base.aff_lookup = c.aff_lookup
 and base.campaign_key = c.campaign_key;
