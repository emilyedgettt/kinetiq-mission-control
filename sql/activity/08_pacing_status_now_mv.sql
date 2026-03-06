-- mv_pacing_status_now
-- Purpose: Materialized "Now" pacing dataset for Lovable (active + today only).
create materialized view public.mv_pacing_status_now as
select
  traffic_type,
  company,
  aff_lookup,
  campaign_key,
  publisher,
  campaign,
  affiliate_manager,
  is_active_effective,
  today_pacific_date,
  current_block_index,
  blocks_fraction,
  today_so_far,
  current_block_count,
  baseline_daily_avg,
  expected_by_now,
  delta_vs_expected,
  pace_ratio,
  status_label
from public.v_pacing_status_all
where is_active_effective = true
  and today_pacific_date = date(timezone('America/Los_Angeles', now()));

-- Unique index required for CONCURRENT refresh.
create unique index if not exists mv_pacing_status_now_uniq_idx
  on public.mv_pacing_status_now (traffic_type, company, aff_lookup, campaign_key);

-- Conservative supporting indexes for Lovable filters.
create index if not exists mv_pacing_status_now_affiliate_manager_idx
  on public.mv_pacing_status_now (affiliate_manager);

create index if not exists mv_pacing_status_now_company_idx
  on public.mv_pacing_status_now (company);

create index if not exists mv_pacing_status_now_status_label_idx
  on public.mv_pacing_status_now (status_label);

-- Scheduled refresh (every 10 minutes).
select cron.schedule(
  'mv_pacing_status_now_refresh_10m',
  '*/10 * * * *',
  $$refresh materialized view concurrently public.mv_pacing_status_now;$$
);
