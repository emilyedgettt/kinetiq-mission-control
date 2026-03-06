-- v_pacing_status_all
-- Purpose: Unified pacing dataset for Lovable (forms + coreg).
create or replace view public.v_pacing_status_all as
select
  'forms'::text as traffic_type,
  lower(company)::text as company,
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
from public.v_forms_pacing_status

union all

select
  'coreg'::text as traffic_type,
  lower(company)::text as company,
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
from public.v_coreg_pacing_status;
