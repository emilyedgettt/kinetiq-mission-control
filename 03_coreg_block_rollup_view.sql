-- v_coreg_blocks
-- Purpose: Canonical 4-hour coreg rollups (facts-backed).
-- Note: Dashboards/Lovable should query this view, not raw coreg tables.
create or replace view public.v_coreg_blocks as
select
  company::text as company,
  aff_lookup::text as aff_lookup,
  (company::text || aff_lookup::text) as campaign_key,
  pacific_date,
  block_index,
  case
    when block_index = 0 then '12am–4am'
    when block_index = 1 then '4am–8am'
    when block_index = 2 then '8am–12pm'
    when block_index = 3 then '12pm–4pm'
    when block_index = 4 then '4pm–8pm'
    else '8pm–12am'
  end as block_label,
  leads::bigint as lead_count
from public.coreg_facts;
