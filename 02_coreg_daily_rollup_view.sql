-- v_coreg_daily
-- Purpose: Canonical daily coreg rollups (facts-backed).
-- Note: Dashboards/Lovable should query this view, not raw coreg tables.
create or replace view public.v_coreg_daily as
select
  company::text as company,
  aff_lookup::text as aff_lookup,
  (company::text || aff_lookup::text) as campaign_key,
  pacific_date,
  sum(leads)::bigint as lead_count
from public.coreg_facts
group by
  company,
  aff_lookup,
  campaign_key,
  pacific_date;
