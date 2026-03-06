-- v_forms_daily
-- Purpose: Daily lead counts in America/Los_Angeles time.
create or replace view public.v_forms_daily as
select
  company::text as company,
  aff_lookup::text as aff_lookup,
  (company::text || aff_lookup::text) as campaign_key,
  date(timezone('America/Los_Angeles', date_created)) as pacific_date,
  count(*)::bigint as lead_count
from public.v_forms_events
group by
  company,
  aff_lookup,
  campaign_key,
  pacific_date;
