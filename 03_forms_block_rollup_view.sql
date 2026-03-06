-- v_forms_blocks
-- Purpose: 4-hour lead blocks in America/Los_Angeles time.
create or replace view public.v_forms_blocks as
with events_local as (
  select
    company,
    aff_lookup,
    timezone('America/Los_Angeles', date_created) as pacific_ts
  from public.v_forms_events
),
events_blocked as (
  select
    company,
    aff_lookup,
    pacific_ts,
    date(pacific_ts) as pacific_date,
    extract(hour from pacific_ts) as pacific_hour
  from events_local
)
select
  company::text as company,
  aff_lookup::text as aff_lookup,
  (company::text || aff_lookup::text) as campaign_key,
  pacific_date,
  case
    when pacific_hour between 0 and 3 then 0
    when pacific_hour between 4 and 7 then 1
    when pacific_hour between 8 and 11 then 2
    when pacific_hour between 12 and 15 then 3
    when pacific_hour between 16 and 19 then 4
    else 5
  end as block_index,
  case
    when pacific_hour between 0 and 3 then '12am–4am'
    when pacific_hour between 4 and 7 then '4am–8am'
    when pacific_hour between 8 and 11 then '8am–12pm'
    when pacific_hour between 12 and 15 then '12pm–4pm'
    when pacific_hour between 16 and 19 then '4pm–8pm'
    else '8pm–12am'
  end as block_label,
  count(*)::bigint as lead_count
from events_blocked
group by
  company,
  aff_lookup,
  campaign_key,
  pacific_date,
  block_index,
  block_label;
