-- v_forms_events
-- Purpose: Normalize form/lead events across TRA, GT, and DRA
-- Source tables:
--   - public."tra_5-0"
--   - public.tra_forms_all
--   - public."gt_5-0"
--   - public."dra_5-0"
-- Campaign key logic:
--   campaign_key = company || aff_lookup
--   aff_lookup = aff_id || off_id (no separator)
-- Notes:
--   - Coreg events are handled separately in v_coreg_events
--   - No filtering or deduplication performed

create or replace view public.v_forms_events as
-- Source: public."tra_5-0"
select
  'tra'::text as company,
  case
    when nullif(btrim(off_id::text), '') is null
      then btrim(aff_id::text)
    else btrim(aff_id::text) || btrim(off_id::text)
  end as aff_lookup,
  date_created::timestamptz as date_created
from public."tra_5-0"

union all

-- Source: public.tra_forms_all
select
  'tra'::text as company,
  case
    when nullif(btrim(off_id::text), '') is null
      then btrim(aff_id::text)
    else btrim(aff_id::text) || btrim(off_id::text)
  end as aff_lookup,
  date_created::timestamptz as date_created
from public.tra_forms_all

union all

-- Source: public."gt_5-0"
select
  'gt'::text as company,
  case
    when nullif(btrim(off_id::text), '') is null
      then btrim(aff_id::text)
    else btrim(aff_id::text) || btrim(off_id::text)
  end as aff_lookup,
  date_created::timestamptz as date_created
from public."gt_5-0"

union all

-- Source: public."dra_5-0"
select
  'dra'::text as company,
  case
    when nullif(btrim(off_id::text), '') is null
      then btrim(aff_id::text)
    else btrim(aff_id::text) || btrim(off_id::text)
  end as aff_lookup,
  date_created::timestamptz as date_created
from public."dra_5-0";
