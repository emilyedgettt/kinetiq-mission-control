-- v_coreg_events
-- Purpose: Normalize coreg events and attach DATA/DEBT company
-- Source tables:
--   - public.tra_coreg_all
--   - public.tra_coreg2026
--   - public.dracoreg2026
--   - public.dra_coreg_all
-- Company derived from public.myprecious (DATA/DEBT)

create or replace view public.v_coreg_events as
-- Source: public.tra_coreg_all
select
  m.company::text as company,
  c.aff_id::text as aff_lookup,
  c.date_created as date_created,
  date(timezone('America/Los_Angeles', c.date_created)) as pacific_date,
  case
    when extract(hour from timezone('America/Los_Angeles', c.date_created)) between 0 and 3 then 0
    when extract(hour from timezone('America/Los_Angeles', c.date_created)) between 4 and 7 then 1
    when extract(hour from timezone('America/Los_Angeles', c.date_created)) between 8 and 11 then 2
    when extract(hour from timezone('America/Los_Angeles', c.date_created)) between 12 and 15 then 3
    when extract(hour from timezone('America/Los_Angeles', c.date_created)) between 16 and 19 then 4
    else 5
  end as block_index
from public.tra_coreg_all c
join public.myprecious m
  on m.aff_lookup = c.aff_id
 and m.company in ('DATA', 'DEBT')

union all

-- Source: public.tra_coreg2026
select
  m.company::text as company,
  c.aff_id::text as aff_lookup,
  c.date_created as date_created,
  date(timezone('America/Los_Angeles', c.date_created)) as pacific_date,
  case
    when extract(hour from timezone('America/Los_Angeles', c.date_created)) between 0 and 3 then 0
    when extract(hour from timezone('America/Los_Angeles', c.date_created)) between 4 and 7 then 1
    when extract(hour from timezone('America/Los_Angeles', c.date_created)) between 8 and 11 then 2
    when extract(hour from timezone('America/Los_Angeles', c.date_created)) between 12 and 15 then 3
    when extract(hour from timezone('America/Los_Angeles', c.date_created)) between 16 and 19 then 4
    else 5
  end as block_index
from public.tra_coreg2026 c
join public.myprecious m
  on m.aff_lookup = c.aff_id
 and m.company in ('DATA', 'DEBT')

union all

-- Source: public.dracoreg2026
select
  m.company::text as company,
  c.aff_id::text as aff_lookup,
  c.date_created as date_created,
  date(timezone('America/Los_Angeles', c.date_created)) as pacific_date,
  case
    when extract(hour from timezone('America/Los_Angeles', c.date_created)) between 0 and 3 then 0
    when extract(hour from timezone('America/Los_Angeles', c.date_created)) between 4 and 7 then 1
    when extract(hour from timezone('America/Los_Angeles', c.date_created)) between 8 and 11 then 2
    when extract(hour from timezone('America/Los_Angeles', c.date_created)) between 12 and 15 then 3
    when extract(hour from timezone('America/Los_Angeles', c.date_created)) between 16 and 19 then 4
    else 5
  end as block_index
from public.dracoreg2026 c
join public.myprecious m
  on m.aff_lookup = c.aff_id
 and m.company in ('DATA', 'DEBT')

union all

-- Source: public.dra_coreg_all
select
  m.company::text as company,
  c.aff_id::text as aff_lookup,
  c.date_created as date_created,
  date(timezone('America/Los_Angeles', c.date_created)) as pacific_date,
  case
    when extract(hour from timezone('America/Los_Angeles', c.date_created)) between 0 and 3 then 0
    when extract(hour from timezone('America/Los_Angeles', c.date_created)) between 4 and 7 then 1
    when extract(hour from timezone('America/Los_Angeles', c.date_created)) between 8 and 11 then 2
    when extract(hour from timezone('America/Los_Angeles', c.date_created)) between 12 and 15 then 3
    when extract(hour from timezone('America/Los_Angeles', c.date_created)) between 16 and 19 then 4
    else 5
  end as block_index
from public.dra_coreg_all c
join public.myprecious m
  on m.aff_lookup = c.aff_id
 and m.company in ('DATA', 'DEBT');
