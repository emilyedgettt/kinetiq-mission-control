-- refresh_coreg_facts
-- Purpose: Incrementally refresh coreg_facts for a small date window.
create or replace function public.refresh_coreg_facts(
  p_start_date date default (timezone('America/Los_Angeles', now())::date - 3),
  p_end_date date default timezone('America/Los_Angeles', now())::date
) returns void
language plpgsql
as $$
declare
  v_start_ts timestamptz;
  v_end_ts timestamptz;
begin
  v_start_ts := (p_start_date::timestamp at time zone 'America/Los_Angeles');
  v_end_ts := ((p_end_date + 1)::timestamp at time zone 'America/Los_Angeles');

  insert into public.coreg_facts (
    company,
    aff_lookup,
    campaign_key,
    pacific_date,
    block_index,
    leads,
    updated_at
  )
  select
    company,
    aff_lookup,
    (company || aff_lookup) as campaign_key,
    pacific_date,
    block_index,
    count(*)::bigint as leads,
    now() as updated_at
  from (
    -- TRA history
    select
      'DATA'::text as company,
      c.aff_id::text as aff_lookup,
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
    where c.date_created >= v_start_ts
      and c.date_created < v_end_ts

    union all

    -- TRA 2026
    select
      'DATA'::text as company,
      c.aff_id::text as aff_lookup,
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
    where c.date_created >= v_start_ts
      and c.date_created < v_end_ts

    union all

    -- DRA history
    select
      'DEBT'::text as company,
      c.aff_id::text as aff_lookup,
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
    where c.date_created >= v_start_ts
      and c.date_created < v_end_ts

    union all

    -- DRA 2026
    select
      'DEBT'::text as company,
      c.aff_id::text as aff_lookup,
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
    where c.date_created >= v_start_ts
      and c.date_created < v_end_ts
  ) s
  group by company, aff_lookup, pacific_date, block_index
  on conflict (company, aff_lookup, pacific_date, block_index)
  do update set
    leads = excluded.leads,
    updated_at = excluded.updated_at;
end;
$$;
