-- mv_periods_v2
-- Purpose: Completed-window ingredients for Lovable (forms + coreg).
drop materialized view if exists public.mv_periods_v2;
create materialized view public.mv_periods_v2 as
with
time_ctx as (
  select
    date(timezone('America/Los_Angeles', now())) as today_pacific_date,
    date(timezone('America/Los_Angeles', now())) - 1 as yesterday_date,
    date_trunc('year', date(timezone('America/Los_Angeles', now())))::date as jan1,
    (date(timezone('America/Los_Angeles', now())) - 1)
      - date_trunc('year', date(timezone('America/Los_Angeles', now())))::date + 1
      as ytd_days
),
period_bounds as (
  select
    today_pacific_date,
    yesterday_date,
    jan1,
    ytd_days,
    (jan1 - ytd_days) as prior_ytd_start,
    (jan1 - 1) as prior_ytd_end
  from time_ctx
),
campaigns as (
  select
    case
      when lower(company) in ('tra', 'gt', 'dra') then 'forms'
      else 'coreg'
    end::text as traffic_type,
    company,
    aff_lookup,
    campaign_key,
    publisher,
    campaign,
    affiliate_manager,
    is_active_effective
  from public.mv_campaign_activity
),
daily_union as (
  select
    'forms'::text as traffic_type,
    company,
    aff_lookup,
    campaign_key,
    pacific_date,
    lead_count
  from public.v_forms_daily
  union all
  select
    'coreg'::text as traffic_type,
    company,
    aff_lookup,
    campaign_key,
    pacific_date,
    lead_count
  from public.v_coreg_daily
),
daily_agg as (
  select
    d.traffic_type,
    d.company,
    d.aff_lookup,
    d.campaign_key,
    min(d.pacific_date) as first_seen_pacific_date,
    sum(case when d.pacific_date <= p.yesterday_date then d.lead_count else 0 end)::bigint
      as lifetime_total_completed,
    sum(case when d.pacific_date = p.yesterday_date then d.lead_count else 0 end)::bigint
      as acquired_yesterday,
    sum(case when d.pacific_date between p.today_pacific_date - 7 and p.today_pacific_date - 1
        then d.lead_count else 0 end)::bigint as acquired_7,
    sum(case when d.pacific_date between p.today_pacific_date - 30 and p.today_pacific_date - 1
        then d.lead_count else 0 end)::bigint as acquired_30,
    sum(case when d.pacific_date between p.today_pacific_date - 45 and p.today_pacific_date - 1
        then d.lead_count else 0 end)::bigint as acquired_45,
    sum(case when d.pacific_date between p.today_pacific_date - 90 and p.today_pacific_date - 1
        then d.lead_count else 0 end)::bigint as acquired_90,
    sum(case when d.pacific_date between p.jan1 and p.yesterday_date
        then d.lead_count else 0 end)::bigint as acquired_ytd_completed,
    sum(case when d.pacific_date between p.prior_ytd_start and p.prior_ytd_end
        then d.lead_count else 0 end)::bigint as prior_ytd_total,
    sum(case when d.pacific_date = p.today_pacific_date - 2
        then d.lead_count else 0 end)::bigint as prior_yesterday_total,
    sum(case when d.pacific_date between p.today_pacific_date - 14 and p.today_pacific_date - 8
        then d.lead_count else 0 end)::bigint as prior_7_total,
    sum(case when d.pacific_date between p.today_pacific_date - 60 and p.today_pacific_date - 31
        then d.lead_count else 0 end)::bigint as prior_30_total,
    sum(case when d.pacific_date between p.today_pacific_date - 90 and p.today_pacific_date - 46
        then d.lead_count else 0 end)::bigint as prior_45_total,
    sum(case when d.pacific_date between p.today_pacific_date - 180 and p.today_pacific_date - 91
        then d.lead_count else 0 end)::bigint as prior_90_total
  from daily_union d
  cross join period_bounds p
  group by d.traffic_type, d.company, d.aff_lookup, d.campaign_key
)
select
  c.traffic_type,
  c.company::text as company,
  c.aff_lookup::text as aff_lookup,
  c.campaign_key::text as campaign_key,
  c.publisher::text as publisher,
  c.campaign::text as campaign,
  c.affiliate_manager::text as affiliate_manager,
  c.is_active_effective as is_active_effective,

  p.today_pacific_date as today_pacific_date,
  p.ytd_days as ytd_days, -- expose it

  coalesce(a.acquired_yesterday, 0)::bigint as acquired_yesterday,
  coalesce(a.acquired_7, 0)::bigint as acquired_7,
  coalesce(a.acquired_30, 0)::bigint as acquired_30,
  coalesce(a.acquired_45, 0)::bigint as acquired_45,
  coalesce(a.acquired_90, 0)::bigint as acquired_90,
  coalesce(a.acquired_ytd_completed, 0)::bigint as acquired_ytd_completed,
  coalesce(a.lifetime_total_completed, 0)::bigint as acquired_lifecycle_completed,

  -- baseline daily avg
  case
    when a.first_seen_pacific_date is null then 0::numeric
    else a.lifetime_total_completed::numeric
      / nullif((p.yesterday_date - a.first_seen_pacific_date + 1), 0)
  end as baseline_lifetime_daily_avg,

  -- prior totals (completed)
  coalesce(a.prior_yesterday_total, 0)::bigint as prior_yesterday_total,
  coalesce(a.prior_7_total, 0)::bigint as prior_7_total,
  coalesce(a.prior_30_total, 0)::bigint as prior_30_total,
  coalesce(a.prior_45_total, 0)::bigint as prior_45_total,
  coalesce(a.prior_90_total, 0)::bigint as prior_90_total,

  -- matching baseline daily avgs
  (coalesce(a.prior_yesterday_total, 0)::numeric / 1) as baseline_prior_yesterday_avg,
  (coalesce(a.prior_7_total, 0)::numeric / 7) as baseline_prior_7_avg,
  (coalesce(a.prior_30_total, 0)::numeric / 30) as baseline_prior_30_avg,
  (coalesce(a.prior_45_total, 0)::numeric / 45) as baseline_prior_45_avg,
  (coalesce(a.prior_90_total, 0)::numeric / 90) as baseline_prior_90_avg,

  -- eligibility flags
  case
    when a.first_seen_pacific_date is null then false
    else a.first_seen_pacific_date <= (p.today_pacific_date - 2)
  end as eligible_yesterday,
  case
    when a.first_seen_pacific_date is null then false
    else a.first_seen_pacific_date <= (p.today_pacific_date - 14)
  end as eligible_7,
  case
    when a.first_seen_pacific_date is null then false
    else a.first_seen_pacific_date <= (p.today_pacific_date - 60)
  end as eligible_30,
  case
    when a.first_seen_pacific_date is null then false
    else a.first_seen_pacific_date <= (p.today_pacific_date - 90)
  end as eligible_45,
  case
    when a.first_seen_pacific_date is null then false
    else a.first_seen_pacific_date <= (p.today_pacific_date - 180)
  end as eligible_90,

  -- expected YTD total based on lifetime daily avg
  (
    case
      when a.first_seen_pacific_date is null then 0::numeric
      else a.lifetime_total_completed::numeric
        / nullif((p.yesterday_date - a.first_seen_pacific_date + 1), 0)
    end
    * p.ytd_days
  ) as expected_ytd_from_lifetime,

  -- delta + velocity for the YTD vs lifetime-baseline comparison
  (coalesce(a.acquired_ytd_completed, 0)::numeric
    - (
      case
        when a.first_seen_pacific_date is null then 0::numeric
        else a.lifetime_total_completed::numeric
          / nullif((p.yesterday_date - a.first_seen_pacific_date + 1), 0)
      end
      * p.ytd_days
    )
  ) as delta_ytd_vs_lifetime,

  case
    when (
      case
        when a.first_seen_pacific_date is null then 0::numeric
        else a.lifetime_total_completed::numeric
          / nullif((p.yesterday_date - a.first_seen_pacific_date + 1), 0)
      end
      * p.ytd_days
    ) = 0 then null
    else coalesce(a.acquired_ytd_completed, 0)::numeric
      / (
        case
          when a.first_seen_pacific_date is null then 0::numeric
          else a.lifetime_total_completed::numeric
            / nullif((p.yesterday_date - a.first_seen_pacific_date + 1), 0)
        end
        * p.ytd_days
      )
  end as velocity_ytd_vs_lifetime

from campaigns c
cross join period_bounds p
left join daily_agg a
  on a.traffic_type = c.traffic_type
 and a.company = c.company
 and a.aff_lookup = c.aff_lookup
 and a.campaign_key = c.campaign_key
with no data;

refresh materialized view public.mv_periods_v2;
