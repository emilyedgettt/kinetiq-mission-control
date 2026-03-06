-- v_forms_baselines
-- Purpose: Baseline daily averages per campaign.
create or replace view public.v_forms_baselines as
with ranked_daily as (
  select
    company,
    aff_lookup,
    campaign_key,
    pacific_date,
    lead_count,
    row_number() over (
      partition by company, aff_lookup, campaign_key
      order by pacific_date desc
    ) as recency_rank
  from public.v_forms_daily
)
select
  company::text as company,
  aff_lookup::text as aff_lookup,
  campaign_key::text as campaign_key,
  min(pacific_date) as first_seen_date,
  max(pacific_date) as last_seen_date,
  count(*)::int as days_with_data,
  round(avg(lead_count), 0) as avg_daily_since_first_seen,
  round(avg(case when recency_rank <= 7 then lead_count end), 0) as avg_daily_last_7_days,
  round(avg(case when recency_rank <= 30 then lead_count end), 0) as avg_daily_last_30_days,
  coalesce(
    round(avg(case when recency_rank <= 30 then lead_count end), 0),
    round(avg(case when recency_rank <= 7 then lead_count end), 0),
    round(avg(lead_count), 0),
    0::numeric
  ) as baseline_daily_avg
from ranked_daily
group by company, aff_lookup, campaign_key;
