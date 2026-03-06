-- Forecast RPC for Lovable: NOW / 7DAY / 30DAY

create or replace function public.rpc_campaign_forecast_v1(
  p_campaign_key text,
  p_company text,
  p_traffic_type text,
  p_mode text  -- 'NOW' | '7DAY' | '30DAY'
) returns jsonb
language plpgsql
as $$
declare
  v_mode text := upper(coalesce(p_mode, 'NOW'));
  pacific_today_date date := date(timezone('America/Los_Angeles', now()));
  week_start date := date_trunc('week', pacific_today_date)::date;
  week_end date := (week_start + 6);
  month_start date := date_trunc('month', pacific_today_date)::date;
  month_end date := (month_start + interval '1 month' - interval '1 day')::date;

  baseline_daily_lifetime numeric;
  acquired_today_so_far bigint;
  acquired_wtd_so_far bigint;
  acquired_mtd_so_far bigint;

  remaining_days_week int;
  remaining_days_month int;

  expected_today_total numeric;
  expected_week_total numeric;
  expected_month_total numeric;

  remaining_needed_total numeric;
  remaining_per_day_needed numeric;
  needed_daily_pace_forward numeric;
  recommendation_pct numeric;
  recommendation_text text;
begin
  select coalesce(baseline_lifetime_daily_avg, 0)
  into baseline_daily_lifetime
  from public.mv_periods_v2
  where traffic_type = p_traffic_type
    and company = p_company
    and campaign_key = p_campaign_key
  limit 1;

  with daily as (
    select pacific_date, lead_count
    from public.v_forms_daily
    where p_traffic_type = 'forms'
      and campaign_key = p_campaign_key
      and company = p_company
      and pacific_date between month_start and pacific_today_date
    union all
    select pacific_date, lead_count
    from public.v_coreg_daily
    where p_traffic_type = 'coreg'
      and campaign_key = p_campaign_key
      and company = p_company
      and pacific_date between month_start and pacific_today_date
  )
  select
    coalesce(sum(case when pacific_date = pacific_today_date then lead_count else 0 end), 0),
    coalesce(sum(case when pacific_date between week_start and pacific_today_date then lead_count else 0 end), 0),
    coalesce(sum(case when pacific_date between month_start and pacific_today_date then lead_count else 0 end), 0)
  into acquired_today_so_far, acquired_wtd_so_far, acquired_mtd_so_far
  from daily;

  remaining_days_week := (week_end - pacific_today_date + 1);
  remaining_days_month := (month_end - pacific_today_date + 1);

  expected_today_total := baseline_daily_lifetime * 1;
  expected_week_total := baseline_daily_lifetime * 7;
  expected_month_total := baseline_daily_lifetime * 30;

  if v_mode = 'NOW' then
    remaining_needed_total := greatest(0, expected_today_total - acquired_today_so_far);
    remaining_per_day_needed := null;
    needed_daily_pace_forward := null;
    recommendation_pct := remaining_needed_total / nullif(expected_today_total, 0);
  elsif v_mode = '7DAY' then
    remaining_needed_total := greatest(0, expected_week_total - acquired_wtd_so_far);
    remaining_per_day_needed := remaining_needed_total / nullif(remaining_days_week, 0);
    needed_daily_pace_forward := (
      expected_week_total - (acquired_wtd_so_far - acquired_today_so_far)
    ) / nullif(remaining_days_week, 0);
    recommendation_pct := (needed_daily_pace_forward / nullif(baseline_daily_lifetime, 0)) - 1;
  elsif v_mode = '30DAY' then
    remaining_needed_total := greatest(0, expected_month_total - acquired_mtd_so_far);
    remaining_per_day_needed := remaining_needed_total / nullif(remaining_days_month, 0);
    needed_daily_pace_forward := (
      expected_month_total - (acquired_mtd_so_far - acquired_today_so_far)
    ) / nullif(remaining_days_month, 0);
    recommendation_pct := (needed_daily_pace_forward / nullif(baseline_daily_lifetime, 0)) - 1;
  else
    raise exception 'Invalid p_mode: %', p_mode;
  end if;

  if recommendation_pct is null or abs(recommendation_pct) < 0.0001 then
    recommendation_text := 'On pace to hit average';
  elsif recommendation_pct > 0 then
    recommendation_text := 'Increase by '
      || trim(to_char(recommendation_pct * 100, 'FM999999990.00'))
      || '% to hit average';
  else
    recommendation_text := 'Decrease by '
      || trim(to_char(abs(recommendation_pct) * 100, 'FM999999990.00'))
      || '% and still hit average';
  end if;

  return jsonb_build_object(
    'campaign_key', p_campaign_key,
    'company', p_company,
    'traffic_type', p_traffic_type,
    'mode', v_mode,
    'pacific_today_date', pacific_today_date,
    'week_start', week_start,
    'week_end', week_end,
    'month_start', month_start,
    'month_end', month_end,
    'baseline_daily_lifetime', baseline_daily_lifetime,
    'acquired_today_so_far', acquired_today_so_far,
    'acquired_wtd_so_far', acquired_wtd_so_far,
    'acquired_mtd_so_far', acquired_mtd_so_far,
    'expected_today_total', expected_today_total,
    'expected_week_total', expected_week_total,
    'expected_month_total', expected_month_total,
    'remaining_days_week', remaining_days_week,
    'remaining_days_month', remaining_days_month,
    'remaining_needed_total', remaining_needed_total,
    'remaining_per_day_needed', remaining_per_day_needed,
    'needed_daily_pace_forward', needed_daily_pace_forward,
    'recommendation_pct', recommendation_pct,
    'recommendation_text', recommendation_text
  );
end;
$$;

grant execute on function public.rpc_campaign_forecast_v1(text, text, text, text) to authenticated;
