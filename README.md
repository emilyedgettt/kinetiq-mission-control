## Kinetiq Mission Control – Campaign Performance Dashboard

This repository contains the SQL schema, materialized views, and Supabase Edge
Functions that power the Mission Control dashboard used to monitor campaign
performance and pacing across Kinetiq Media traffic sources.

The system normalizes lead events, aggregates them into Pacific-time rollups,
computes pacing baselines, and exposes forecasting metrics consumed by the
Lovable frontend dashboard.

All analytics logic is designed to be scalable and to avoid direct scanning of
raw lead tables wherever possible.

Timezone standard: America/Los_Angeles



## Design Goals

Mission Control is designed around several core principles:
1. Avoid scanning raw lead tables whenever possible to reduce DB load.
2. Normalize campaign identity across all traffic sources.
3. Standardize analytics to Pacific time.
4. Provide real-time pacing context for campaigns.
5. Support scalable dashboard queries via materialized views.



## High-Level Architecture

Raw Lead Tables -- Source ingestion tables for forms and coreg traffic.
    ↓
Event Normalization Views --  Normalize campaign identifiers and timestamps.
    ↓
Daily + 4-Hour Block Rollups -- Aggregate events into daily and intraday blocks.
    ↓
Campaign Activity Layer -- Determines which campaigns are currently active.
    ↓
Materialized Dashboard Views -- Provide fast dashboard queries for pacing and analytics.
    ↓
Forecast RPC -- Computes pacing forecasts used by the UI.
    ↓
Lovable Dashboard



## Campaign Identity Model

The campaign_key is the canonical identifier used across all
analytics views and dashboard queries. Campaigns are normalized using two identifiers:

aff_lookup
    Combination of affiliate and offer identifiers.

campaign_key
    company || aff_lookup

Examples:

tra + 683      → tra683
gt  + 112      → gt112
DATA + 1905    → DATA1905



## Traffic Types

Mission Control tracks two independent traffic pipelines.
1) Forms traffic
2) Coreg traffic


## Forms Pipeline

Source Tables:
public."tra_5-0"
public."dra_5-0"
public."gt_5-0"

Historical:
public.tra_forms_all
public.dra_forms_all
public.gt_forms_all

Pipeline:
`public.v_forms_events` -- Normalizes form events across TRA, GT, and DRA (3 separate brands serviced by Kinetiq Media).
      ↓
`public.v_forms_daily` -- Aggregates leads per campaign per Pacific day.
      ↓
`public.v_forms_blocks` -- Aggregates leads into 4-hour pacing blocks.



## Coreg Pipeline

Coreg data is pre-aggregated using a facts table to avoid large scans of raw coreg tables.

Source Tables:
public.tra_coreg2026
public.dracoreg2026

Historical Data:
public.tra_coreg_all
public.dra_coreg_all


Pipeline:
raw_coreg tables
      ↓
`public.refresh_coreg_facts(start_date, end_date)` -- Updates a rolling window of recent dates.
      ↓
coreg_facts -- Incrementally maintained aggregated facts table.
      ↓
`public.v_coreg_daily` -- Canonical daily rollups.
      ↓
`public.v_coreg_blocks` -- Canonical 4-hour rollups.



## Time Model

All analytics are normalized to Pacific time. All lead timestamps are converted at the event normalization layer.

Primary Fields:
pacific_date -- Date used for daily rollups.

block_index -- Intraday pacing block.



## Intraday Block Model

Blocks represent 4-hour pacing windows. These blocks allow the dashboard to evaluate whether a campaign is pacing correctly throughout the day.

0 → 12am - 4am
1 → 4am - 8am
2 → 8am - 12pm
3 → 12pm - 4pm
4 → 4pm - 8pm
5 → 8pm - 12pm



## Campaign Activity Layer

This determines whether a campaign should be considered active, as the dashboard automatically shows only campaigns that are "ON". (To see OFF requires toggling.) A campaign is active if it has events within the current or previous month.

`public.v_campaign_activity` -- Normalized events.
`public.mv_campaign_activity` -- Campaign activity rollup.

Manual override is supported via `public.myprecious_is_active_override`



## Dashboard Materialized Views

These views power the Lovable Mission Control dashboard. They are refreshed automatically using pg_cron.

`public.mv_campaign_activity` -- Campaign metadata and active status; refresh every 15m.
`public.mv_now_v2` -- Intraday pacing context; refresh every 10m
`public.mv_periods_v2` -- Completed time-window analytics.
`public.mv_lifecycle_averages_v1` -- Lifetime baseline averages; refresh daily.
`public.mv_ytd_yoy_v1` -- Year-over-year comparisons; refresh daily. 



## Forecast Engine

Forecast calculations are exposed through an RPC. The function returns a JSON payload consumed directly by the dashboard UI.

Modes supported:
NOW
7DAY forecast
30DAY forecast

The RPC computes:
- expected totals
- remaining leads required to hit view average
- required daily pace to hit view average
- current velocity vs. baseline
- recommended pacing adjustment



## Watchlist System

Users can monitor specific campaigns instead of looking at the whole dashboard.

`public.watchlist_campaigns` -- RLS ensures users can only access their own entries.
Features:
- per-user watchlist
- campaign pinning
- notes
- snooze timers



## Authentication

Misson Control uses a callsign-based authentication system.

Schema: mc_auth

Tables:
`mc_auth.mc_commanders` -- Callsign → email & display name mapping.
`mc_auth.mc_login_throttle` -- Per-callsign + per-IP throttle state.

Edge Functions: 
`supabase/functions/mc_resolve_callsign` -- Validates callsign format and throttle state, resolves callsign and commander display name from `mc_auth.mc_commanders`, and enforces cooldowns after repeated failures (per callsign and per IP).

`supabase/functions/mc_record_login_result` -- Records login success/failure to the same throttle table, resets throttle state on success and increments failures on invalid attempts, and enforces aforementioned cooldown behavior as resolve.

Pipeline:
User enters callsign
      ↓
`mc_resolve_callsign`
      ↓
Commander email returned
      ↓
Login attempt recorded
      ↓
`mc_record_login_result`
      ↓
Throttle state updated



## Future Goals

Potential future additions to the system include:
- Visualized campaign lifecycle metrics
- Alerts triggered by pacing anomalies
- Automated campaign health scoring
- "Leaderboard" campaign analytics



## Repo structure
- docs/
    `domain_notes.md`
- sql/
    activity/
        `02_campaign_activity_mv.sql`
        `06_pacing_status_all.sql`
        `08_pacing_status_now_mv.sql`
        `09_mv_nbow_v2.sql`
        `10_mv_periods_v2.sql`
        `11_lifecycle_averages_v1.sql`
        `12_mv_ytd_yoy_v1.sql`
        `13_watchlist_campaigns.sql`
        `14_rpc_campaign_forecase_v1.sql`
    auth/
        `01_mc_auth_tables.sql`
    coreg/
        `02_coreg_daily_rollup_view.sql`
        `02_coreg_daily_events_view.sql`
        `03_coreg_block_rollup_view.sql`
        `04_coreg_baselines_view.sql`
        `05_coreg_pacing_status_view.sql`
        `06_refresh_coreg_facts.sql`
    forms/
        `01_forms_events_view.sql`
        `02_forms_daily_rollup_view.sql`
        `03_forms_block_rollup_view.sql`
        `04_forms_baselines_view.sql`
        `02_forms_pacing_status_view.sql`
    `sql_all.sql`
- supbase/functions/
    mc_resolve_callsign/
        `index.ts`
    mc_record_login_result/
        `index.ts`
- supabase_exports/
    `constraints.csv`
    `indexes.csv`
    `policies.csv`
    `rls.csv`
    `schema_columns.csv`

