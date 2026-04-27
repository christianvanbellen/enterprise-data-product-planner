{{
    config(
        materialized='table',
        tags=['mart', 'pricing', 'rate_change', 'attribution'],
    )
}}

-- ─────────────────────────────────────────────────────────────────────────
-- Initiative: rate_change_attribution_analytics
-- Output:    mart_pack_b.mart_rate_change_attribution_analytics_dashboard
-- Grain:     one row per (quote_id, layer_id, pas_id) on RENEWAL business
-- Sources:   stg_ll_quote_policy_detail (primary, layer-grain)
--            int_coverage_layer_rollup  (bridge → layer-grain rollup)
--            stg_rate_monitoring_layer  (renewal-only, LAYER grain)
--            stg_ll_quote_setup         (header dimension)
--
-- Goal: decompose the renewal rate change into its named drivers so pricing
-- and actuarial users can see *how much* of a headline movement is genuine
-- pricing action vs structural change in the risk.
--
-- Decomposition identity (per stg_rate_monitoring docstring):
--   net_rarc ≈ gross_rarc − claims_inflation − breadth_of_cover_change
--                        − policy_term_change − other_changes
-- The mart additionally surfaces gross_exposure_change and
-- gross_limits_and_excess_change as standalone structural drivers so the
-- waterfall is auditable end-to-end.
-- ─────────────────────────────────────────────────────────────────────────
--
-- Spec realignment (vs output/spec_log/rate_change_attribution_analytics/current.md)
--
-- R1. Spec lists `rate_monitoring_total_our_share_usd` only for
--     `expiring_inception_date` / `expiring_expiry_date`. The actual
--     rate-monitoring seed carries the full rate-change decomposition.
--     We pull all decomposition columns through and now consume them at
--     LAYER grain so a primary vs excess layer can attribute differently.
--
-- R2. rate_monitoring is RENEWAL-ONLY. The spec frames the mart as ready_now
--     without flagging that new-business rows have no rate-change signal.
--     We resolve by filtering to (new_renewal = 'Renewal') in the final
--     select, leaving new business out of this mart entirely.
--
-- ✓ R3 RESOLVED — Pack B v2 ships rate_monitoring at (quote_id, layer_id)
--   grain. The mart now joins on the full layer-grain key and produces a
--   per-layer decomposition; no more identical rarc figures repeated across
--   sibling layers.
--
-- R4. Bridge fan-out — coverages roll up to layer grain via
--     int_coverage_layer_rollup. Layers with >1 coverage are flagged
--     via has_multi_coverage_rollup so consumers can audit.
-- ─────────────────────────────────────────────────────────────────────────

with policy as (
    select * from {{ ref('stg_ll_quote_policy_detail') }}
),

coverage as (
    select * from {{ ref('int_coverage_layer_rollup') }}
),

rate as (
    select * from {{ ref('stg_rate_monitoring_layer') }}
),

setup as (
    select * from {{ ref('stg_ll_quote_setup') }}
),

joined as (
    select
        -- ── Identifiers ────────────────────────────────────────────────
        p.quote_id,
        p.layer_id,
        p.pas_id,

        -- ── Header context (from setup) ────────────────────────────────
        s.policyholder_name,
        s.underwriter,
        s.broker_primary,
        s.carrier_branch,
        s.premium_currency,
        s.quote_date,
        s.inception_date                       as quote_inception_date,
        s.expiry_date                          as quote_expiry_date,

        -- ── Coverage / structural dimensions (layer-grain rollup) ─────
        c.section,
        c.coverage,
        c.exposure_type,
        c.limit_type,
        c.deductible_type,
        c.claims_trigger,
        c.policy_coverage_jurisdiction,
        c.new_renewal,
        c.has_multi_coverage_rollup,

        -- ── Structural measures (current period) ──────────────────────
        c.total_exposure                       as exposure,
        c.total_coverage_limit_amount          as coverage_limit_amount,
        c.total_excess                         as excess,
        c.total_deductible_value               as deductible_value,

        -- ── Pricing decomposition (current period, Liberty share, USD) ─
        p.tech_gnwp,
        p.modtech_gnwp,
        p.sold_gnwp,
        p.tech_elc,
        p.modtech_elc,
        p.commission,
        p.tech_gg_elr,
        p.modtech_gg_elr,
        p.sold_to_modtech_ratio,
        p.modtech_to_tech_ratio,

        -- ── Rate-change decomposition (renewal-only) ──────────────────
        r.expiring_inception_date,
        r.expiring_expiry_date,
        r.expiring_ggwp,
        r.expiring_gnwp,
        r.expiring_modtech_gnwp,
        r.expiring_tech_gnwp,
        r.expiring_as_if_ggwp,
        r.expiring_exposure,
        r.expiring_limit,
        r.expiring_excess,
        r.expiring_deductible,

        -- The headline movement and its named drivers
        r.gross_rarc,
        r.net_rarc,
        r.claims_inflation,
        r.breadth_of_cover_change,
        r.gross_exposure_change,
        r.gross_limits_and_excess_change,
        r.policy_term_change,
        r.other_changes

    from policy p
    inner join coverage c
        on  p.quote_id = c.quote_id
        and p.layer_id = c.layer_id
        and p.pas_id   = c.pas_id
    inner join setup s
        on p.quote_id = s.quote_id
    -- INNER join on (quote_id, layer_id): this mart is renewal-only by
    -- design, so layers without a rate-change row are intentionally
    -- excluded. Layer-grain join means each layer's decomposition is
    -- its own independent draw.
    inner join rate r
        on  p.quote_id = r.quote_id
        and p.layer_id = r.layer_id
)

select
    *,

    -- Derived audit field: residual = gross_rarc minus the named drivers.
    -- A non-zero residual indicates either (a) other_changes is doing real
    -- work, or (b) the upstream decomposition does not algebraically
    -- reconcile — surface it for QA dashboards.
    coalesce(gross_rarc, 0)
        - coalesce(claims_inflation,             0)
        - coalesce(breadth_of_cover_change,      0)
        - coalesce(gross_exposure_change,        0)
        - coalesce(gross_limits_and_excess_change, 0)
        - coalesce(policy_term_change,           0)
        - coalesce(other_changes,                0)
                                                as gross_rarc_residual_check,

    current_timestamp()                         as mart_built_at

from joined
where new_renewal = 'Renewal'
