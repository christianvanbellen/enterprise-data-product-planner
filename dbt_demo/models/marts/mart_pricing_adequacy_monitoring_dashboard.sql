{{
    config(
        materialized='table',
        tags=['mart', 'pricing', 'adequacy', 'monitoring'],
    )
}}

-- ─────────────────────────────────────────────────────────────────────────
-- Initiative: pricing_adequacy_monitoring
-- Output:    mart_pack_b.mart_pricing_adequacy_monitoring_dashboard
-- Grain:     one row per (quote_id, layer_id, pas_id), all business
-- Sources:   stg_ll_quote_policy_detail (primary, layer-grain, Liberty share)
--            stg_quote_policy_detail    (full-share twin, for benchmark context)
--            int_coverage_layer_rollup  (bridge → layer-grain rollup)
--            stg_rate_monitoring_layer  (renewal-only, LAYER grain, LEFT joined)
--            stg_ll_quote_setup         (header dimension)
--
-- Goal: surface — at the layer grain — every place where sold_gnwp is
-- diverging from the technical / modified-technical price benchmarks, and
-- accompany it with the rate-change signal so consumers can read drift in
-- real time.
--
-- Headline measures (computed here, not in staging):
--   • adequacy_gap_modtech_pct  = sold_gnwp / modtech_gnwp − 1
--   • adequacy_gap_tech_pct     = sold_gnwp / tech_gnwp    − 1
--   • commission_load_pct       = commission / sold_gnwp
--   • elr_drift_pct             = modtech_gg_elr − tech_gg_elr  (deterioration of expected loss ratio under the underwriter modifier)
-- ─────────────────────────────────────────────────────────────────────────
--
-- Spec realignment (vs output/spec_log/pricing_adequacy_monitoring/current.md)
--
-- ✓ R1 RESOLVED — Pack B v2 ships rate_monitoring at (quote_id, layer_id)
--   grain alongside the quote-grain rate_monitoring_total_our_share_usd.
--   The two reconcile by premium-weighted aggregation; this mart consumes
--   the layer-grain seed directly via stg_rate_monitoring_layer.
--
-- ✓ R2 RESOLVED — layer_id is now present on stg_rate_monitoring_layer, so
--   the join is layer-grain on both sides. No more "carry quote-level rarc
--   down to every layer" artefact: each layer reports its own rate change.
--
-- R3. LEFT JOIN to stg_rate_monitoring_layer keeps new-business quotes in
--     the mart (with NULL rate-change columns). Pricing-adequacy monitoring
--     covers *all* business — new business contributes adequacy_gap_*
--     signals via sold/tech ratios even without a rate-change context.
--
-- R4. Bridge fan-out resolved via int_coverage_layer_rollup.
-- ─────────────────────────────────────────────────────────────────────────

with policy_lib as (
    select * from {{ ref('stg_ll_quote_policy_detail') }}
),

policy_full as (
    select * from {{ ref('stg_quote_policy_detail') }}
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
        -- ── Identifiers ───────────────────────────────────────────────
        pl.quote_id,
        pl.layer_id,
        pl.pas_id,

        -- ── Header context ────────────────────────────────────────────
        s.policyholder_name,
        s.underwriter,
        s.broker_primary,
        s.carrier_branch,
        s.premium_currency,
        s.quote_date,
        s.inception_date                       as quote_inception_date,
        s.expiry_date                          as quote_expiry_date,
        s.is_quoted,

        -- ── Coverage dimensions ───────────────────────────────────────
        c.section,
        c.coverage,
        c.exposure_type,
        c.limit_type,
        c.deductible_type,
        c.claims_trigger,
        c.policy_coverage_jurisdiction,
        c.new_renewal,
        c.has_multi_coverage_rollup,

        -- ── Structural measures ───────────────────────────────────────
        c.total_exposure                       as exposure,
        c.total_coverage_limit_amount          as coverage_limit_amount,
        c.total_excess                         as excess,
        c.total_deductible_value               as deductible_value,

        -- ── Subscription share economics ──────────────────────────────
        pl.our_share_pct,
        pl.our_share_pct_london,
        pl.our_share_pct_non_london,
        pl.london_estimated_signed_line,
        pl.london_order_percentage,

        -- ── Liberty-share pricing decomposition ───────────────────────
        pl.tech_gnwp,
        pl.modtech_gnwp,
        pl.sold_gnwp,
        pl.tech_ggwp,
        pl.modtech_ggwp,
        pl.sold_ggwp,
        pl.tech_elc,
        pl.modtech_elc,
        pl.commission,

        -- ── Full-share benchmark (100% market context) ────────────────
        -- These are the same row at full-share scale; useful for ranking
        -- a layer's adequacy gap against the wider market view.
        pf.tech_gnwp_full,
        pf.modtech_gnwp_full,
        pf.sold_gnwp_full,

        -- ── ELR and pricing-adequacy ratios (carried from staging) ────
        pl.tech_gg_elr,
        pl.modtech_gg_elr,
        pl.tech_gn_elr,
        pl.modtech_gn_elr,
        pl.sold_to_modtech_ratio,
        pl.modtech_to_tech_ratio,

        -- ── Rate-change context (renewal-only — NULL for new business) ─
        -- LAYER grain. Each layer reports its own rate-change components;
        -- consumers averaging across layers can do so directly.
        r.gross_rarc,
        r.net_rarc,
        r.claims_inflation,
        r.breadth_of_cover_change,
        r.gross_exposure_change,
        r.gross_limits_and_excess_change,
        r.policy_term_change,
        r.other_changes,
        r.expiring_inception_date,
        r.expiring_expiry_date

    from policy_lib pl
    inner join coverage c
        on  pl.quote_id = c.quote_id
        and pl.layer_id = c.layer_id
        and pl.pas_id   = c.pas_id
    inner join policy_full pf
        on  pl.quote_id = pf.quote_id
        and pl.layer_id = pf.layer_id
        and pl.pas_id   = pf.pas_id
    inner join setup s
        on pl.quote_id = s.quote_id
    -- LEFT join on (quote_id, layer_id): keep new business + non-renewal layers
    -- with NULL rate-change columns.
    left join rate r
        on  pl.quote_id = r.quote_id
        and pl.layer_id = r.layer_id
)

select
    *,

    -- ── Derived adequacy measures ─────────────────────────────────────
    -- Adequacy gap vs modified-technical benchmark. Negative => sold below
    -- the underwriter-adjusted technical price (concession territory).
    case
        when modtech_gnwp is null or modtech_gnwp = 0 then null
        else (sold_gnwp / modtech_gnwp) - 1
    end                                         as adequacy_gap_modtech_pct,

    -- Adequacy gap vs raw technical benchmark. Negative => sold below the
    -- actuarial floor; the most conservative read.
    case
        when tech_gnwp is null or tech_gnwp = 0 then null
        else (sold_gnwp / tech_gnwp) - 1
    end                                         as adequacy_gap_tech_pct,

    -- Commission load — fraction of sold premium paid to the broker.
    case
        when sold_gnwp is null or sold_gnwp = 0 then null
        else commission / sold_gnwp
    end                                         as commission_load_pct,

    -- ELR drift — increase in expected loss ratio under the underwriter
    -- modifier (modtech_gg_elr - tech_gg_elr). Positive => modifier
    -- worsened the expected loss ratio.
    case
        when tech_gg_elr is null or modtech_gg_elr is null then null
        else modtech_gg_elr - tech_gg_elr
    end                                         as elr_drift_modtech_minus_tech,

    -- Categorical flag for at-a-glance dashboards. Buckets the headline
    -- adequacy gap. Thresholds are demo defaults — final cutoffs should be
    -- tuned with the pricing team.
    case
        when modtech_gnwp is null or modtech_gnwp = 0
            then 'unknown'
        when (sold_gnwp / modtech_gnwp) - 1 < -0.10
            then 'below_modtech_severe'
        when (sold_gnwp / modtech_gnwp) - 1 < -0.025
            then 'below_modtech_mild'
        when (sold_gnwp / modtech_gnwp) - 1 <= 0.025
            then 'on_modtech'
        else 'above_modtech'
    end                                         as adequacy_band,

    current_timestamp()                         as mart_built_at

from joined
