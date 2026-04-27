{{
    config(
        materialized='table',
        tags=['mart', 'canonical', 'pricing'],
    )
}}

-- ─────────────────────────────────────────────────────────────────────────
-- Canonical mart: mart_pricing_adequacy
-- Output:        mart_pack_b.mart_pricing_adequacy
-- Grain:         one row per (quote_id, layer_id, pas_id), all business
--
-- Canonical question
-- ------------------
-- Where is sold premium drifting from technical price across my book, and
-- what's driving the gap?
--
-- Fulfils initiatives (per ontology/mart_plan.yaml)
-- -------------------------------------------------
--   pricing_adequacy_monitoring        → view_portfolio_drift_heatmap
--   layer_rate_adequacy_monitoring     → view_layer_attachment_ladder
--   technical_price_benchmarking       → view_segment_benchmark
--   rate_change_attribution_analytics  → view_rate_change_waterfall
--
-- Sources
-- -------
--   stg_ll_quote_policy_detail   (primary, layer-grain, Liberty share USD)
--   int_coverage_layer_rollup    (bridge → layer-grain rollup)
--   stg_rate_monitoring_layer    (renewal-only, LAYER grain, LEFT joined)
--   stg_ll_quote_setup           (header dimension)
--
-- Wide-fact policy
-- ----------------
-- This mart carries the union of columns needed by all four fulfilment
-- views, plus the headline derived KPIs (adequacy_gap_*, rate_on_line,
-- elr_drift, gross_rarc_residual_check, adequacy_band). Per-question
-- views project subsets of these columns; the canonical mart is the
-- single source of truth for "every signal you might need to talk about
-- pricing adequacy at layer grain."
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
        -- ── Identifiers ───────────────────────────────────────────────
        p.quote_id,
        p.layer_id,
        p.pas_id,

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

        -- ── Coverage / structural dimensions ──────────────────────────
        c.section,
        c.coverage,
        c.subcoverage_code,
        c.exposure_type,
        c.limit_type,
        c.deductible_type,
        c.claims_trigger,
        c.policy_coverage_jurisdiction,
        c.new_renewal,
        c.has_multi_coverage_rollup,

        -- ── Layer structural measures (current period) ───────────────
        c.total_exposure                       as exposure,
        c.total_coverage_limit_amount          as coverage_limit_amount,
        c.total_excess                         as excess,
        c.total_deductible_value               as deductible_value,

        -- ── Subscription share economics ──────────────────────────────
        p.our_share_pct,
        p.our_share_pct_london,
        p.our_share_pct_non_london,
        p.london_estimated_signed_line,
        p.london_order_percentage,

        -- ── Liberty-share pricing decomposition ───────────────────────
        p.tech_gnwp,
        p.modtech_gnwp,
        p.sold_gnwp,
        p.tech_ggwp,
        p.modtech_ggwp,
        p.sold_ggwp,
        p.tech_elc,
        p.modtech_elc,
        p.commission,

        -- ── ELR ratios ────────────────────────────────────────────────
        p.tech_gg_elr,
        p.modtech_gg_elr,
        p.tech_gn_elr,
        p.modtech_gn_elr,

        -- ── Pricing-adequacy ratios (from staging) ────────────────────
        p.sold_to_modtech_ratio,
        p.modtech_to_tech_ratio,

        -- ── Rate-change context (renewal-only, layer-grain LEFT join) ─
        r.expiring_inception_date,
        r.expiring_expiry_date,
        r.expiring_gnwp,
        r.expiring_ggwp,
        r.expiring_modtech_gnwp,
        r.expiring_tech_gnwp,
        r.expiring_as_if_ggwp,
        r.expiring_exposure,
        r.expiring_limit,
        r.expiring_excess,
        r.expiring_deductible,
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
    left join rate r
        on  p.quote_id = r.quote_id
        and p.layer_id = r.layer_id
)

select
    *,

    -- ── Derived adequacy measures ─────────────────────────────────────
    case
        when modtech_gnwp is null or modtech_gnwp = 0 then null
        else (sold_gnwp / modtech_gnwp) - 1
    end                                         as adequacy_gap_modtech_pct,

    case
        when tech_gnwp is null or tech_gnwp = 0 then null
        else (sold_gnwp / tech_gnwp) - 1
    end                                         as adequacy_gap_tech_pct,

    case
        when sold_gnwp is null or sold_gnwp = 0 then null
        else commission / sold_gnwp
    end                                         as commission_load_pct,

    case
        when tech_gg_elr is null or modtech_gg_elr is null then null
        else modtech_gg_elr - tech_gg_elr
    end                                         as elr_drift_modtech_minus_tech,

    -- ── Layer-economics measures ──────────────────────────────────────
    case
        when coverage_limit_amount is null or coverage_limit_amount = 0 then null
        else sold_gnwp / coverage_limit_amount
    end                                         as rate_on_line,

    case
        when coverage_limit_amount is null or coverage_limit_amount = 0 then null
        else tech_gnwp / coverage_limit_amount
    end                                         as technical_rate_on_line,

    case
        when coverage_limit_amount is null or coverage_limit_amount = 0 then null
        else modtech_gnwp / coverage_limit_amount
    end                                         as modtech_rate_on_line,

    case
        when coverage_limit_amount is null or coverage_limit_amount = 0 then null
        else excess / coverage_limit_amount
    end                                         as layer_attachment_ratio,

    -- ── Year-on-year change measures (renewal-only — NULL for NB) ────
    case
        when expiring_gnwp is null or expiring_gnwp = 0 then null
        else (sold_gnwp / expiring_gnwp) - 1
    end                                         as year_on_year_premium_change_pct,

    case
        when expiring_limit is null or expiring_limit = 0 then null
        else (coverage_limit_amount / expiring_limit) - 1
    end                                         as year_on_year_limit_change_pct,

    -- ── Rate-change residual (auditing the decomposition) ────────────
    -- gross_rarc minus the sum of named drivers. Persistently large
    -- residuals indicate the upstream decomposition does not algebraically
    -- reconcile — surface as a data-quality signal, not a hard invariant.
    coalesce(gross_rarc, 0)
        - coalesce(claims_inflation,             0)
        - coalesce(breadth_of_cover_change,      0)
        - coalesce(gross_exposure_change,        0)
        - coalesce(gross_limits_and_excess_change, 0)
        - coalesce(policy_term_change,           0)
        - coalesce(other_changes,                0)
                                                as gross_rarc_residual_check,

    -- ── Categorical band (consumed by every drift / heatmap view) ────
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
