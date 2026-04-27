{{
    config(
        materialized='table',
        tags=['mart', 'pricing', 'layer', 'adequacy', 'monitoring'],
    )
}}

-- ─────────────────────────────────────────────────────────────────────────
-- Initiative: layer_rate_adequacy_monitoring
-- Output:    mart_pack_b.mart_layer_rate_adequacy_monitoring_dashboard
-- Grain:     one row per (quote_id, layer_id, pas_id), all business
-- Sources:   stg_ll_quote_policy_detail (primary, layer-grain)
--            int_coverage_layer_rollup  (bridge → layer-grain rollup)
--            stg_rate_monitoring_layer  (renewal-only, LAYER grain, LEFT joined)
--            stg_ll_quote_setup         (header dimension)
--
-- Goal: per-layer rate-adequacy view tailored to specialty excess business,
-- where layer attachment-point and limit structure dominate adequacy more
-- than account-level averages. Distinct from pricing_adequacy_monitoring
-- in that it foregrounds the *layer attachment economics* (rate-on-line,
-- limit, excess, deductible) rather than the portfolio drift signal.
--
-- Headline measures (computed here):
--   • rate_on_line                     = sold_gnwp / coverage_limit_amount
--   • technical_rate_on_line           = tech_gnwp / coverage_limit_amount
--   • adequacy_gap_modtech_pct         = sold_gnwp / modtech_gnwp − 1
--   • layer_attachment_ratio           = excess / coverage_limit_amount  (informative)
--   • year_on_year_premium_change_pct  = sold_gnwp / expiring_gnwp − 1   (renewal only)
-- ─────────────────────────────────────────────────────────────────────────
--
-- Spec realignment (vs output/spec_log/layer_rate_adequacy_monitoring/current.md)
--
-- ✓ R1 RESOLVED — Pack B v2 ships rate_monitoring at (quote_id, layer_id)
--   grain. The spec's prescribed join key (layer_id + quote_id) is now
--   honoured directly via stg_rate_monitoring_layer.
--
-- R2. Spec's data-quality table flags rate_monitoring at 4% test coverage.
--     stg_rate_monitoring_layer adds not_null/unique on (quote_id, layer_id)
--     and bound checks on gross_rarc / net_rarc / claims_inflation — risk
--     meaningfully reduced.
--
-- R3. LEFT JOIN to layer-grain rate keeps new business in the mart with
--     NULL rate columns; the year-on-year premium delta is also NULL for
--     new business. adequacy_gap_modtech_pct remains the always-available
--     signal.
--
-- R4. Bridge fan-out resolved via int_coverage_layer_rollup.
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

        -- ── Coverage / structural dimensions ──────────────────────────
        c.section,
        c.coverage,
        c.exposure_type,
        c.limit_type,
        c.deductible_type,
        c.claims_trigger,
        c.policy_coverage_jurisdiction,
        c.new_renewal,
        c.has_multi_coverage_rollup,

        -- ── Layer structural measures ─────────────────────────────────
        c.total_exposure                       as exposure,
        c.total_coverage_limit_amount          as coverage_limit_amount,
        c.total_excess                         as excess,
        c.total_deductible_value               as deductible_value,

        -- ── Pricing decomposition (Liberty share, USD) ────────────────
        p.tech_gnwp,
        p.modtech_gnwp,
        p.sold_gnwp,
        p.tech_ggwp,
        p.modtech_ggwp,
        p.sold_ggwp,
        p.tech_elc,
        p.modtech_elc,
        p.commission,
        p.london_order_percentage,
        p.tech_gg_elr,
        p.modtech_gg_elr,
        p.tech_gn_elr,
        p.modtech_gn_elr,
        p.sold_to_modtech_ratio,
        p.modtech_to_tech_ratio,

        -- ── Rate-change context (renewal-only, LEFT joined) ───────────
        r.expiring_inception_date,
        r.expiring_expiry_date,
        r.expiring_gnwp,
        r.expiring_ggwp,
        r.expiring_modtech_gnwp,
        r.expiring_tech_gnwp,
        r.expiring_limit,
        r.expiring_excess,
        r.expiring_deductible,
        r.gross_rarc,
        r.net_rarc

    from policy p
    inner join coverage c
        on  p.quote_id = c.quote_id
        and p.layer_id = c.layer_id
        and p.pas_id   = c.pas_id
    inner join setup s
        on p.quote_id = s.quote_id
    -- LEFT join on (quote_id, layer_id): keep new business + non-renewal layers
    -- with NULL rate-change columns.
    left join rate r
        on  p.quote_id = r.quote_id
        and p.layer_id = r.layer_id
)

select
    *,

    -- ── Layer-economics derived measures ──────────────────────────────
    -- Rate on Line — premium per unit of layer limit. The standard layer
    -- adequacy benchmark in specialty excess.
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

    -- Where the layer attaches — excess as a fraction of layer limit.
    -- High ratios indicate high excess layers (more remote risk).
    case
        when coverage_limit_amount is null or coverage_limit_amount = 0 then null
        else excess / coverage_limit_amount
    end                                         as layer_attachment_ratio,

    -- Adequacy gap vs modtech (negative => below modtech).
    case
        when modtech_gnwp is null or modtech_gnwp = 0 then null
        else (sold_gnwp / modtech_gnwp) - 1
    end                                         as adequacy_gap_modtech_pct,

    case
        when tech_gnwp is null or tech_gnwp = 0 then null
        else (sold_gnwp / tech_gnwp) - 1
    end                                         as adequacy_gap_tech_pct,

    -- Year-on-year premium change at layer (renewal only — NULL for NB).
    case
        when expiring_gnwp is null or expiring_gnwp = 0 then null
        else (sold_gnwp / expiring_gnwp) - 1
    end                                         as year_on_year_premium_change_pct,

    -- Year-on-year limit change at layer.
    case
        when expiring_limit is null or expiring_limit = 0 then null
        else (coverage_limit_amount / expiring_limit) - 1
    end                                         as year_on_year_limit_change_pct,

    -- Adequacy band for at-a-glance dashboards.
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
