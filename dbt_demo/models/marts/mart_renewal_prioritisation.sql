{{
    config(
        materialized='table',
        tags=['mart', 'renewal', 'prioritisation', 'underwriting'],
    )
}}

-- ─────────────────────────────────────────────────────────────────────────
-- Initiative: renewal_prioritisation
-- Output:    mart_pack_b.mart_renewal_prioritisation
-- Grain:     one row per (quote_id, layer_id, pas_id) on RENEWAL business
-- Sources:   stg_ll_quote_policy_detail (primary, layer-grain)
--            int_coverage_layer_rollup  (bridge → layer-grain rollup)
--            stg_ll_quote_setup         (header dimension — broker, UW, dates)
--            stg_rate_monitoring        (renewal-only, quote-grain)
--
-- Goal: a ranked list that lets an underwriter focus their attention on
-- renewals where pricing or risk indicators warrant active negotiation
-- versus those that can pass on a light touch.
--
-- Priority score (composite, normalised 0–100) blends:
--   • adequacy gap     — how far sold premium is below modtech (negative => higher priority)
--   • elr deterioration — modtech_gg_elr level (high ELR => higher priority)
--   • rate change     — net_rarc level (negative net rarc => higher priority)
--   • premium scale   — log(sold_gnwp) (bigger layers attract more triage attention)
--   • days to renewal — fewer days remaining => higher priority
-- Component weights are demo defaults; production should calibrate against
-- realised outcomes (renewed-and-deteriorated vs renewed-and-improved).
-- ─────────────────────────────────────────────────────────────────────────
--
-- ⚠ DETECTED RISKS (vs spec output/spec_log/renewal_prioritisation/current.md)
--
-- R1. Spec lists ll_quote_setup with 0% test coverage. Pack B's _stg_models.yml
--     has since added not_null/unique tests on quote_id and accepted_values
--     on premium_currency / branch — risk mostly mitigated. Original spec
--     warning is stale.
--
-- R2. Spec frames the mart at all-business grain but the *prioritisation*
--     concept only applies to renewals. We filter to (new_renewal = 'Renewal')
--     in the final select; new business is out of scope.
--
-- R3. Spec does not include `inception_date` / days_to_renewal as a priority
--     dimension — but that is the most operationally load-bearing signal
--     for an UW queue ("which renewals expire soonest"). We add it.
--
-- R4. Bridge fan-out resolved via int_coverage_layer_rollup.
-- ─────────────────────────────────────────────────────────────────────────

with policy as (
    select * from {{ ref('stg_ll_quote_policy_detail') }}
),

coverage as (
    select * from {{ ref('int_coverage_layer_rollup') }}
),

setup as (
    select * from {{ ref('stg_ll_quote_setup') }}
),

rate as (
    select * from {{ ref('stg_rate_monitoring') }}
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
        s.last_updated_at,
        s.inception_date                       as renewal_inception_date,
        s.expiry_date                          as renewal_expiry_date,

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

        -- ── Pricing decomposition ─────────────────────────────────────
        p.tech_gnwp,
        p.modtech_gnwp,
        p.sold_gnwp,
        p.tech_elc,
        p.commission,
        p.london_order_percentage,
        p.modtech_ggwp,
        p.modtech_gg_elr,
        p.sold_to_modtech_ratio,

        -- ── Rate-change context (renewal-only) ────────────────────────
        r.gross_rarc,
        r.net_rarc,
        r.claims_inflation,
        r.expiring_inception_date,
        r.expiring_expiry_date

    from policy p
    inner join coverage c
        on  p.quote_id = c.quote_id
        and p.layer_id = c.layer_id
        and p.pas_id   = c.pas_id
    inner join setup s
        on p.quote_id = s.quote_id
    -- INNER: renewal-only mart by design.
    inner join rate r
        on p.quote_id = r.quote_id
),

with_signals as (
    select
        *,

        -- Days remaining until the renewal incepts. Negative when the
        -- renewal has already incepted (in-flight, less actionable but
        -- still surfaced).
        datediff('day', current_date, renewal_inception_date)
                                                as days_to_renewal_inception,

        -- Adequacy gap vs modtech (sold/modtech − 1). Negative is worse.
        case
            when modtech_gnwp is null or modtech_gnwp = 0 then null
            else (sold_gnwp / modtech_gnwp) - 1
        end                                     as adequacy_gap_modtech_pct,

        -- log(sold_gnwp) for scale weighting; fall back to 0 when null/zero.
        case
            when sold_gnwp is null or sold_gnwp <= 0 then 0
            else ln(sold_gnwp)
        end                                     as log_sold_gnwp

    from joined
    where new_renewal = 'Renewal'
),

scored as (
    select
        *,

        -- ── Component scores (each 0..1, higher => more attention) ────
        --
        -- Adequacy: convert (sold/modtech − 1) into a 0..1 priority where
        -- −15% gap or worse maps to 1.0, on-modtech maps to ~0.5, +5% above
        -- maps to 0.0. Linear inside the band.
        greatest(0, least(1,
            case
                when adequacy_gap_modtech_pct is null then 0.5
                else (0.05 - adequacy_gap_modtech_pct) / 0.20
            end
        ))                                      as score_adequacy,

        -- Expected-loss-ratio level: 0.40 maps to 0, 0.80 maps to 1.0.
        greatest(0, least(1,
            case
                when modtech_gg_elr is null then 0.5
                else (modtech_gg_elr - 0.40) / 0.40
            end
        ))                                      as score_elr,

        -- Net rate change: −10% maps to 1.0, +10% maps to 0.0.
        greatest(0, least(1,
            case
                when net_rarc is null then 0.5
                else (0.10 - net_rarc) / 0.20
            end
        ))                                      as score_rate_change,

        -- Premium scale: rescale ln(sold_gnwp) into 0..1 across the
        -- portfolio's observed range (per-build percentile-style banding).
        case
            when log_sold_gnwp <= 9   then 0.0   -- ~ ln(8 100)
            when log_sold_gnwp >= 14  then 1.0   -- ~ ln(1.2 m)
            else (log_sold_gnwp - 9) / 5.0
        end                                     as score_scale,

        -- Time pressure: 0 days => 1.0, 90+ days => 0.0.
        case
            when days_to_renewal_inception is null then 0.0
            when days_to_renewal_inception <= 0    then 1.0
            when days_to_renewal_inception >= 90   then 0.0
            else 1.0 - (days_to_renewal_inception / 90.0)
        end                                     as score_time_pressure

    from with_signals
)

select
    quote_id,
    layer_id,
    pas_id,

    policyholder_name,
    underwriter,
    broker_primary,
    carrier_branch,
    premium_currency,

    quote_date,
    last_updated_at,
    renewal_inception_date,
    renewal_expiry_date,
    expiring_inception_date,
    expiring_expiry_date,
    days_to_renewal_inception,

    section,
    coverage,
    exposure_type,
    limit_type,
    deductible_type,
    claims_trigger,
    policy_coverage_jurisdiction,
    new_renewal,
    has_multi_coverage_rollup,

    exposure,
    coverage_limit_amount,
    excess,
    deductible_value,

    tech_gnwp,
    modtech_gnwp,
    sold_gnwp,
    tech_elc,
    commission,
    london_order_percentage,
    modtech_ggwp,
    modtech_gg_elr,
    sold_to_modtech_ratio,

    gross_rarc,
    net_rarc,
    claims_inflation,

    adequacy_gap_modtech_pct,
    score_adequacy,
    score_elr,
    score_rate_change,
    score_scale,
    score_time_pressure,

    -- Composite priority score, 0..100. Weights are documented above.
    round(100 * (
            0.30 * score_adequacy
          + 0.25 * score_elr
          + 0.20 * score_rate_change
          + 0.15 * score_scale
          + 0.10 * score_time_pressure
    ), 2)                                       as priority_score,

    -- Discrete priority band for triage queues.
    case
        when (
              0.30 * score_adequacy
            + 0.25 * score_elr
            + 0.20 * score_rate_change
            + 0.15 * score_scale
            + 0.10 * score_time_pressure
        ) >= 0.60 then 'high'
        when (
              0.30 * score_adequacy
            + 0.25 * score_elr
            + 0.20 * score_rate_change
            + 0.15 * score_scale
            + 0.10 * score_time_pressure
        ) >= 0.40 then 'medium'
        else 'low'
    end                                         as priority_band,

    current_timestamp()                         as mart_built_at

from scored
