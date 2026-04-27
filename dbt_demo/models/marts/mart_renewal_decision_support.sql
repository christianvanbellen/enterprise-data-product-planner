{{
    config(
        materialized='table',
        tags=['mart', 'canonical', 'renewal'],
    )
}}

-- ─────────────────────────────────────────────────────────────────────────
-- Canonical mart: mart_renewal_decision_support
-- Output:        mart_pack_b.mart_renewal_decision_support
-- Grain:         one row per (quote_id, layer_id, pas_id) on RENEWAL business
--
-- Canonical question
-- ------------------
-- For each open renewal, which warrant active negotiation, and what
-- context does the underwriter need to decide?
--
-- Fulfils initiatives (per ontology/mart_plan.yaml)
-- -------------------------------------------------
--   renewal_prioritisation         → view_renewal_priority_queue
--   underwriting_decision_support  → view_underwriting_risk_context
--
-- Sources
-- -------
--   stg_ll_quote_policy_detail   (primary, layer-grain)
--   int_coverage_layer_rollup    (bridge → layer-grain rollup)
--   stg_ll_quote_setup           (header dimension)
--   stg_rate_monitoring          (renewal-only, QUOTE grain)
--
-- Why quote-grain rate (not layer-grain)
-- --------------------------------------
-- Priority is a per-quote operational decision: an underwriter prioritises
-- the *renewal*, not individual layers. The quote-grain rate-monitoring
-- view carries the premium-weighted aggregation of the layer-grain seed
-- (see Pack B v2 reconciliation invariant) so the headline rate-change
-- figure is consistent with the layer-grain mart.
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

        -- ── Pricing decomposition ─────────────────────────────────────
        p.tech_gnwp,
        p.modtech_gnwp,
        p.sold_gnwp,
        p.tech_elc,
        p.commission,
        p.london_order_percentage,
        p.modtech_ggwp,
        p.tech_gg_elr,
        p.modtech_gg_elr,
        p.sold_to_modtech_ratio,

        -- ── Rate-change context (quote-grain, renewal-only) ──────────
        r.gross_rarc,
        r.net_rarc,
        r.claims_inflation,
        r.expiring_inception_date,
        r.expiring_expiry_date,
        r.expiring_gnwp

    from policy p
    inner join coverage c
        on  p.quote_id = c.quote_id
        and p.layer_id = c.layer_id
        and p.pas_id   = c.pas_id
    inner join setup s
        on p.quote_id = s.quote_id
    inner join rate r       -- inner: this mart is renewal-only by design
        on p.quote_id = r.quote_id
),

with_signals as (
    select
        *,

        -- Days from today to the renewal's inception (negative = already incepted)
        datediff('day', current_date, renewal_inception_date)
                                                as days_to_renewal_inception,

        -- Headline adequacy gap; negative => sold below modtech
        case
            when modtech_gnwp is null or modtech_gnwp = 0 then null
            else (sold_gnwp / modtech_gnwp) - 1
        end                                     as adequacy_gap_modtech_pct,

        -- Rate on line — premium per unit of layer limit (specialty-excess benchmark)
        case
            when coverage_limit_amount is null or coverage_limit_amount = 0 then null
            else sold_gnwp / coverage_limit_amount
        end                                     as rate_on_line,

        -- Year-on-year premium change at layer (renewal-only)
        case
            when expiring_gnwp is null or expiring_gnwp = 0 then null
            else (sold_gnwp / expiring_gnwp) - 1
        end                                     as year_on_year_premium_change_pct,

        -- log(sold_gnwp) for scale weighting in priority score
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
        -- See per-mart docstring; weights are demo defaults.
        greatest(0, least(1,
            case
                when adequacy_gap_modtech_pct is null then 0.5
                else (0.05 - adequacy_gap_modtech_pct) / 0.20
            end
        ))                                      as score_adequacy,

        greatest(0, least(1,
            case
                when modtech_gg_elr is null then 0.5
                else (modtech_gg_elr - 0.40) / 0.40
            end
        ))                                      as score_elr,

        greatest(0, least(1,
            case
                when net_rarc is null then 0.5
                else (0.10 - net_rarc) / 0.20
            end
        ))                                      as score_rate_change,

        case
            when log_sold_gnwp <= 9   then 0.0
            when log_sold_gnwp >= 14  then 1.0
            else (log_sold_gnwp - 9) / 5.0
        end                                     as score_scale,

        case
            when days_to_renewal_inception is null then 0.0
            when days_to_renewal_inception <= 0    then 1.0
            when days_to_renewal_inception >= 90   then 0.0
            else 1.0 - (days_to_renewal_inception / 90.0)
        end                                     as score_time_pressure

    from with_signals
)

select
    *,

    -- Composite priority score, 0..100 (weights: 0.30 / 0.25 / 0.20 / 0.15 / 0.10)
    round(100 * (
            0.30 * score_adequacy
          + 0.25 * score_elr
          + 0.20 * score_rate_change
          + 0.15 * score_scale
          + 0.10 * score_time_pressure
    ), 2)                                       as priority_score,

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
