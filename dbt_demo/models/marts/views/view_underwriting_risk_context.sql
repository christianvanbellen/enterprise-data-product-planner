{{
    config(
        materialized='view',
        tags=['view', 'renewal', 'fulfilment'],
    )
}}

-- ─────────────────────────────────────────────────────────────────────────
-- Initiative answered: underwriting_decision_support
-- Asker:               underwriter
-- Cadence:             per_quote
-- Question:            For the risk in front of me right now, what does the
--                      warehouse already know about comparable layers, this
--                      insured's history, and our pricing position?
--
-- Sourced from:        mart_renewal_decision_support (canonical mart)
--
-- Wider column projection than the priority queue — designed to be the
-- "decision card" an underwriter reads before binding. Same row grain;
-- different presentation: queue is a ranked list, this is a per-row
-- detail view consumed at quote time.
-- ─────────────────────────────────────────────────────────────────────────

select
    quote_id,
    layer_id,
    pas_id,

    -- Who and where
    policyholder_name,
    underwriter,
    broker_primary,
    carrier_branch,
    premium_currency,

    -- Coverage / risk-shape attributes
    section,
    coverage,
    exposure_type,
    limit_type,
    deductible_type,
    claims_trigger,
    policy_coverage_jurisdiction,

    -- Layer structure
    exposure,
    coverage_limit_amount,
    excess,
    deductible_value,

    -- Pricing position
    tech_gnwp,
    modtech_gnwp,
    sold_gnwp,
    tech_elc,
    commission,
    london_order_percentage,
    tech_gg_elr,
    modtech_gg_elr,
    sold_to_modtech_ratio,

    -- Layer adequacy benchmarks
    rate_on_line,
    adequacy_gap_modtech_pct,

    -- Renewal context (vs expiring period)
    renewal_inception_date,
    renewal_expiry_date,
    expiring_inception_date,
    expiring_expiry_date,
    expiring_gnwp,
    year_on_year_premium_change_pct,

    -- Forward-looking signals
    gross_rarc,
    net_rarc,
    claims_inflation,

    mart_built_at

from {{ ref('mart_renewal_decision_support') }}
