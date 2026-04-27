{{
    config(
        materialized='view',
        tags=['view', 'pricing', 'fulfilment'],
    )
}}

-- ─────────────────────────────────────────────────────────────────────────
-- Initiative answered: layer_rate_adequacy_monitoring
-- Asker:               pricing_team
-- Cadence:             weekly
-- Question:            Which layers in my specialty excess book are written
--                      below technical adequacy, and is the gap widening on
--                      renewal?
--
-- Sourced from:        mart_pricing_adequacy (canonical mart)
--
-- Foregrounds the layer-economics measures (rate-on-line, attachment
-- ratio) and the YoY deltas needed for "is the gap widening" analysis.
-- ─────────────────────────────────────────────────────────────────────────

select
    quote_id,
    layer_id,
    pas_id,

    -- Layer / coverage context
    section,
    coverage,
    exposure_type,
    limit_type,
    deductible_type,
    new_renewal,

    -- Layer structure
    exposure,
    coverage_limit_amount,
    excess,
    deductible_value,
    layer_attachment_ratio,

    -- Layer pricing
    tech_gnwp,
    modtech_gnwp,
    sold_gnwp,

    -- Layer-economics benchmarks
    rate_on_line,
    technical_rate_on_line,
    modtech_rate_on_line,

    -- Adequacy
    adequacy_gap_modtech_pct,
    adequacy_gap_tech_pct,
    adequacy_band,

    -- Year-on-year deltas (renewal-only — NULL for new business)
    year_on_year_premium_change_pct,
    year_on_year_limit_change_pct,
    expiring_inception_date,
    expiring_expiry_date,

    mart_built_at

from {{ ref('mart_pricing_adequacy') }}
