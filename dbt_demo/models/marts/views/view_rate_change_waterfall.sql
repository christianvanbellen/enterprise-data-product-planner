{{
    config(
        materialized='view',
        tags=['view', 'pricing', 'fulfilment'],
    )
}}

-- ─────────────────────────────────────────────────────────────────────────
-- Initiative answered: rate_change_attribution_analytics
-- Asker:               pricing_team / actuary
-- Cadence:             per_renewal
-- Question:            How much of this renewal's rate change came from
--                      deliberate pricing action versus structural changes
--                      in exposure, limit, breadth, or claims inflation?
--
-- Sourced from:        mart_pricing_adequacy (canonical mart)
--
-- Filters to renewals only (rate_monitoring is renewal-only). Each row
-- carries the full driver decomposition plus the residual_check column
-- for users to spot non-reconciling rows.
-- ─────────────────────────────────────────────────────────────────────────

select
    quote_id,
    layer_id,
    pas_id,

    -- Layer context for the waterfall labels
    policyholder_name,
    section,
    coverage,
    underwriter,
    broker_primary,

    -- Time anchor — current vs expiring period
    quote_inception_date,
    expiring_inception_date,
    expiring_expiry_date,

    -- Premium scale
    sold_gnwp,
    expiring_gnwp,
    expiring_as_if_ggwp,

    -- Headline rate movements
    gross_rarc,
    net_rarc,

    -- Named drivers (the waterfall components)
    claims_inflation,
    breadth_of_cover_change,
    gross_exposure_change,
    gross_limits_and_excess_change,
    policy_term_change,
    other_changes,

    -- Reconciliation audit
    gross_rarc_residual_check,

    mart_built_at

from {{ ref('mart_pricing_adequacy') }}
where new_renewal = 'Renewal'
  and gross_rarc is not null
