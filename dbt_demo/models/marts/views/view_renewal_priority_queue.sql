{{
    config(
        materialized='view',
        tags=['view', 'renewal', 'fulfilment'],
    )
}}

-- ─────────────────────────────────────────────────────────────────────────
-- Initiative answered: renewal_prioritisation
-- Asker:               underwriter
-- Cadence:             weekly
-- Question:            Of my open renewal queue, which renewals warrant
--                      active negotiation versus a light-touch pass?
--
-- Sourced from:        mart_renewal_decision_support (canonical mart)
--
-- Narrow row shape suited to a ranked queue: identifiers, who/where/when
-- context, the headline priority score and band, and the diagnostic
-- signals an underwriter glances at before opening the renewal.
-- ─────────────────────────────────────────────────────────────────────────

select
    quote_id,
    layer_id,
    pas_id,

    -- Who / what
    policyholder_name,
    underwriter,
    broker_primary,
    carrier_branch,
    section,
    coverage,

    -- When
    renewal_inception_date,
    renewal_expiry_date,
    days_to_renewal_inception,

    -- Premium scale
    sold_gnwp,

    -- Diagnostic signals visible on the queue row
    adequacy_gap_modtech_pct,
    modtech_gg_elr,
    net_rarc,
    year_on_year_premium_change_pct,

    -- Composite priority
    priority_score,
    priority_band,

    mart_built_at

from {{ ref('mart_renewal_decision_support') }}
order by priority_score desc
