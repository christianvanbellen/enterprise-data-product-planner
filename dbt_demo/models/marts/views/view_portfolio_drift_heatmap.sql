{{
    config(
        materialized='view',
        tags=['view', 'pricing', 'fulfilment'],
    )
}}

-- ─────────────────────────────────────────────────────────────────────────
-- Initiative answered: pricing_adequacy_monitoring
-- Asker:               pricing_team
-- Cadence:             weekly
-- Question:            Where in my portfolio is sold premium drifting below
--                      technical price, and by how much?
--
-- Sourced from:        mart_pricing_adequacy (canonical mart)
--
-- This view is a column-projection over the canonical mart, kept narrow
-- to the columns a portfolio-drift dashboard needs: identifiers,
-- segmentation dimensions, the headline adequacy gap, the categorical
-- band, and a premium weight for ranking.
-- ─────────────────────────────────────────────────────────────────────────

select
    -- Identifiers
    quote_id,
    layer_id,
    pas_id,

    -- Segmentation dimensions (the BI tool will pivot on these)
    section,
    coverage,
    underwriter,
    broker_primary,
    carrier_branch,
    new_renewal,

    -- Premium scale (for weighting / ranking on the heatmap)
    sold_gnwp,
    modtech_gnwp,
    tech_gnwp,

    -- Headline drift signals
    adequacy_gap_modtech_pct,
    adequacy_gap_tech_pct,
    elr_drift_modtech_minus_tech,
    commission_load_pct,

    -- Categorical band — the heatmap colour
    adequacy_band,

    mart_built_at

from {{ ref('mart_pricing_adequacy') }}
