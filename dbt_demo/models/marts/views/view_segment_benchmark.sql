{{
    config(
        materialized='view',
        tags=['view', 'pricing', 'fulfilment'],
    )
}}

-- ─────────────────────────────────────────────────────────────────────────
-- Initiative answered: technical_price_benchmarking
-- Asker:               pricing_team
-- Cadence:             monthly
-- Question:            How does our written rate compare to the technical
--                      price across segments, brokers, and underwriters?
--
-- Sourced from:        mart_pricing_adequacy (canonical mart)
--
-- Wider segmentation dimensions than the drift heatmap; intended for a
-- pivot-table style benchmark report. Row level is preserved (the BI
-- tool aggregates) so an analyst can drill into outliers per segment.
-- ─────────────────────────────────────────────────────────────────────────

select
    quote_id,
    layer_id,
    pas_id,

    -- Full segmentation surface
    section,
    coverage,
    subcoverage_code,
    underwriter,
    broker_primary,
    carrier_branch,
    policy_coverage_jurisdiction,
    exposure_type,
    new_renewal,

    -- Premium decomposition (the benchmark spine)
    tech_gnwp,
    modtech_gnwp,
    sold_gnwp,
    tech_ggwp,
    modtech_ggwp,
    sold_ggwp,
    commission,

    -- Adequacy ratios (the headline benchmark measures)
    sold_to_modtech_ratio,
    modtech_to_tech_ratio,
    adequacy_gap_modtech_pct,
    adequacy_gap_tech_pct,

    -- ELR ratios (technical-vs-modified)
    tech_gg_elr,
    modtech_gg_elr,
    elr_drift_modtech_minus_tech,

    -- Subscription share (lets the benchmark filter on London exposure)
    our_share_pct,
    london_order_percentage,

    mart_built_at

from {{ ref('mart_pricing_adequacy') }}
