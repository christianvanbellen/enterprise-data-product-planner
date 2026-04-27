{{
    config(
        materialized='view',
        tags=['staging', 'rate_change', 'renewals', 'layer_grain'],
    )
}}

-- Renewal-only risk-adjusted rate-change measures at LAYER grain.
-- One row per (quote_id, layer_id) for renewing programmes; each layer
-- carries independently drawn rate-change components so primary vs excess
-- layers can move differently in a multi-layer programme.
-- Source: rate_monitoring seed.
--
-- Decomposition identity (per-layer):
--   net_rarc ≈ gross_rarc − claims_inflation − breadth_of_cover_change
--                        − policy_term_change − other_changes
--
-- Twin: stg_rate_monitoring (quote-grain, derived from this seed by
-- premium-weighted aggregation; weight = tech_gnwp_full per layer).
-- A consumer can choose:
--   • this view → per-layer adequacy / attribution work
--   • stg_rate_monitoring → quote-level prioritisation / triage

select
    -- Composite primary key
    quote_id                          as quote_id,
    layer_id                          as layer_id,

    -- Expiring policy reference dates (denormalised per quote)
    expiring_inception_date           as expiring_inception_date,
    expiring_expiry_date              as expiring_expiry_date,

    -- Expiring exposure / limit / excess / deductible (per layer)
    expiring_exposure                 as expiring_exposure,
    expiring_limit                    as expiring_limit,
    expiring_excess                   as expiring_excess,
    expiring_deductible               as expiring_deductible,

    -- Expiring share & commission (denormalised per quote)
    expiring_our_share_pct            as expiring_our_share_pct,
    expiring_commission_percentage    as expiring_commission_pct,
    our_share_pct_london              as our_share_pct_london,
    our_share_pct_non_london          as our_share_pct_non_london,

    -- Expiring premium decomposition (per layer)
    expiring_ggwp                     as expiring_ggwp,
    expiring_gnwp                     as expiring_gnwp,
    expiring_modtech_gnwp             as expiring_modtech_gnwp,
    expiring_tech_gnwp                as expiring_tech_gnwp,
    expiring_as_if_ggwp               as expiring_as_if_ggwp,

    -- Risk-adjusted rate change (per layer)
    gross_rarc                        as gross_rarc,
    net_rarc                          as net_rarc,

    -- Decomposition components (per layer)
    claims_inflation                  as claims_inflation,
    breadth_of_cover_change           as breadth_of_cover_change,
    gross_exposure_change             as gross_exposure_change,
    gross_limits_and_excess_change    as gross_limits_and_excess_change,
    policy_term_change                as policy_term_change,
    other_changes                     as other_changes,

    -- Audit
    _pdm_last_update_timestamp        as loaded_at

from {{ ref('rate_monitoring') }}
