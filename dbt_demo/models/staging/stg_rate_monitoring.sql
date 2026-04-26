{{
    config(
        materialized='view',
        tags=['staging', 'rate_change', 'renewals'],
    )
}}

-- Renewal-only risk-adjusted rate-change measures at quote_id grain.
-- Compares the current quote against the expiring policy (inception − 365d).
-- Source: rate_monitoring_total_our_share_usd seed.
--
-- Decomposition identity:
--   net_rarc ≈ gross_rarc − claims_inflation − breadth_of_cover_change
--                      − policy_term_change − other_changes
-- Joins to stg_ll_quote_setup on quote_id; absent for new business (LEFT
-- JOIN downstream so non-renewal quotes are not dropped).

select
    -- Primary key
    quote_id                          as quote_id,

    -- Expiring policy reference dates
    expiring_inception_date           as expiring_inception_date,
    expiring_expiry_date              as expiring_expiry_date,

    -- Expiring exposure / limit / excess / deductible (for delta tracking)
    expiring_exposure                 as expiring_exposure,
    expiring_limit                    as expiring_limit,
    expiring_excess                   as expiring_excess,
    expiring_deductible               as expiring_deductible,

    -- Expiring share & commission
    expiring_our_share_pct            as expiring_our_share_pct,
    expiring_commission_percentage    as expiring_commission_pct,
    our_share_pct_london              as our_share_pct_london,
    our_share_pct_non_london          as our_share_pct_non_london,

    -- Expiring premium decomposition
    expiring_ggwp                     as expiring_ggwp,
    expiring_gnwp                     as expiring_gnwp,
    expiring_modtech_gnwp             as expiring_modtech_gnwp,
    expiring_tech_gnwp                as expiring_tech_gnwp,
    expiring_as_if_ggwp               as expiring_as_if_ggwp,

    -- Risk-adjusted rate change (gross of inflation; net of inflation+breadth)
    gross_rarc                        as gross_rarc,
    net_rarc                          as net_rarc,

    -- Decomposition components
    claims_inflation                  as claims_inflation,
    breadth_of_cover_change           as breadth_of_cover_change,
    gross_exposure_change             as gross_exposure_change,
    gross_limits_and_excess_change    as gross_limits_and_excess_change,
    policy_term_change                as policy_term_change,
    other_changes                     as other_changes,

    -- Audit
    _pdm_last_update_timestamp        as loaded_at

from {{ ref('rate_monitoring_total_our_share_usd') }}
