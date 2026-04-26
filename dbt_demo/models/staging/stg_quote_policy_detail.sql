{{
    config(
        materialized='view',
        tags=['staging', 'pricing', 'full_share'],
    )
}}

-- Full-share (100% market) twin of stg_ll_quote_policy_detail. Same grain
-- (quote_id, layer_id, pas_id) and column shape; measures represent the full
-- subscription order rather than Liberty's signed line.
-- Source: quote_policy_detail seed.
--
-- Used by initiatives that compare Liberty's economics against the wider
-- market (subscription_share_performance, technical_price_benchmarking,
-- product_line_performance_dashboard).

select
    -- Composite primary key
    quote_id                          as quote_id,
    layer_id                          as layer_id,
    pas_id                            as pas_id,

    -- Dimensions
    rating_basis                      as rating_basis,
    conditional_premium_reduction     as conditional_premium_reduction,

    -- Subscription share economics (always 1.0 for full-share view, retained
    -- for join-key compatibility with stg_ll_quote_policy_detail).
    our_share_pct                     as our_share_pct,
    our_share_pct_london              as our_share_pct_london,
    our_share_pct_non_london          as our_share_pct_non_london,
    london_estimated_signed_line      as london_estimated_signed_line,
    london_order_percentage           as london_order_percentage,

    -- Premium decomposition (USD, full-share)
    tech_gnwp                         as tech_gnwp_full,
    modtech_gnwp                      as modtech_gnwp_full,
    sold_gnwp                         as sold_gnwp_full,
    tech_ggwp                         as tech_ggwp_full,
    modtech_ggwp                      as modtech_ggwp_full,
    sold_ggwp                         as sold_ggwp_full,
    sold_ggwp_our_share               as sold_ggwp_our_share,

    -- Expected loss costs
    tech_elc                          as tech_elc_full,
    modtech_elc                       as modtech_elc_full,

    -- ELR ratios
    tech_gg_elr                       as tech_gg_elr,
    modtech_gg_elr                    as modtech_gg_elr,
    tech_gn_elr                       as tech_gn_elr,
    modtech_gn_elr                    as modtech_gn_elr,

    -- Acquisition cost
    commission                        as commission_full,

    -- Pricing hand-off ratios
    sold_to_modtech                   as sold_to_modtech_ratio,
    modtech_to_tech                   as modtech_to_tech_ratio,

    -- Audit
    _pdm_last_update_timestamp        as loaded_at

from {{ ref('quote_policy_detail') }}
