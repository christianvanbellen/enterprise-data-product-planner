{{
    config(
        materialized='view',
        tags=['staging', 'pricing', 'lsm_share'],
    )
}}

-- Liberty-share (Liberty Specialty Markets, USD) view of pricing decomposition
-- at quote-layer-pas grain. The technical / modified-technical / sold premium
-- hierarchy is the spine of pricing-adequacy and rate-change analytics.
-- Source: ll_quote_policy_detail seed.
--
-- Relationship to stg_quote_policy_detail (full-share twin):
--   ll_*.measure = stg_quote_policy_detail.measure × our_share_pct
-- Both share the same (quote_id, layer_id, pas_id) key tuples.

select
    -- Composite primary key
    quote_id                          as quote_id,
    layer_id                          as layer_id,
    pas_id                            as pas_id,

    -- Dimensions
    rating_basis                      as rating_basis,
    conditional_premium_reduction     as conditional_premium_reduction,

    -- Subscription share economics
    our_share_pct                     as our_share_pct,
    our_share_pct_london              as our_share_pct_london,
    our_share_pct_non_london          as our_share_pct_non_london,
    london_estimated_signed_line      as london_estimated_signed_line,
    london_order_percentage           as london_order_percentage,

    -- Premium decomposition (USD, Liberty share)
    tech_gnwp                         as tech_gnwp,
    modtech_gnwp                      as modtech_gnwp,
    sold_gnwp                         as sold_gnwp,
    tech_ggwp                         as tech_ggwp,
    modtech_ggwp                      as modtech_ggwp,
    sold_ggwp                         as sold_ggwp,
    sold_ggwp_our_share               as sold_ggwp_our_share,

    -- Expected loss costs
    tech_elc                          as tech_elc,
    modtech_elc                       as modtech_elc,

    -- ELR ratios (gross-gross and gross-net basis)
    tech_gg_elr                       as tech_gg_elr,
    modtech_gg_elr                    as modtech_gg_elr,
    tech_gn_elr                       as tech_gn_elr,
    modtech_gn_elr                    as modtech_gn_elr,

    -- Acquisition cost
    commission                        as commission,

    -- Pricing hand-off ratios (sold-vs-modtech adequacy, modtech-vs-tech modifier)
    sold_to_modtech                   as sold_to_modtech_ratio,
    modtech_to_tech                   as modtech_to_tech_ratio,

    -- Audit
    _pdm_last_update_timestamp        as loaded_at

from {{ ref('ll_quote_policy_detail') }}
