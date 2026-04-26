{{
    config(
        materialized='view',
        tags=['staging', 'quote'],
    )
}}

-- Quote header dimension. One row per quote_id; carries broker /
-- underwriter / policyholder / currency / inception-expiry context.
-- Source: ll_quote_setup seed (Liberty Specialty Markets quote setup feed).

select
    -- Primary key
    quote_id                          as quote_id,

    -- Dimensions
    platform                          as source_platform,
    model_name                        as pricing_model_name,
    policyholder_name                 as policyholder_name,
    entity                            as carrier_entity,
    branch                            as carrier_branch,
    underwriter                       as underwriter,
    broker_primary                    as broker_primary,
    premium_currency                  as premium_currency,

    -- Dates
    quote_date                        as quote_date,
    inception_date                    as inception_date,
    expiry_date                       as expiry_date,

    -- Flags
    policy_is_quoted                  as is_quoted,

    -- Audit
    last_updated_at                   as last_updated_at,
    _pdm_last_update_timestamp        as loaded_at

from {{ ref('ll_quote_setup') }}
