{{
    config(
        materialized='view',
        tags=['staging', 'coverage', 'exposure'],
    )
}}

-- Coverage-grain attributes: exposure / limit / excess / deductible plus the
-- policy term context (inception, expiry, jurisdiction, claims trigger).
-- Source: ll_quote_coverage_detail seed.
--
-- Grain: (quote_id, layer_id, pas_id, coverage_id) — the finest grain in the
-- Pack B mock set. A single layer typically has 1-2 coverages; multi-coverage
-- layers should be aggregated to layer grain before joining to fact tables
-- whose grain is (quote_id, layer_id, pas_id).
--
-- NOTE on column rename: `limit` is a reserved word in Snowflake; we surface
-- it as `coverage_limit_amount` here. Downstream models reference the renamed
-- column.

select
    -- Composite primary key
    quote_id                          as quote_id,
    layer_id                          as layer_id,
    pas_id                            as pas_id,
    coverage_id                       as coverage_id,

    -- Dimensions
    quote_name                        as quote_name,
    new_renewal                       as new_renewal,
    section                           as section,
    coverage                          as coverage,
    subcoveragecode                   as subcoverage_code,
    primary_coverage                  as is_primary_coverage,

    -- Policy term
    inception_date                    as inception_date,
    expiry_date                       as expiry_date,

    -- Exposure / limit / excess / deductible
    exposure                          as exposure,
    exposure_type                     as exposure_type,
    "limit"                           as coverage_limit_amount,
    limit_type                        as limit_type,
    excess                            as excess,
    deductible_value                  as deductible_value,
    deductible_type                   as deductible_type,

    -- Coverage attributes
    claims_trigger                    as claims_trigger,
    policy_coverage_jurisdiction      as policy_coverage_jurisdiction,

    -- Audit
    _pdm_last_update_timestamp        as loaded_at

from {{ ref('ll_quote_coverage_detail') }}
