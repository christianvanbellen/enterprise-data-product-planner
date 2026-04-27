{{
    config(
        materialized='ephemeral',
        tags=['intermediate', 'coverage', 'layer_grain'],
    )
}}

-- Layer-grain rollup of coverage attributes.
--
-- The Pack B `ll_quote_coverage_detail` seed is at coverage grain
-- (quote_id, layer_id, pas_id, coverage_id). Most marts operate at the
-- coarser (quote_id, layer_id, pas_id) layer grain — joining the bridge
-- directly fans out facts whenever a layer has more than one coverage
-- and silently overstates premium / exposure / commission measures.
--
-- This model collapses coverage rows to layer grain by:
--   1. Picking the primary coverage's categorical attributes (section,
--      coverage, claims_trigger, jurisdiction, etc.) — the layer's
--      "headline" coverage. Falls back to the first coverage_id when no
--      flag is set.
--   2. SUM-ing additive measures: exposure, coverage_limit_amount,
--      excess, deductible_value. (Limit and excess are not strictly
--      additive across coverages on the same layer, but for the demo
--      portfolio this is acceptable; a real-world implementation should
--      validate per-layer limit-stack semantics with the carrier.)
--   3. MAX-ing inception / expiry — typically identical across the
--      coverages on a layer.
--   4. Counting coverages so downstream models can flag layers whose
--      headline-coverage rollup is lossy.

with coverage as (
    select * from {{ ref('stg_ll_quote_coverage_detail') }}
),

primary_coverage as (
    -- One row per layer carrying the primary coverage's dimensions.
    -- ROW_NUMBER picks the flagged primary coverage; if none is flagged,
    -- falls back to the lowest coverage_id.
    select
        quote_id,
        layer_id,
        pas_id,
        quote_name,
        new_renewal,
        section,
        coverage,
        subcoverage_code,
        exposure_type,
        limit_type,
        deductible_type,
        claims_trigger,
        policy_coverage_jurisdiction,
        is_primary_coverage,
        inception_date                         as primary_inception_date,
        expiry_date                            as primary_expiry_date,
        row_number() over (
            partition by quote_id, layer_id, pas_id
            order by case when is_primary_coverage then 0 else 1 end,
                     coverage_id
        )                                      as rn
    from coverage
),

primary_coverage_one_per_layer as (
    select * from primary_coverage where rn = 1
),

aggregated_measures as (
    select
        quote_id,
        layer_id,
        pas_id,
        sum(exposure)                          as total_exposure,
        sum(coverage_limit_amount)             as total_coverage_limit_amount,
        sum(excess)                            as total_excess,
        sum(deductible_value)                  as total_deductible_value,
        max(inception_date)                    as max_inception_date,
        max(expiry_date)                       as max_expiry_date,
        count(*)                               as coverage_count,
        sum(case when is_primary_coverage then 1 else 0 end)
                                               as primary_coverage_count
    from coverage
    group by 1, 2, 3
)

select
    pc.quote_id,
    pc.layer_id,
    pc.pas_id,

    -- Headline categorical attributes from the primary coverage row
    pc.quote_name,
    pc.new_renewal,
    pc.section,
    pc.coverage,
    pc.subcoverage_code,
    pc.exposure_type,
    pc.limit_type,
    pc.deductible_type,
    pc.claims_trigger,
    pc.policy_coverage_jurisdiction,
    pc.primary_inception_date                  as inception_date,
    pc.primary_expiry_date                     as expiry_date,

    -- Aggregated measures across all coverages on the layer
    am.total_exposure,
    am.total_coverage_limit_amount,
    am.total_excess,
    am.total_deductible_value,

    -- Coverage-count diagnostics — surfaced so downstream models can flag
    -- layers whose rollup discarded a non-trivial second coverage.
    am.coverage_count,
    am.primary_coverage_count,
    case
        when am.coverage_count > 1 then true
        else false
    end                                         as has_multi_coverage_rollup

from primary_coverage_one_per_layer pc
inner join aggregated_measures am
    on  pc.quote_id = am.quote_id
    and pc.layer_id = am.layer_id
    and pc.pas_id   = am.pas_id
