{{ config(materialized='table') }}

with abr as (
    select * from {{ ref('stg_abr') }}
),

cc as (
    select * from {{ ref('stg_commoncrawl') }}
),

fuzzy as (
    select * from staging.fuzzy_matches
),

llm as (
    select * from {{ ref('stg_llm_matches') }}
),

final_matches as (

    -- 1. Perfect fuzzy matches (deterministic)
    select
        abr.id as abr_id,
        cc.id as cc_id,
        100 as match_score,
        'deterministic' as match_type,
        abr.entity_name as unified_name,
        abr.address,
        abr.postcode,
        abr.state,
        cc.company_name as website_name,
        cc.website_url,
        cc.industry,
        abr.start_date
    from fuzzy f
    join abr on f.abr_id = abr.id
    join cc  on f.cc_id = cc.id
    where f.similarity = 100

    union all

    -- 2. LLM matches (semantic)
    select
        abr.id as abr_id,
        cc.id as cc_id,
        llm.llm_confidence as match_score,
        'llm' as match_type,
        abr.entity_name as unified_name,
        abr.address,
        abr.postcode,
        abr.state,
        cc.company_name as website_name,
        cc.website_url,
        cc.industry,
        abr.start_date
    from llm
    join abr on llm.abr_id = abr.id
    join cc  on llm.cc_id = cc.id
    where llm.llm_decision = 'yes'

)

select *
from final_matches
