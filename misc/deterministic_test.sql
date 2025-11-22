WITH abr_norm AS (
    SELECT
        id AS abr_id,
        abn,
        entity_name,
        lower(regexp_replace(entity_name, '[^a-zA-Z0-9 ]', '', 'g')) AS name_clean,
        replace(lower(regexp_replace(entity_name, '[^a-zA-Z0-9]', '', 'g')), ' ', '') AS name_flat,
        postcode,
        state
    FROM staging.abr_raw
),

cc_norm AS (
    SELECT
        id AS cc_id,
        website_url,
        company_name,
        industry,
        lower(regexp_replace(company_name, '[^a-zA-Z0-9 ]', '', 'g')) AS name_clean,
        replace(lower(regexp_replace(company_name, '[^a-zA-Z0-9]', '', 'g')), ' ', '') AS name_flat
    FROM staging.commoncrawl_raw
),

deterministic AS (
    SELECT
        abr.abr_id,
        cc.cc_id,
        abr.entity_name AS abr_name,
        cc.company_name AS cc_name,
        cc.website_url,
        abr.abn,
        cc.industry,
        'exact_name' AS match_type
    FROM abr_norm abr
    JOIN cc_norm cc
      ON abr.name_clean = cc.name_clean

    UNION ALL

    SELECT
        abr.abr_id,
        cc.cc_id,
        abr.entity_name,
        cc.company_name,
        cc.website_url,
        abr.abn,
        cc.industry,
        'name_in_domain'
    FROM abr_norm abr
    JOIN cc_norm cc
      ON cc.website_url LIKE '%' || abr.name_flat || '%'
)

SELECT * FROM deterministic;
