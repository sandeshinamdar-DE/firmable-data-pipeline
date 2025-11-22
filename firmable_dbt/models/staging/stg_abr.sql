select
    id,
    abn,
    entity_name,
    entity_type,
    entity_status,
    address,
    postcode,
    state,
    start_date,
    ingest_date
from staging.abr_raw
