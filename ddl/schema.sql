CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS metadata;
CREATE SCHEMA IF NOT EXISTS core;

-- staging: common crawl
CREATE TABLE IF NOT EXISTS staging.commoncrawl_raw (
    id SERIAL PRIMARY KEY,
    website_url TEXT,
    company_name TEXT,
    industry TEXT,
    ingest_date DATE,
    raw JSONB
);

-- staging: abr
CREATE TABLE IF NOT EXISTS staging.abr_raw (
    id SERIAL PRIMARY KEY,
    abn TEXT,
    entity_name TEXT,
    entity_type TEXT,
    entity_status TEXT,
    address TEXT,
    postcode TEXT,
    state TEXT,
    start_date DATE,
    ingest_date DATE,
    raw JSONB
);

-- partition hash table
CREATE TABLE IF NOT EXISTS metadata.partition_hashes (
    source TEXT,
    partition_date DATE,
    partition_hash TEXT,
    record_count BIGINT,
    updated_at TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (source, partition_date)
);

-- final company master table
CREATE TABLE IF NOT EXISTS core.company_master (
    company_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    canonical_name TEXT,
    primary_abn TEXT,
    primary_website TEXT,
    industry TEXT,
    address TEXT,
    postcode TEXT,
    state TEXT,
    first_seen TIMESTAMPTZ,
    last_seen TIMESTAMPTZ,
    raw_sources JSONB
);

-- extensions
CREATE EXTENSION IF NOT EXISTS pgcrypto;
CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- indexes
CREATE INDEX IF NOT EXISTS idx_company_name_trgm 
    ON core.company_master USING gin (canonical_name gin_trgm_ops);

CREATE INDEX IF NOT EXISTS idx_company_postcode 
    ON core.company_master(postcode);

CREATE INDEX IF NOT EXISTS idx_company_state 
    ON core.company_master(state);
