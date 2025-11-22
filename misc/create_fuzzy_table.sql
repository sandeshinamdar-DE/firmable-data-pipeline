CREATE TABLE IF NOT EXISTS staging.fuzzy_matches (
    abr_id INT,
    cc_id INT,
    abr_name TEXT,
    cc_name TEXT,
    similarity FLOAT,
    PRIMARY KEY (abr_id, cc_id)
);
