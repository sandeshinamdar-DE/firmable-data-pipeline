CREATE TABLE IF NOT EXISTS staging.llm_matches (
    abr_id INT,
    cc_id INT,
    abr_name TEXT,
    cc_name TEXT,
    fuzzy_similarity FLOAT,
    llm_decision TEXT,
    llm_confidence INT,
    llm_reasoning TEXT,
    PRIMARY KEY (abr_id, cc_id)
);
