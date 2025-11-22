# Firmable Data Pipeline — Technical Assessment

**Author:** Shanthi Sandesh
**Role:** Data Engineer (Assessment)  
**Repo:** Firmable Data Pipeline (Common Crawl + ABR)

---

## Summary
This project implements an end-to-end data pipeline to extract Australian company data from Common Crawl and the Australian Business Register (ABR), performs entity matching (deterministic, fuzzy, and LLM-assisted), and stores a unified company master in PostgreSQL using dbt for transformations.

Key highlights:
- Python extractors for ABR (XML) and Common Crawl (WET)
- Staging tables in Postgres
- Fuzzy matching (RapidFuzz)
- LLM-assisted matching (GPT-4o-mini) with safe mock mode
- dbt models for staging and `core.company_master`
- Hash-based partition metadata for incremental runs

---

## Quick stats (sample run)
- ABR extracted: 3 records
- Common Crawl extracted: 3 records
- Deterministic matches: 3
- LLM-assisted matches (demonstration synthetic): 1
> Replace with your final counts from `metadata.partition_hashes` and final `core.company_master`.

---

## Files & Structure
/etl/ # extract_abr.py, extract_commoncrawl.py, load_*_to_db.py
/ddl/schema.sql # DDL for schemas & tables
/data/ # sample files & generated outputs (abr_output.jsonl, cc_output.jsonl)
/firmable_dbt/ # dbt project (models/staging, models/core)
docs/
architecture.png # architecture diagram
README.md