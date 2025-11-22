import json
import psycopg2
from psycopg2.extras import execute_batch
import sys
from datetime import date

DB_CONN = {
    "host": "localhost",
    "dbname": "firmabledb",
    "user": "firmable",
    "password": "firmablepass",
    "port": 5432
}

def load_cc(jsonl_path):
    conn = psycopg2.connect(**DB_CONN)
    cur = conn.cursor()

    records = []

    with open(jsonl_path, 'r', encoding='utf-8') as f:
        for line in f:
            r = json.loads(line)
            records.append((
                r.get("website_url"),
                r.get("company_name"),
                r.get("industry"),
                str(date.today()),
                json.dumps(r)
            ))

    sql = """
    INSERT INTO staging.commoncrawl_raw
    (website_url, company_name, industry, ingest_date, raw)
    VALUES (%s, %s, %s, %s, %s);
    """

    execute_batch(cur, sql, records)
    conn.commit()

    print("Loaded", len(records), "CC records into Postgres.")
    cur.close()
    conn.close()

if __name__ == "__main__":
    load_cc(sys.argv[1])
