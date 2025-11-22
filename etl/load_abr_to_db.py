import json
import psycopg2
from psycopg2.extras import execute_batch
import sys

DB_CONN = {
    "host": "localhost",
    "dbname": "firmabledb",
    "user": "firmable",
    "password": "firmablepass",
    "port": 5432
}

def load_abr(jsonl_path):
    conn = psycopg2.connect(**DB_CONN)
    cur = conn.cursor()

    records = []

    with open(jsonl_path, 'r', encoding='utf-8') as f:
        for line in f:
            r = json.loads(line)
            records.append((
                r.get("abn"),
                r.get("entity_name"),
                r.get("entity_type"),
                r.get("entity_status"),
                r.get("address"),
                r.get("postcode"),
                r.get("state"),
                r.get("start_date"),
                r.get("ingest_date"),
                json.dumps(r)  # raw JSON
            ))

    sql = """
    INSERT INTO staging.abr_raw
    (abn, entity_name, entity_type, entity_status, address, postcode, state,
     start_date, ingest_date, raw)
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);
    """

    execute_batch(cur, sql, records)
    conn.commit()

    print("Loaded", len(records), "ABR records into Postgres.")
    cur.close()
    conn.close()

if __name__ == "__main__":
    load_abr(sys.argv[1])
