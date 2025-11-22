import hashlib
import json
import psycopg2
import sys
from datetime import date

DB_CONN = {
    "host": "localhost",
    "dbname": "firmabledb",
    "user": "firmable",
    "password": "firmablepass",
    "port": 5432
}

def compute_hash_for_file(source, jsonl_path):
    md5 = hashlib.md5()
    record_count = 0

    with open(jsonl_path, 'r', encoding='utf-8') as f:
        for line in f:
            md5.update(line.encode('utf-8'))
            record_count += 1

    hash_value = md5.hexdigest()
    partition_date = str(date.today())

    print(f"Source: {source}")
    print(f"Partition date: {partition_date}")
    print(f"Hash: {hash_value}")
    print(f"Record count: {record_count}")

    # Store in Postgres metadata table
    conn = psycopg2.connect(**DB_CONN)
    cur = conn.cursor()

    sql = """
    INSERT INTO metadata.partition_hashes
    (source, partition_date, partition_hash, record_count)
    VALUES (%s, %s, %s, %s)
    ON CONFLICT (source, partition_date)
    DO UPDATE SET partition_hash = EXCLUDED.partition_hash,
                  record_count = EXCLUDED.record_count,
                  updated_at = now();
    """

    cur.execute(sql, (source, partition_date, hash_value, record_count))
    conn.commit()
    cur.close()
    conn.close()

if __name__ == "__main__":
    source = sys.argv[1]
    jsonl_path = sys.argv[2]
    compute_hash_for_file(source, jsonl_path)
