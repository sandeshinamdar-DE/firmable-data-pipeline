import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import psycopg2
from rapidfuzz import fuzz
from etl.normalize import normalize_name

DB_CONN = {
    "host": "localhost",
    "dbname": "firmabledb",
    "user": "firmable",
    "password": "firmablepass",
    "port": 5432
}

def load_data():
    conn = psycopg2.connect(**DB_CONN)
    cur = conn.cursor()

    # Load ABR data
    cur.execute("""
        SELECT id, entity_name
        FROM staging.abr_raw;
    """)
    abr = cur.fetchall()

    # Load CC data (only rows with company_name)
    cur.execute("""
        SELECT id, company_name
        FROM staging.commoncrawl_raw
        WHERE company_name IS NOT NULL;
    """)
    cc = cur.fetchall()

    conn.close()
    return abr, cc


def fuzzy_match():
    abr, cc = load_data()

    results = []

    for abr_id, abr_name in abr:
        abr_norm = normalize_name(abr_name)

        for cc_id, cc_name in cc:
            cc_norm = normalize_name(cc_name)

            score = fuzz.token_set_ratio(abr_norm, cc_norm)

            results.append((abr_id, cc_id, abr_name, cc_name, score))

    return results


def save_results(results):
    conn = psycopg2.connect(**DB_CONN)
    cur = conn.cursor()

    cur.execute("TRUNCATE TABLE staging.fuzzy_matches;")

    sql = """
    INSERT INTO staging.fuzzy_matches
    (abr_id, cc_id, abr_name, cc_name, similarity)
    VALUES (%s, %s, %s, %s, %s)
    """

    cur.executemany(sql, results)
    conn.commit()
    conn.close()


if __name__ == "__main__":
    r = fuzzy_match()
    save_results(r)
    print("Inserted", len(r), "fuzzy match rows.")
