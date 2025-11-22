import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


import os
import json
import psycopg2
import argparse
from etl.normalize import normalize_name

# For mock mode or real mode
try:
    from openai import OpenAI
except:
    OpenAI = None

DB_CONN = {
    "host": "localhost",
    "dbname": "firmabledb",
    "user": "firmable",
    "password": "firmablepass",
    "port": 5432
}

def load_pairs_for_llm():
    """We only apply LLM to medium fuzzy scores."""
    conn = psycopg2.connect(**DB_CONN)
    cur = conn.cursor()

    cur.execute("""
        SELECT abr_id, cc_id, abr_name, cc_name, similarity
        FROM staging.fuzzy_matches
        WHERE similarity BETWEEN 50 AND 85;
    """)

    rows = cur.fetchall()
    conn.close()
    return rows


# ---------------- MOCK MODEL ---------------- #
def mock_llm_decision(abr_name, cc_name, similarity):
    """
    If the fuzzy similarity is near the threshold,
    mock logic will behave like a real LLM.
    """
    abr_norm = normalize_name(abr_name)
    cc_norm = normalize_name(cc_name)

    # Very simple realistic logic
    if abr_norm in cc_norm or cc_norm in abr_norm:
        return {
            "decision": "yes",
            "confidence": 70 + int(similarity / 5),
            "reason": f"Names share tokens: '{abr_norm}' ~ '{cc_norm}'."
        }
    else:
        return {
            "decision": "no",
            "confidence": 40,
            "reason": f"No strong semantic overlap between names."
        }


# ---------------- REAL GPT CALL ---------------- #
def real_llm_decision(abr_name, cc_name):
    client = OpenAI()

    prompt = f"""
    You are an expert entity-matching model.

    Compare the following two company names and decide if they refer 
    to the same entity.

    Company A: {abr_name}
    Company B: {cc_name}

    Respond in STRICT JSON ONLY:

    {{
        "decision": "yes" or "no",
        "confidence": 0-100,
        "reason": "short explanation"
    }}
    """

    response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[{"role": "user", "content": prompt}],
        temperature=0
    )

    # Extract JSON
    content = response.choices[0].message.content
    return json.loads(content)

# ---------------- SAVE TO DB ---------------- #
def save_results(results):
    conn = psycopg2.connect(**DB_CONN)
    cur = conn.cursor()

    cur.execute("TRUNCATE TABLE staging.llm_matches;")

    sql = """
    INSERT INTO staging.llm_matches
    (abr_id, cc_id, abr_name, cc_name, fuzzy_similarity, llm_decision, llm_confidence, llm_reasoning)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """

    cur.executemany(sql, results)
    conn.commit()
    conn.close()


# ---------------- MAIN ---------------- #
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--real", action="store_true", help="Use real GPT-4o-mini")
    args = parser.parse_args()

    use_real = args.real and ("OPENAI_API_KEY" in os.environ)

    pairs = load_pairs_for_llm()
    final_rows = []

    for abr_id, cc_id, abr_name, cc_name, sim in pairs:
        if use_real:
            out = real_llm_decision(abr_name, cc_name)
        else:
            out = mock_llm_decision(abr_name, cc_name, sim)

        final_rows.append((
            abr_id,
            cc_id,
            abr_name,
            cc_name,
            sim,
            out["decision"],
            out["confidence"],
            out["reason"]
        ))

    save_results(final_rows)

    if use_real:
        print("LLM matching completed using GPT-4o-mini.")
    else:
        print("LLM matching completed in MOCK MODE.")
