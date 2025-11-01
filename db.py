import psycopg2
import pandas as pd
from datetime import datetime

def get_connection():
    import os
    return psycopg2.connect(os.environ["DATABASE_URL"])

def init_schema():
    conn = get_connection()
    with conn, conn.cursor() as cur:
        cur.execute(open("schema.sql").read())
    conn.close()

def upsert_repo(repo_id, name, owner, stars):
    conn = get_connection()
    with conn, conn.cursor() as cur:
        cur.execute("""
        INSERT INTO repositories (repo_id, name, owner, stars, updated_at)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (repo_id)
        DO UPDATE SET stars = EXCLUDED.stars, updated_at = EXCLUDED.updated_at;
        """, (repo_id, name, owner, stars, datetime.utcnow()))
        cur.execute("""
        INSERT INTO stars_history (repo_id, stars, collected_at)
        VALUES (%s, %s, %s);
        """, (repo_id, stars, datetime.utcnow()))
    conn.commit()
    conn.close()

def dump_to_csv(filename="repos_dump.csv"):
    conn = get_connection()
    df = pd.read_sql("SELECT * FROM repositories;", conn)
    df.to_csv(filename, index=False)
    conn.close()
