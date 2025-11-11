
from fastapi import FastAPI, HTTPException, Header, Depends
from pydantic import BaseModel
import psycopg2
from psycopg2 import pool
import os
import re
import logging
from typing import List, Optional
from langchain_ollama import ChatOllama
# ---------- CONFIG ----------
DATABASE_URL = "postgresql://postgres:medical123@localhost:5432/medicaldb"
API_TOKEN = os.getenv("MCP_API_TOKEN", "change_me_token")
MAX_ROWS = int(os.getenv("MCP_MAX_ROWS", "200"))  # safety row limit
# ----------------------------
llm = ChatOllama(model="qwen2.5:0.5b", temperature=0)

logging.basicConfig(level=logging.INFO)
app = FastAPI(title="MCP Postgres Tooling")

# Create a connection pool
pg_pool = psycopg2.pool.ThreadedConnectionPool(minconn=1, maxconn=10, dsn=DATABASE_URL)
# Request models
class AskRequest(BaseModel):
    question: str
# Request models
class SQLRequest(BaseModel):
    sql: str
    params: Optional[List] = None
    row_limit: Optional[int] = None

# ----------------- UTILITIES -----------------
def extract_sql_from_llm(llm_response: str) -> str:
    """Remove ```sql``` fences and extra text."""
    resp = llm_response.strip()
    resp = resp.replace("```sql", "").replace("```", "").strip()
    return resp

def run_query(sql: str):
    if re.search(r"\bLIMIT\b", sql, re.IGNORECASE) is None:
        sql = f"{sql.rstrip(';')} LIMIT {MAX_ROWS}"
    conn = pg_pool.getconn()
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            if cur.description:
                cols = [desc[0] for desc in cur.description]
                rows = cur.fetchall()
                # convert rows to list of dicts for Streamlit
                rows_dict = [dict(zip(cols, row)) for row in rows]
            else:
                cols = []
                rows_dict = []
        return {"columns": cols, "rows": rows_dict}
    finally:
        pg_pool.putconn(conn)
# ----------------- ENDPOINTS -----------------
@app.get("/list_tables")
def list_tables():
    conn = pg_pool.getconn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT table_schema, table_name
                FROM information_schema.tables
                WHERE table_type='BASE TABLE' AND table_schema NOT IN ('pg_catalog','information_schema')
                ORDER BY table_schema, table_name
            """)
            result = [{"schema": r[0], "table": r[1]} for r in cur.fetchall()]
        return {"tables": result}
    finally:
        pg_pool.putconn(conn)

@app.get("/get_schema/{schema}/{table}")
def get_schema(schema: str, table: str):
    conn = pg_pool.getconn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_schema=%s AND table_name=%s
                ORDER BY ordinal_position
            """, (schema, table))
            cols = [{"column": r[0], "type": r[1]} for r in cur.fetchall()]
        return {"schema": schema, "table": table, "columns": cols}
    finally:
        pg_pool.putconn(conn)

@app.post("/ask")
def ask(req: AskRequest):
    question = req.question
    # Generate SQL using LLM
    try:
        llm_resp = llm.invoke(f"""
You are an expert SQL generator for PostgreSQL.

Tables:
- patients(id, name, gender, birth_date)
- patient_conditions(id, patient_id, disease_id, diagnosed_date, notes)
- treatments(id, patient_condition_id, treatment, start_date, end_date, notes)
- medicines(id, treatment_id, medicine_name, dosage, frequency, duration_days)
- doctors(id, name, specialization)
- appointments(id, patient_id, doctor_id, appointment_date, notes)
- diseases(id, name, description)
- symptoms(id, name, description)

Rules:
1. Use proper joins to fetch accurate data.
2. Only use the columns listed above.
3. Output SQL only.
4. Return zero rows if no matching data exists.

User question: "{question}"
""")
        sql = extract_sql_from_llm(llm_resp.content)
        print("Generated SQL:", sql)
        if not sql:
            return {"sql": "", "rows": [], "message": "LLM failed to generate SQL."}
    except Exception as e:
        return {"sql": "", "rows": [], "message": f"Error generating SQL: {e}"}
    # Execute query safely
    try:
        result = run_query(sql)
        if not result["rows"]:
            message = "No matching records found."
        else:
            message = f"{len(result['rows'])} record(s) found."
        return {"sql": sql, "rows": result["rows"], "message": message}
    except Exception as e:
        return {"sql": sql, "rows": [], "message": f"Error executing SQL: {e}"}

# Optional raw SQL endpoint
@app.post("/run_sql")
def run_sql(req: SQLRequest):
    sql = req.sql
    try:
        result = run_query(sql)
        if not result["rows"]:
            message = "No matching records found."
        else:
            message = f"{len(result['rows'])} record(s) found."
        return {"sql": sql, "rows": result["rows"], "message": message}
    except Exception as e:
        return {"sql": sql, "error": str(e), "rows": []}

# ----------------- MAIN -----------------
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("server:app", host="0.0.0.0", port=8000, reload=True)