# text_to_sql_agent.py
import psycopg2
import re
from langchain_ollama import OllamaLLM

# ---------------- CONFIG ----------------
DB_CONFIG = {
    "dbname": "medicaldb",
    "user": "postgres",
    "password": "medical123",
    "host": "localhost",
    "port": 5432
}
llm = OllamaLLM(model="qwen2.5:1.5b")
# Connect to Postgres
conn = psycopg2.connect(**DB_CONFIG)
cur = conn.cursor()
# Database schema awareness
SCHEMA_PROMPT = """
Database Schema:
Table patients(id, name, gender, birth_date, age, phone)
Table patient_conditions(id, patient_id, disease_id, diagnosed_date, notes)
Table diseases(id, name, description)
Table medicines(id, patient_id, medicine_name, dose, date)
Table doctors(id, name, specialization)
Table appointments(id, patient_id, doctor_id, appointment_date, notes)

Notes:
- 'birth_date' is DATE
- 'age' is INT
- 'gender' values: 'Male', 'Female', 'Other'
- Use joins when needed
- Always generate valid PostgreSQL SQL
"""

# Gender mapping
gender_map = {"male": "Male", "female": "Female", "other": "Other"}

# ---------------- FUNCTIONS ----------------
def extract_sql(llm_response: str) -> str:
    match = re.search(r"SELECT .*?;", llm_response, re.DOTALL | re.IGNORECASE)
    if match:
        return match.group(0)
    return llm_response.strip()

# ---------------- CUSTOM HANDLERS ----------------
def handle_gender(query):
    match = re.search(r'\b(female|male)\b', query, re.IGNORECASE)
    if match:
        gender = match.group(1).capitalize()
        return f"SELECT COUNT(id) FROM patients WHERE gender = '{gender}';"
    return None

def handle_birth_year(query):
    match_after = re.search(r"born after (\d{4})", query, re.IGNORECASE)
    if match_after:
        return f"SELECT COUNT(id) FROM patients WHERE EXTRACT(YEAR FROM birth_date) > {match_after.group(1)};"
    match_before = re.search(r"born before (\d{4})", query, re.IGNORECASE)
    if match_before:
        return f"SELECT COUNT(id) FROM patients WHERE EXTRACT(YEAR FROM birth_date) < {match_before.group(1)};"
    return None

def handle_disease(query):
    diseases = ["diabetes", "hypertension", "asthma", "flu", "covid"]
    disease = next((d for d in diseases if d.lower() in query.lower()), None)
    if disease:
        return f"""
        SELECT COUNT(p.id) 
        FROM patients p
        JOIN patient_conditions pc ON p.id = pc.patient_id
        JOIN diseases d ON pc.disease_id = d.id
        WHERE d.name ILIKE '{disease}';
        """
    return None

def handle_gender_disease(query):
    gender_match = re.search(r'\b(female|male)\b', query, re.IGNORECASE)
    diseases = ["diabetes", "hypertension", "asthma", "flu", "covid"]
    disease = next((d for d in diseases if d.lower() in query.lower()), None)
    if gender_match and disease:
        gender = gender_match.group(1).capitalize()
        return f"""
        SELECT COUNT(p.id)
        FROM patients p
        JOIN patient_conditions pc ON p.id = pc.patient_id
        JOIN diseases d ON pc.disease_id = d.id
        WHERE p.gender = '{gender}' AND d.name ILIKE '{disease}';
        """
    return None
def format_result(result, cursor):
    if not result:
        return "No records found."
    # If multiple columns, return list of dicts
    colnames = [desc[0] for desc in cursor.description]
    if len(result[0]) == 1:
        # Single column: return as list
        return [row[0] for row in result]
    return [dict(zip(colnames, row)) for row in result]

# ---------------- AGENT FUNCTION ----------------
"""
def ask_agent(query):
    sql = handle_gender_disease(query) or handle_gender(query) or handle_birth_year(query) or handle_disease(query)
    if not sql:
        return "I can only answer queries about gender, birth year, and known diseases."
    try:
        cur.execute(sql)
        result = cur.fetchall()
        return f"Result: {result[0][0]}"
    except Exception as e:
        return f"SQL execution error: {e}"
"""
def ask_agent(user_query: str):
    """
    Unified agent:
    1 Predefined handlers for common queries
    2 Fallback to LLM for all other queries
    """
    # Try handlers first
    sql = handle_gender_disease(user_query) or handle_gender(user_query) or handle_birth_year(user_query) or handle_disease(user_query)

    # If no handler matched, fallback to LLM
    if not sql:
        # Map gender keywords
        for key, value in gender_map.items():
            if key in user_query.lower():
                user_query = re.sub(key, value, user_query, flags=re.IGNORECASE)

        prompt = f"""
{SCHEMA_PROMPT}
Convert this natural language query into a valid SQL statement.
Query: {user_query}
Only provide SQL (no explanation).
"""
        response = llm.invoke([{"role": "user", "content": prompt}])
        response_text = response["content"] if isinstance(response, dict) else str(response)
        sql = extract_sql(response_text)
        if not sql:
            return "Could not generate valid SQL. Please rephrase your question."

    # Execute SQL
    try:
        cur.execute(sql)
        result = cur.fetchall()
        return format_result(result,cur)
    except Exception as e:
        return f"SQL execution error: {e}"
# ---------------- MAIN LOOP ----------------
if __name__ == "__main__":
    print("Text-to-SQL Medical Agent Connected! Type 'exit' to quit.\n")
    while True:
        user_input = input("You: ").strip()
        if user_input.lower() in ["exit", "quit"]:
            break
        answer = ask_agent(user_input)
        print("Bot:", answer, "\n")