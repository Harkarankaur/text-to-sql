# text_to_sql_agent_llm_validated.py
import psycopg2
import re
from langchain_ollama import OllamaLLM
import os
from dotenv import load_dotenv
load_dotenv()
# ---------------- CONFIG ----------------
DB_CONFIG = {
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASS"),
    "host": os.getenv("DB_HOST"),
    "port": int(os.getenv("DB_PORT"))  
}

llm = OllamaLLM(model="qwen2.5:1.5b")
conn = psycopg2.connect(**DB_CONFIG)
cur = conn.cursor()

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
- Include COUNT when query asks "how many" or "number of"
- Only provide SQL, no explanation
"""

# ---------------- HELPER FUNCTIONS ----------------
def get_distinct_values(table, column):
    """Fetch distinct values from a table column for validation."""
    cur.execute(f"SELECT DISTINCT {column} FROM {table};")
    return [row[0] for row in cur.fetchall()]

def extract_sql(llm_response: str) -> str:
    """Extract SQL from LLM output using regex."""
    match = re.search(r"SELECT .*?;", llm_response, re.DOTALL | re.IGNORECASE)
    return match.group(0) if match else llm_response.strip()

def format_result(result, cursor):
    """Format SQL results into list or dict."""
    if not result:
        return "No records found."
    colnames = [desc[0] for desc in cursor.description]
    if len(result[0]) == 1:
        return [row[0] for row in result]
    return [dict(zip(colnames, row)) for row in result]

def validate_filters_in_sql(sql):
    """Check if SQL contains only valid filters present in DB."""
    # Validate gender
    genders = get_distinct_values("patients", "gender")
    for gender in genders:
        sql = re.sub(fr"\b{gender}\b", gender, sql, flags=re.IGNORECASE)

    # Validate diseases
    diseases = get_distinct_values("diseases", "name")
    for disease in diseases:
        sql = re.sub(fr"\b{disease}\b", disease, sql, flags=re.IGNORECASE)

    # Validate medicines
    medicines = get_distinct_values("medicines", "medicine_name")
    for med in medicines:
        sql = re.sub(fr"\b{med}\b", med, sql, flags=re.IGNORECASE)

    # Validate doctors
    doctors = get_distinct_values("doctors", "name")
    for doc in doctors:
        sql = re.sub(fr"\b{doc}\b", doc, sql, flags=re.IGNORECASE)

    return sql

# ---------------- AGENT ----------------
def ask_agent(user_query):
    """Generate SQL via LLM, then validate before execution."""
    prompt = f"""
{SCHEMA_PROMPT}
Convert this natural language query into valid SQL.
Query: {user_query}
"""
    response = llm.invoke([{"role": "user", "content": prompt}])
    response_text = response["content"] if isinstance(response, dict) else str(response)
    sql = extract_sql(response_text)

    if not sql:
        return "Could not generate SQL. Please rephrase your query."

    # Validate SQL filters to prevent hallucinations
    sql = validate_filters_in_sql(sql)

    try:
        cur.execute(sql)
        result = cur.fetchall()
         # ----------------- Plain Language Conversion -----------------
        if not result:
            return "No records found."

        # If result is a single number (COUNT)
        if len(result[0]) == 1 and isinstance(result[0][0], (int, float)):
            return f"The answer is {result[0][0]}."

        # If result is a list of names or strings
        if all(len(row) == 1 for row in result):
            names = [row[0] for row in result]
            return "Results: " + ", ".join(str(n) for n in names)

        # Otherwise, return a simplified table as text
        colnames = [desc[0] for desc in cur.description]
        text_rows = []
        for row in result:
            text_rows.append(", ".join(f"{col}: {val}" for col, val in zip(colnames, row)))
        return "\n".join(text_rows)
        #return format_result(result, cur)
    except Exception as e:
        return f"SQL execution error: {e}"

# ---------------- MAIN LOOP ----------------
if __name__ == "__main__":
    print("LLM-Driven Text-to-SQL Medical Agent with Validation Connected. Type 'exit' to quit.\n")
    while True:
        user_input = input("You: ").strip()
        if user_input.lower() in ["exit", "quit"]:
            break
        answer = ask_agent(user_input)
        print("Bot:", answer, "\n")
