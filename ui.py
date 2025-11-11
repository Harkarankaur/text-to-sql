import streamlit as st
from db import create_tables, insert_patient, fetch_patient_details, run_raw_query
import requests
from age import ask_agent
# Make sure tables exist
create_tables()


st.title("Hospital Patient Management")

menu = ["View Patients", "Add Patient","Run Query"]
choice = st.sidebar.selectbox("Menu", menu)

if choice == "View Patients":
    st.subheader("All Patients")
    data = fetch_patient_details()
    st.dataframe(data)

elif choice == "Add Patient":
    st.subheader("Add a New Patient")
    name = st.text_input("Full Name")
    gender = st.selectbox("Gender", ["male", "female", "other"])
    birth_date = st.date_input("Birth Date")

    if st.button("Add Patient"):
        new_id = insert_patient(name, gender, birth_date.isoformat())
        st.success(f"Patient added with ID: {new_id}")

elif choice == "Run Query":
    st.subheader("Ask a question or enter SQL query")
    question = st.text_area("Enter your question or SQL")
    
    if st.button("Run Query"):
        if question.strip():
            try:
                """
                r = requests.post("http://localhost:8000/ask", json={"question": question})
                r.raise_for_status()
                result = r.json()

            # Show generated SQL
                st.subheader("Generated SQL")
                st.code(result["sql"], language="sql")

            # Show query results
                if not result["rows"]:
                    st.info("No data found for this query.")
                else:
                    st.subheader("Query Result")
                    st.dataframe(result["rows"])
                    """
            #result = ask_agent(question)
            #st.code(result["sql"], language="sql")
            #if "error" in result:
                #st.error(result["error"])
            #elif result.get("message"):
                #st.info(result["message"])
            #else:
                #st.dataframe(result["rows"])
                result = ask_agent(question)

                    
                st.subheader("Query Result")
                st.write(result)
            except Exception as e:
                st.error(f"Error contacting MCP server: {e}")
    else:
        st.warning("Please enter a question.")