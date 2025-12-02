###############
# app.py
import os
import sqlite3
import pandas as pd
import streamlit as st
from mini_project2 import (
    ex1, ex2, ex3, ex4, ex5, ex6, ex7, ex8, ex9, ex10, ex11
)

# Optional: import Groq if API key is available
try:
    from groq import Groq
except ImportError:
    Groq = None

# ========================
# Paths and configs
# ========================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "normalized.db")
APP_PASSWORD = os.getenv("APP_PASSWORD", "Devraj0901@")
GROQ_API_KEY = "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"

st.set_page_config(page_title="Sales Dashboard", layout="wide")

# ========================
# Styles
# ========================
st.markdown("""
<style>
/* Main page background and font */
body, .block-container {
    background-color: #f5f7fa;
    color: #1f2937;
    font-family: 'Arial', sans-serif;
}

/* Buttons */
.stButton>button {
    background-color: #1f77b4;
    color: white;
    font-weight: 600;
    border-radius: 10px;
    height: 44px;
    width: 100%;
    transition: 0.3s;
}
.stButton>button:hover {
    background-color: #155d8b;
}

/* Sidebar */
[data-testid="stSidebar"] {
    background-color: #e1e5eb;
    color: #1f2937;
    padding: 20px;
    border-radius: 12px;
}

/* Headers */
h1,h2,h3 {
    color: #111827;
}

/* Dataframes */
.css-1lcbmhc.e1tzin5v1 {
    border-radius: 12px;
    overflow: hidden;
}
</style>
""", unsafe_allow_html=True)

# ========================
# Database connection
# ========================
def get_connection():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    return conn

@st.cache_data
def get_customer_names():
    conn = get_connection()
    df = pd.read_sql_query(
        "SELECT DISTINCT FirstName || ' ' || LastName AS Name FROM Customer ORDER BY Name;", conn
    )
    return df["Name"].tolist()

def run_query(sql: str) -> pd.DataFrame:
    conn = get_connection()
    return pd.read_sql_query(sql, conn)

def show_top_10_tables():
    conn = get_connection()
    tables = ["Region", "Country", "Customer", "ProductCategory", "Product", "OrderDetail"]
    st.markdown("## Database Preview (Top 10 Rows)")
    for table in tables:
        with st.expander(f"Top 10 rows from {table}", expanded=False):
            df = pd.read_sql_query(f"SELECT * FROM {table} LIMIT 10;", conn)
            st.dataframe(df, use_container_width=True)

# ========================
# Authentication
# ========================
if "authenticated" not in st.session_state:
    st.session_state.authenticated = False

if not st.session_state.authenticated:
    st.markdown("<h2 style='text-align:center;'>Login</h2>", unsafe_allow_html=True)
    pw = st.text_input("Enter password", type="password")
    login_btn = st.button("Login")
    if login_btn:
        if pw == APP_PASSWORD:
            st.session_state.authenticated = True
            st.rerun()
        else:
            st.error("Incorrect password")
    st.stop()

# ========================
# Initialize Groq client
# ========================
groq_client = None
if Groq and GROQ_API_KEY:
    groq_client = Groq(api_key=GROQ_API_KEY)
elif not GROQ_API_KEY:
    st.warning("GROQ_API_KEY not set. AI assistant will be disabled.")

# ========================
# Main App
# ========================
st.title("Sales Dashboard")
show_top_10_tables()

# -------------------------
# Sidebar
# -------------------------
st.sidebar.markdown("""
## How to Use
1. Select a predefined query or write your own SQL
2. Click Run
3. View results in the main panel
4. Ask AI to generate SQL automatically
""")

# -------------------------
# Layout Columns
# -------------------------
left_col, right_col = st.columns([1, 1.5])

# -------------------------
# Left column - Inputs
# -------------------------
with left_col:
    st.subheader("Predefined Queries")
    query_option = st.selectbox(
        "Select a predefined query",
        [
            "ex1: Customer order details with totals",
            "ex2: Customer total spending",
            "ex3: All customers ranked by total",
            "ex4: Regional sales totals",
            "ex5: Country sales totals",
            "ex6: Countries ranked within each region",
            "ex7: Top country per region",
            "ex8: Customer sales by quarter-year",
            "ex9: Top 5 customers per quarter",
            "ex10: Monthly sales ranking across years",
            "ex11: Customer maximum days without orders",
        ],
    )

    needs_customer = query_option.startswith(("ex1", "ex2"))
    customers = get_customer_names()
    selected_customer = st.selectbox(
        "Select a customer",
        customers,
        disabled=not needs_customer
    )

    run_predefined = st.button("Run Predefined Query")

    st.markdown("### Custom SQL Query")
    custom_sql = st.text_area("Write your own SQL", "SELECT * FROM Customer LIMIT 5;", height=160)
    run_custom_sql = st.button("Run Custom SQL")

    st.markdown("### AI-Powered SQL Assistant")
    nl_question = st.text_input("Ask a question in plain English")
    run_ai = st.button("Generate SQL with AI")

# -------------------------
# Right column - Output
# -------------------------
with right_col:
    result_df = None
    result_sql = ""
    ai_generated_sql = None

    # Predefined queries
    if run_predefined:
        conn = get_connection()
        try:
            if query_option.startswith("ex1"):
                result_sql = ex1(conn, selected_customer)
            elif query_option.startswith("ex2"):
                result_sql = ex2(conn, selected_customer)
            elif query_option.startswith("ex3"):
                result_sql = ex3(conn)
            elif query_option.startswith("ex4"):
                result_sql = ex4(conn)
            elif query_option.startswith("ex5"):
                result_sql = ex5(conn)
            elif query_option.startswith("ex6"):
                result_sql = ex6(conn)
            elif query_option.startswith("ex7"):
                result_sql = ex7(conn)
            elif query_option.startswith("ex8"):
                result_sql = ex8(conn)
            elif query_option.startswith("ex9"):
                result_sql = ex9(conn)
            elif query_option.startswith("ex10"):
                result_sql = ex10(conn)
            elif query_option.startswith("ex11"):
                result_sql = ex11(conn)

            result_df = run_query(result_sql)
        except Exception as e:
            st.error(f"Error running query: {e}")

    # Custom SQL
    if run_custom_sql:
        result_sql = custom_sql
        try:
            result_df = run_query(custom_sql)
        except Exception as e:
            st.error(f"Error running custom SQL: {e}")

    # AI-generated SQL
    if run_ai:
        if groq_client is None:
            st.error("AI is disabled because GROQ_API_KEY is not set.")
        elif not nl_question.strip():
            st.warning("Please type a question.")
        else:
            schema_description = """
Tables:
- Region(RegionID, Region)
- Country(CountryID, CountryName, RegionID)
- Customer(CustomerID, FirstName, LastName, Address, City, CountryID)
- ProductCategory(ProductCategoryID, ProductCategory, ProductCategoryDescription)
- Product(ProductID, ProductName, ProductUnitPrice, ProductCategoryID)
- OrderDetail(OrderID, CustomerID, ProductID, OrderDate, QuantityOrdered)
"""

            system_prompt = (
                "You are an assistant that writes SQL for a SQLite database. "
                "Return ONLY a valid SQL SELECT statement. "
                "Do not include explanations, comments, or markdown."
            )

            user_prompt = f"""
            {schema_description}

            Question:
            {nl_question}

            SQL:
            """

            try:
                with st.spinner("Generating SQL with Groq..."):
                    response = groq_client.chat.completions.create(
                        model="llama-3.3-70b-versatile",
                        messages=[
                            {"role": "system", "content": system_prompt},
                            {"role": "user", "content": user_prompt},
                        ],
                        temperature=0,
                    )
                    ai_sql = response.choices[0].message.content.strip()
            except Exception as e:
                st.error(f"Groq API error: {e}")
                ai_sql = ""

            if ai_sql:
                # Clean code fences
                if ai_sql.startswith("```"):
                    ai_sql = ai_sql.strip("`").strip()
                    if ai_sql.lower().startswith("sql"):
                        ai_sql = ai_sql[3:].strip()

                ai_generated_sql = ai_sql
                result_sql = ai_sql
                try:
                    result_df = run_query(ai_sql)
                except Exception as e:
                    st.error(f"SQL execution error: {e}")

    # Display SQL
    if ai_generated_sql:
        st.markdown("AI-generated SQL:")
        st.code(ai_generated_sql, language="sql")
    elif result_sql:
        st.markdown("SQL being executed:")
        st.code(result_sql, language="sql")

    # Display results
    if result_df is not None:
        st.markdown("Results")
        st.dataframe(result_df, use_container_width=True)
    else:
        st.info("Run a predefined query, custom SQL, or AI-generated SQL to see results here.")
