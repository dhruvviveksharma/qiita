import sys
from openai import OpenAI
from qiita_db.study import Study, StudyPerson
from qiita_db.sql_connection import TRN
import pandas as pd
import json
from dotenv import load_dotenv
import os
load_dotenv()
API_KEY = os.getenv("API_KEY")

# Initialize Claude client
client = OpenAI(
    # This is the default and can be omitted
    api_key = API_KEY,
    base_url = "https://ellm.nrp-nautilus.io/v1"
)

def search_studies_with_sql(custom_sql_where="", params=None):
    """
    Search studies using custom SQL WHERE clause
    
    Parameters
    ----------
    custom_sql_where : str
        Custom WHERE clause (without the WHERE keyword)
    params : list
        Parameters for the SQL query
    
    Returns
    -------
    pd.DataFrame
        DataFrame with study information
    """
    if params is None:
        params = []
    
    with TRN:
        sql = f"""
        SELECT DISTINCT s.study_id, s.study_title, s.study_abstract,
               sp_pi.name as pi_name, sp_pi.email as pi_email, 
               sp_pi.affiliation as pi_affiliation,
               sp_lab.name as lab_person_name
        FROM qiita.study s
        LEFT JOIN qiita.study_person sp_pi 
            ON s.principal_investigator_id = sp_pi.study_person_id
        LEFT JOIN qiita.study_person sp_lab 
            ON s.lab_person_id = sp_lab.study_person_id
        LEFT JOIN qiita.study_artifact sa ON s.study_id = sa.study_id
        LEFT JOIN qiita.artifact a ON sa.artifact_id = a.artifact_id
        LEFT JOIN qiita.visibility v ON a.visibility_id = v.visibility_id
        WHERE {custom_sql_where if custom_sql_where else '1=1'}
        ORDER BY s.study_id
        """
        
        TRN.add(sql, params)
        results = TRN.execute_fetchindex()
    
    if not results:
        return pd.DataFrame()
    
    df = pd.DataFrame(results, columns=[
        'study_id', 'study_title', 'study_abstract', 
        'pi_name', 'pi_email', 'pi_affiliation', 'lab_person_name'
    ])
    
    return df

def llm_query_to_sql(user_query):
    system_prompt = """You are a SQL query generator for a microbiome study database (Qiita).

Available tables and columns:
- s.study_id (integer)
- s.study_title (text)
- s.study_abstract (text)
- sp_pi.name (text) - Principal Investigator name
- sp_pi.email (text) - PI email
- sp_pi.affiliation (text) - PI institution
- sp_lab.name (text) - Lab person name
- v.visibility (text) - Values: 'public', 'private', 'sandbox', 'awaiting_approval'

Your task:
1. Convert the user's natural language query into a PostgreSQL WHERE clause
2. Use ILIKE for case-insensitive text matching (e.g., field ILIKE '%keyword%')
3. Use parameterized queries with %s placeholders
4. Return ONLY a JSON object with 'where_clause' and 'params' fields

Examples:

User: "Find studies about soil microbiome"
Response: {
  "where_clause": "(s.study_title ILIKE %s OR s.study_abstract ILIKE %s)",
  "params": ["%soil%", "%soil%"]
}

User: "Studies by Rob Knight"
Response: {
  "where_clause": "sp_pi.name ILIKE %s",
  "params": ["%Rob Knight%"]
}

Return ONLY valid JSON, no other text."""

    message = client.chat.completions.create(
        model="gemma3",
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_query}
        ]
    )

    response_text = message.choices[0].message.content.strip()
    
    # Remove markdown code blocks if present
    if response_text.startswith("```"):
        response_text = response_text.split("```")[1]
        if response_text.startswith("json"):
            response_text = response_text[4:]
        response_text = response_text.strip()
    
    try:
        result = json.loads(response_text)
        return result
    except json.JSONDecodeError:
        print(f"Warning: Could not parse LLM response: {response_text}")
        # Extract keywords from query
        keywords = user_query.lower().replace("find", "").replace("studies", "").replace("about", "").strip()
        return {
            "where_clause": "(s.study_title ILIKE %s OR s.study_abstract ILIKE %s)",
            "params": [f"%{keywords}%", f"%{keywords}%"]
        }


def smart_search_studies(user_query):
    """
    Search studies using natural language query powered by LLM
    
    Parameters
    ----------
    user_query : str
        Natural language query (e.g., "Find studies about soil by Rob Knight")
    
    Returns
    -------
    pd.DataFrame
        DataFrame with matching studies
    """
    print(f"Processing query: '{user_query}'")
    
    # Convert to SQL using LLM
    sql_query = llm_query_to_sql(user_query)
    
    print("Generated SQL query:", sql_query)
    print(f"Generated WHERE clause: {sql_query['where_clause']}")
    print(f"Parameters: {sql_query['params']}")
    
    # Execute search
    results = search_studies_with_sql(
        custom_sql_where=sql_query['where_clause'],
        params=sql_query['params']
    )
    
    return results


# Example usage:
if __name__ == "__main__":
    print("=" * 80)
    print("LLM-POWERED QIITA STUDY SEARCH")
    print("=" * 80)
    
    # Example queries
    queries = [
        "Find me studies that talk about Sirius Black"
    ]
    
    for query in queries:
        print(f"\n{'=' * 80}")
        print(f"QUERY: {query}")
        print('=' * 80)
        
        results = smart_search_studies(query)
        
        if not results.empty:
            print(f"\nFound {len(results)} studies:\n")
            for _, row in results.iterrows():
                print(f"Study {row['study_id']}: {row['study_title']}")
                print(f"  PI: {row['pi_name']} ({row['pi_affiliation']})")
                print(f"  Abstract: {row['study_abstract'][:200]}...")
                print()
        else:
            print("No studies found matching this query")