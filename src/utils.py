# src/utils.py
from sqlalchemy import create_engine, inspect, MetaData
from sqlalchemy.engine import Engine
import json, os
from langchain_community.embeddings import OpenAIEmbeddings
from langchain_community.vectorstores import FAISS

# def dump_schema(uri: str, out="data/schema_snapshot.json"):
#     eng = create_engine(uri)
#     insp = inspect(eng)
#     md = MetaData()
#     md.reflect(eng)
#     schema = {
#         t.name: [c.name for c in t.columns]
#         for t in md.sorted_tables
#     }
#     os.makedirs("data", exist_ok=True)
#     json.dump(schema, open(out, "w"), indent=2)

def get_live_schema(engine: Engine) -> dict:
    """
    Reflects all tables and columns from a live DB connection.
    Returns: {table_name: [col1, col2, ...]}
    """
    metadata = MetaData()
    metadata.reflect(engine)
    schema = {
        table.name: [col.name for col in table.columns]
        for table in metadata.sorted_tables
    }
    return schema

def build_syn_index(schema_json="data/schema_snapshot.json"):
    cols = [f"{tbl}.{col}" for tbl, cols in json.load(open(schema_json)).items() for col in cols]
    vect = FAISS.from_texts(cols, OpenAIEmbeddings())
    vect.save_local("data/schema.faiss")

def similar_terms(term: str, k=5):
    vect = FAISS.load_local("data/schema.faiss", OpenAIEmbeddings())
    return [t for t, _ in vect.similarity_search_with_score(term, k=k)]

if __name__ == "__main__":
    from dotenv import load_dotenv; load_dotenv()
    # dump_schema(os.getenv("DATABASE_URL"))
    build_syn_index()


