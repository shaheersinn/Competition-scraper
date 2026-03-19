"""MongoDB connection — singleton client."""
from pymongo import MongoClient
from pymongo.collection import Collection
from .config import settings

_client = None

def _get_client() -> MongoClient:
    global _client
    if _client is None:
        _client = MongoClient(settings.MONGO_URI)
    return _client

def get_collection(name: str) -> Collection:
    return _get_client()[settings.MONGO_DB][name]

def create_indexes():
    """Run once on startup to ensure fast queries."""
    col = get_collection("cases")
    col.create_index("source")
    col.create_index("act_sections")
    col.create_index("date_decision")
    col.create_index([("full_text", "text"), ("title", "text")])  # full-text search
