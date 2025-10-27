import os
from typing import List, Dict

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import DatabricksError

# Lazily initialise so a missing env var throws at first real use
def _client() -> WorkspaceClient:
    return WorkspaceClient(
        host=os.environ["DATABRICKS_WORKSPACE_URL"],
        client_id=os.environ["DATABRICKS_CLIENT_ID"],
        client_secret=os.environ["DATABRICKS_CLIENT_SECRET"],
    )

# ---------- Replace the two stubs below with real SQL / Lakehouse calls ----------

async def search_records(query: str) -> List[Dict]:
    """
    TODO: Replace with a Databricks SQL query that returns IDs + snippets.
    """
    # Example placeholder: return any records containing the query in lower-case.
    from pathlib import Path, json
    records = json.loads(Path(__file__).with_name("records.json").read_text())
    toks = query.lower().split()
    out = []
    for r in records:
        hay = " ".join([r["title"], r["text"], " ".join(r.get("metadata", {}).values())]).lower()
        if any(t in hay for t in toks):
            out.append({"id": r["id"], "title": r["title"], "text": r["text"][:120], "url": r.get("url")})
    return out


async def fetch_record(record_id: str) -> Dict:
    """
    TODO: Replace with a Databricks SQL lookup by primary key.
    """
    from pathlib import Path, json
    records = {r["id"]: r for r in json.loads(Path(__file__).with_name("records.json").read_text())}
    if record_id not in records:
        raise KeyError("unknown id")
    return records[record_id]
