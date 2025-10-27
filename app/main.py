import os
import json
import logging
import asyncio
import sys
from typing import Any, Dict, List
import uuid
import httpx
from fastapi import HTTPException, Request
from fastapi.responses import JSONResponse
from fastmcp import FastMCP
from fastmcp.server.dependencies import get_http_headers
from pydantic import BaseModel, Field


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logger = logging.getLogger("databricks-mcp")
logger.setLevel(logging.INFO)
if not logger.hasHandlers():
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
    logger.addHandler(handler)

# ---------------------------------------------------------------------------
# Configuration (env-driven)
# ---------------------------------------------------------------------------

DATABRICKS_HOST: str = os.getenv("DATABRICKS_HOST", "https://dbc-c4b6c037-dafc.cloud.databricks.com")
DATABRICKS_WAREHOUSE_ID: str | None = os.getenv("DATABRICKS_WAREHOUSE_ID", "ebe19db6230ee504")

HTTP_TIMEOUT = 180.0  # seconds
POLL_INTERVAL = 0.5  # seconds

# ---------------------------------------------------------------------------
# Databricks SQL helper – single responsibility, keeps tools tiny
# ---------------------------------------------------------------------------

class DatabricksSQLClient:
    """Thin async wrapper around the Databricks SQL Statements API."""

    def __init__(self, host: str, warehouse_id: str, auth_header: str):
        if not warehouse_id:
            raise HTTPException(status_code=500, detail="DATABRICKS_WAREHOUSE_ID env var not set")

        self._warehouse_id = warehouse_id
        self._headers = {"Authorization": auth_header}
        self._client = httpx.AsyncClient(base_url=host, timeout=HTTP_TIMEOUT)

    async def close(self) -> None:
        await self._client.aclose()

    async def run(self, statement: str) -> dict[str, Any]:
        """Submit a statement, poll until completion, and return the payload."""
        logger.info(f"Running SQL statement: {statement}")
        submit = await self._client.post(
            "/api/2.0/sql/statements",
            json={"statement": statement, "warehouse_id": self._warehouse_id, "format": "JSON_ARRAY"},
            headers=self._headers,
        )
        submit.raise_for_status()
        stmt_id = submit.json().get("statement_id")
        if not stmt_id:
            raise HTTPException(status_code=500, detail="Databricks missing statement_id")

        while True:
            status = await self._client.get(f"/api/2.0/sql/statements/{stmt_id}", headers=self._headers)
            status.raise_for_status()
            payload = status.json()
            logger.info(f"SQL execution status: {payload}")
            state = payload.get("status", {}).get("state")
            if state in {"SUCCEEDED", "FAILED", "CANCELED"}:
                return payload
            await asyncio.sleep(POLL_INTERVAL)

# ---------------------------------------------------------------------------
# Pydantic models for MCP tool schemas
# ---------------------------------------------------------------------------

class SearchResult(BaseModel):
    id: str
    title: str
    text: str


class SearchResultPage(BaseModel):
    results: List[SearchResult]


class FetchResult(BaseModel):
    id: str
    title: str
    text: str
    url: str | None = None
    metadata: Dict[str, Any] | None = None


# ---------------------------------------------------------------------------
# FastMCP server
# ---------------------------------------------------------------------------

mcp = FastMCP(
    "GTM Analytics (Databricks MCP)",
    instructions=(
        (
            "Execute SQL on a Databricks warehouse.\n\n"
            "Usage:\n"
            "• Call `search(statement)` to run a SQL query and get a SearchResultPage.\n"
            "• Call `fetch(id)` to retrieve a single row by primary key as a FetchResult.\n\n"
            "Available tables and columns:\n"
            "* workspace.revops.accounts\n"
            "* workspace.revops.opportunities\n"
            "* workspace.revops.usage_metrics\n"
            "\n"
            "Table metadata:\n"
            "workspace.revops.accounts (\n"
            "account_id: unique account identifier,\n"
            "account_name: company / customer name,\n"
            "domain: primary web domain for the account,\n"
            "industry: customer industry segment (e.g. Fintech, Healthcare),\n"
            "segment: sales segment / GTM band (e.g. SMB, Mid-Market, Enterprise),\n"
            "arr_usd: current annual recurring revenue in USD,\n"
            "lifecycle_stage: stage in customer journey (Prospect / Customer / Churned, etc.),\n"
            "account_owner: current AE/CSM owner responsible for the account\n"
            ")\n"
            "\n"
            "workspace.revops.opportunities (\n"
            "opportunity_id: unique opportunity / deal identifier,\n"
            "account_id: FK to accounts.account_id,\n"
            "opportunity_name: descriptive deal name,\n"
            "stage: current pipeline stage (e.g. Qualification, Discovery, Proposal, Negotiation),\n"
            "forecast_category: forecast rollup bucket (Pipeline / Best Case / Commit),\n"
            "acv_usd: expected annual contract value in USD for this opportunity,\n"
            "expected_close_date: projected close date (YYYY-MM-DD),\n"
            "source: acquisition source (e.g. Inbound Demo, Outbound, CS Expansion, Renewal),\n"
            "ae_owner: account executive / deal owner\n"
            ")\n"
            "\n"
            "workspace.revops.usage_metrics (\n"
            "date: usage observation date (YYYY-MM-DD),\n"
            "account_id: FK to accounts.account_id,\n"
            "active_users: number of distinct active end users in the period,\n"
            "seats_provisioned: number of provisioned licenses/seats for the account,\n"
            "feature_adoption_pct: fraction (0–1) of key features adopted by the account,\n"
            "last_active_at: most recent activity timestamp for any user in that account (ISO8601),\n"
            "usage_score: internal health / engagement score (0–100) used for churn/expansion signals\n"
            ")"
        )
    ),
)

# ---------------------------------------------------------------------------
# OAuth discovery & static client registration (unchanged)
# ---------------------------------------------------------------------------

@mcp.custom_route("/.well-known/oauth-authorization-server", methods=["GET"])
async def oauth_metadata(_: Request):
    logger.info("Serving OAuth discovery document")
    return JSONResponse(
        {
            "authorization_endpoint": f"{DATABRICKS_HOST}/oidc/v1/authorize",
            "token_endpoint": f"{DATABRICKS_HOST}/oidc/v1/token",
            "registration_endpoint": "https://databricks-mcp.onrender.com/oauth/register",
            "issuer": f"{DATABRICKS_HOST}/oidc",
            "jwks_uri": "https://oregon.cloud.databricks.com/oidc/jwks.json",
            "scopes_supported": ["all-apis", "email", "offline_access", "openid", "profile", "sql"],
            "response_types_supported": ["code", "id_token"],
            "response_modes_supported": ["query", "fragment", "form_post"],
            "grant_types_supported": ["client_credentials", "authorization_code", "refresh_token"],
            "code_challenge_methods_supported": ["S256"],
            "token_endpoint_auth_methods_supported": ["client_secret_post"],
            "subject_types_supported": ["public"],
            "id_token_signing_alg_values_supported": ["RS256"],
            "claims_supported": [
                "iss",
                "sub",
                "aud",
                "iat",
                "exp",
                "jti",
                "name",
                "family_name",
                "given_name",
                "preferred_username",
            ],
            "request_uri_parameter_supported": False,
        }
    )


@mcp.custom_route("/oauth/register", methods=["POST"])
async def register(_: Request):
    logger.info("Handling OAuth client registration request")
    return JSONResponse(
        {
            "client_id": os.getenv("DATABRICKS_CLIENT_ID", "demo-public-client"),
            "client_secret": os.getenv("DATABRICKS_CLIENT_SECRET", "demo-public-secret"),
            "scopes": ["all-apis", "offline_access", "openid", "profile", "email"],
            "token_endpoint_auth_method": "client_secret_post",
        }
    )

# ---------------------------------------------------------------------------
# Utility functions
# ---------------------------------------------------------------------------

def _rows_to_dicts(columns: list[str], rows: list[list[Any]]) -> list[dict[str, Any]]:
    return [dict(zip(columns, r)) for r in rows]


def _get_sql_client() -> DatabricksSQLClient:
    auth = get_http_headers().get("authorization") or get_http_headers().get("Authorization")
    if not auth:
        logger.warning("Missing Authorization header")
        raise HTTPException(status_code=401, detail="Missing Authorization header")
    return DatabricksSQLClient(DATABRICKS_HOST, DATABRICKS_WAREHOUSE_ID, auth)

# ---------------------------------------------------------------------------
# Tool: search
# ---------------------------------------------------------------------------

@mcp.tool(
    name="search",
    description=(
        "Run a SQL statement and return results. You have access to the following tables:\n"
        "* workspace.revops.accounts\n"
        "* workspace.revops.opportunities\n"
        "* workspace.revops.usage_metrics\n"
        "\n"
        "Table metadata:\n"
        "workspace.revops.accounts (\n"
        "account_id: unique account identifier,\n"
        "account_name: company / customer name,\n"
        "domain: primary web domain for the account,\n"
        "industry: customer industry segment (e.g. Fintech, Healthcare),\n"
        "segment: sales segment / GTM band (e.g. SMB, Mid-Market, Enterprise),\n"
        "arr_usd: current annual recurring revenue in USD,\n"
        "lifecycle_stage: stage in customer journey (Prospect / Customer / Churned, etc.),\n"
        "account_owner: current AE/CSM owner responsible for the account\n"
        ")\n"
        "\n"
        "workspace.revops.opportunities (\n"
        "opportunity_id: unique opportunity / deal identifier,\n"
        "account_id: FK to accounts.account_id,\n"
        "opportunity_name: descriptive deal name,\n"
        "stage: current pipeline stage (e.g. Qualification, Discovery, Proposal, Negotiation),\n"
        "forecast_category: forecast rollup bucket (Pipeline / Best Case / Commit),\n"
        "acv_usd: expected annual contract value in USD for this opportunity,\n"
        "expected_close_date: projected close date (YYYY-MM-DD),\n"
        "source: acquisition source (e.g. Inbound Demo, Outbound, CS Expansion, Renewal),\n"
        "ae_owner: account executive / deal owner\n"
        ")\n"
        "\n"
        "workspace.revops.usage_metrics (\n"
        "date: usage observation date (YYYY-MM-DD),\n"
        "account_id: FK to accounts.account_id,\n"
        "active_users: number of distinct active end users in the period,\n"
        "seats_provisioned: number of provisioned licenses/seats for the account,\n"
        "feature_adoption_pct: fraction (0–1) of key features adopted by the account,\n"
        "last_active_at: most recent activity timestamp for any user in that account (ISO8601),\n"
        "usage_score: internal health / engagement score (0–100) used for churn/expansion signals\n"
        ")"
    )
)
async def search(query: str):
    logger.info(f"search() called with query: {query}")
    client = _get_sql_client()
    
    payload = await client.run(query)
    logger.info(f"Payload: {payload}")
    if ("result" not in payload) or ("data_array" not in payload["result"]):
        random_id = str(uuid.uuid4())
        formatted_payload = [
            {
                "id": str(random_id),
                "title": "Databricks SQL Query Result",
                "text": str(payload),
                "url": "databricks.com"
            }
        ]
        logger.info(f"Formatted payload: {formatted_payload}")
        return {"results": formatted_payload}
    
    cols = [c["name"] for c in payload["manifest"]["schema"]["columns"]]
    rows = payload["result"]["data_array"]

    dict_rows = _rows_to_dicts(cols, rows)
    results = [
        {
            "id": str(r["id"]) if "id" in r else str(uuid.uuid4()),
            "title": "Databricks SQL Query Result",
            "text": str(r)
        }
        for idx, r in enumerate(dict_rows)
    ]
    return {"results": results}
# ---------------------------------------------------------------------------
# Tool: fetch
# ---------------------------------------------------------------------------

@mcp.tool(name="fetch", description="Retrieve a single row from the table by primary key value. If you do not have the primary key, you can use the search tool to get the id of the row you want to fetch. This will search for the ID across all tables and return the first row that matches.")
async def fetch(id: str):
    client = _get_sql_client()
    # Join all possible tables to pick up the id across all of them, using fully qualified table names
    statement = f"""
        SELECT *
        FROM workspace.gtm.order_items AS oi
        LEFT JOIN workspace.gtm.customers AS c
            ON oi.customer_id = c.customer_id
        LEFT JOIN workspace.gtm.products AS p
            ON oi.product_id = p.product_id
        -- Minimal awareness of claims table: join by the input policy_id
        LEFT JOIN workspace.mcp.insurance_claims_data AS icd
            ON CAST(icd.policy_id AS STRING) = CAST({id} AS STRING)
        WHERE
            oi.id = {id}
            OR oi.order_id = {id}
            OR oi.product_id = {id}
            OR c.customer_id = {id}
            OR p.product_id = {id}
            OR CAST(icd.policy_id AS STRING) = CAST({id} AS STRING)
        LIMIT 1
    """
    payload = await client.run(statement)
    if "result" not in payload:
        logger.error(f"No 'result' in Databricks SQL response: {payload}")
        raise HTTPException(status_code=502, detail="No results returned from Databricks SQL API")

    cols = [c["name"] for c in payload["manifest"]["schema"]["columns"]]
    rows = payload["result"]["data_array"]
    if "data_array" not in payload["result"]:
        logger.error(f"No 'data_array' in Databricks SQL response: {payload}")
        return {
            "id": str(id),
            "title": str(id),
            "text": payload,
            "metadata": None
        }
    
    if not rows:
        raise HTTPException(status_code=404, detail="Row not found")

    row_dict = _rows_to_dicts(cols, rows)[0]
    return {
        "id": str(id),
        "title": str(id),
        "text": json.dumps(row_dict, default=str),
        "metadata": row_dict
    }


# ---------------------------------------------------------------------------
# Expose ASGI app
# ---------------------------------------------------------------------------

try:
    app = mcp.http_app(transport="sse")
    logger.info("ASGI app created with SSE transport")
except AttributeError:
    app = mcp
    logger.info("ASGI app created without SSE transport")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    logger.info("Starting Databricks MCP server")
    mcp.run(transport="sse")
