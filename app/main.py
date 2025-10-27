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
            "  - workspace.gtm.customers:\n"
            "      • customer_id: unique customer ID\n"
            "      • name: full name\n"
            "      • email: email address\n"
            "      • age: customer age\n"
            "      • gender: male/female/other\n"
            "      • region: geographic region\n"
            "      • signup_date: account creation date\n"
            "      • loyalty_status: new/silver/gold/platinum\n"
            "      • lifetime_value: total revenue from this customer\n"
            "      • last_login: last active date\n"
            "      • preferred_device: mobile/desktop/tablet\n"
            "  - workspace.gtm.products:\n"
            "      • product_id: unique SKU ID\n"
            "      • name: product name\n"
            "      • category: always 'coffee_gear'\n"
            "      • subcategory: item type like espresso machine\n"
            "      • brand: product brand\n"
            "      • price: selling price in USD\n"
            "      • stock_quantity: inventory available\n"
            "      • cost_of_goods_sold: estimated cost basis\n"
            "  - workspace.gtm.orders:\n"
            "      • order_id: unique order ID\n"
            "      • customer_id: FK to customer\n"
            "      • order_date: purchase date\n"
            "      • total_amount: order total incl. shipping\n"
            "      • discount_code_used: promo code or blank\n"
            "      • shipping_fee: cost of delivery\n"
            "      • payment_method: credit_card/paypal/etc.\n"
            "      • order_status: completed/returned/cancelled\n"
            "      • shipping_country: delivery destination\n"
            "  - workspace.gtm.order_items:\n"
            "      • order_id: FK to orders\n"
            "      • product_id: FK to products\n"
            "      • quantity: number of units\n"
            "      • item_price: per-unit price\n"
            "      • discount_applied: discount in dollars\n"
            "      • review_score: 1–5 star rating\n"
            "  - workspace.gtm.sessions:\n"
            "      • session_id: unique session ID\n"
            "      • customer_id: FK to customer\n"
            "      • visit_time: session timestamp\n"
            "      • pages_viewed: number of pages\n"
            "      • entry_page: first page type\n"
            "      • exit_page: last page type\n"
            "      • device_type: mobile/desktop/tablet\n"
            "  - workspace.mcp.insurance_claims_data:\n"
            "      • policy_id: unique policy identifier\n"
            "      • subscription_length: policy subscription length (months)\n"
            "      • vehicle_age: vehicle age (years)\n"
            "      • customer_age: customer age (years)\n"
            "      • region_code: geographic region code\n"
            "      • region_density: population density for region\n"
            "      • segment: vehicle segment/class\n"
            "      • model: vehicle model code/name\n"
            "      • fuel_type: fuel category (e.g., Diesel, Petrol)\n"
            "      • max_torque: engine max torque (as given)\n"
            "      • max_power: engine max power (as given)\n"
            "      • engine_type: engine family/variant\n"
            "      • airbags: number of airbags\n"
            "      • is_esc: electronic stability control present (yes/no)\n"
            "      • is_adjustable_steering: adjustable steering present (yes/no)\n"
            "      • is_tpms: tire pressure monitoring system present (yes/no)\n"
            "      • is_parking_sensors: parking sensors present (yes/no)\n"
            "      • is_parking_camera: parking camera present (yes/no)\n"
            "      • rear_brakes_type: rear brake type\n"
            "      • displacement: engine displacement (cc)\n"
            "      • cylinder: number of cylinders\n"
            "      • transmission_type: transmission type\n"
            "      • steering_type: steering system type\n"
            "      • turning_radius: minimum turning radius (m)\n"
            "      • length: vehicle length (mm)\n"
            "      • width: vehicle width (mm)\n"
            "      • gross_weight: gross vehicle weight (kg)\n"
            "      • is_front_fog_lights: front fog lights present (yes/no)\n"
            "      • is_rear_window_wiper: rear window wiper present (yes/no)\n"
            "      • is_rear_window_washer: rear window washer present (yes/no)\n"
            "      • is_rear_window_defogger: rear window defogger present (yes/no)\n"
            "      • is_brake_assist: brake assist present (yes/no)\n"
            "      • is_power_door_locks: power door locks present (yes/no)\n"
            "      • is_central_locking: central locking present (yes/no)\n"
            "      • is_power_steering: power steering present (yes/no)\n"
            "      • is_driver_seat_height_adjustable: driver seat height adjust present (yes/no)\n"
            "      • is_day_night_rear_view_mirror: day/night rear-view mirror present (yes/no)\n"
            "      • is_ecw: emergency call/warning present (yes/no)\n"
            "      • is_speed_alert: speed alert present (yes/no)\n"
            "      • ncap_rating: safety rating (stars)\n"
            "      • claim_status: claim label (0/1)\n"
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
        "  - workspace.mcp.insurance_claims_data:\n"
        "  - workspace.gtm.customers\n"
        "  - workspace.gtm.products\n"
        "  - workspace.gtm.orders\n"
        "  - workspace.gtm.order_items\n"
        "  - workspace.gtm.sessions\n"
        "\n"
        "Table metadata:\n"
        " workspace.mcp.insurance_claims_data:\n"
        "    policy_id: unique policy identifier\n"
        "    subscription_length: policy subscription length (months)\n"
        "    vehicle_age: vehicle age (years)\n"
        "    customer_age: customer age (years)\n"
        "    region_code: geographic region code\n"
        "    region_density: population density for region\n"
        "    segment: vehicle segment/class\n"
        "    model: vehicle model code/name\n"
        "    fuel_type: fuel category (e.g., Diesel, Petrol)\n"
        "    max_torque: engine max torque (as given)\n"
        "    max_power: engine max power (as given)\n"
        "    engine_type: engine family/variant\n"
        "    airbags: number of airbags\n"
        "    is_esc: electronic stability control present (yes/no)\n"
        "    is_adjustable_steering: adjustable steering present (yes/no)\n"
        "    is_tpms: tire pressure monitoring system present (yes/no)\n"
        "    is_parking_sensors: parking sensors present (yes/no)\n"
        "    is_parking_camera: parking camera present (yes/no)\n"
        "    rear_brakes_type: rear brake type\n"
        "    displacement: engine displacement (cc)\n"
        "    cylinder: number of cylinders\n"
        "    transmission_type: transmission type\n"
        "    steering_type: steering system type\n"
        "    turning_radius: minimum turning radius (m)\n"
        "    length: vehicle length (mm)\n"
        "    width: vehicle width (mm)\n"
        "    gross_weight: gross vehicle weight (kg)\n"
        "    is_front_fog_lights: front fog lights present (yes/no)\n"
        "    is_rear_window_wiper: rear window wiper present (yes/no)\n"
        "    is_rear_window_washer: rear window washer present (yes/no)\n"
        "    is_rear_window_defogger: rear window defogger present (yes/no)\n"
        "    is_brake_assist: brake assist present (yes/no)\n"
        "    is_power_door_locks: power door locks present (yes/no)\n"
        "    is_central_locking: central locking present (yes/no)\n"
        "    is_power_steering: power steering present (yes/no)\n"
        "    is_driver_seat_height_adjustable: driver seat height adjust present (yes/no)\n"
        "    is_day_night_rear_view_mirror: day/night rear-view mirror present (yes/no)\n"
        "    is_ecw: emergency call/warning present (yes/no)\n"
        "    is_speed_alert: speed alert present (yes/no)\n"
        "    ncap_rating: safety rating (stars)\n"
        "    claim_status: claim label (0/1)\n"        
        "workspace.gtm.customers (\n"
        "    customer_id: unique customer ID,\n"
        "    name: full name,\n"
        "    email: email address,\n"
        "    age: customer age,\n"
        "    gender: male/female/other,\n"
        "    region: geographic region,\n"
        "    signup_date: account creation date,\n"
        "    loyalty_status: new/silver/gold/platinum,\n"
        "    lifetime_value: total revenue from this customer,\n"
        "    last_login: last active date,\n"
        "    preferred_device: mobile/desktop/tablet\n"
        ")\n"
        "workspace.gtm.products (\n"
        "    product_id: unique SKU ID,\n"
        "    name: product name,\n"
        "    category: always 'coffee_gear',\n"
        "    subcategory: item type like espresso machine,\n"
        "    brand: product brand,\n"
        "    price: selling price in USD,\n"
        "    stock_quantity: inventory available,\n"
        "    cost_of_goods_sold: estimated cost basis\n"
        ")\n"
        "workspace.gtm.orders (\n"
        "    order_id: unique order ID,\n"
        "    customer_id: FK to customer,\n"
        "    order_date: purchase date,\n"
        "    total_amount: order total incl. shipping,\n"
        "    discount_code_used: promo code or blank,\n"
        "    shipping_fee: cost of delivery,\n"
        "    payment_method: credit_card/paypal/etc.,\n"
        "    order_status: completed/returned/cancelled,\n"
        "    shipping_country: delivery destination\n"
        ")\n"
        "workspace.gtm.order_items (\n"
        "    order_id: FK to orders,\n"
        "    product_id: FK to products,\n"
        "    quantity: number of units,\n"
        "    item_price: per-unit price,\n"
        "    discount_applied: discount in dollars,\n"
        "    review_score: 1–5 star rating\n"
        ")\n"
        "workspace.gtm.sessions (\n"
        "    session_id: unique session ID,\n"
        "    customer_id: FK to customer,\n"
        "    visit_time: session timestamp,\n"
        "    pages_viewed: number of pages,\n"
        "    entry_page: first page type,\n"
        "    exit_page: last page type,\n"
        "    device_type: mobile/desktop/tablet\n"
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
