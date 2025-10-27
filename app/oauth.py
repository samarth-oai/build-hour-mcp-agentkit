"""
Very small OAuth façade for ChatGPT to proxy PKCE-based OAuth (U2M) to Databricks.

Supports:
- Static client registration (databricks-cli) for ChatGPT
- Metadata advertisement with PKCE
- Token exchange proxy without client secret

In production, consider adding logging, security, and user session validation.
"""

import os
from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, Field, SecretStr
import httpx
import logging

router = APIRouter()

# -------------------- OAuth Metadata --------------------
AZ_METADATA = {
    "issuer": os.getenv("MCP_ISSUER_URL", "https://databricks-mcp.onrender.com"),
    "authorization_endpoint": f"https://{os.getenv('DATABRICKS_WORKSPACE_URL')}/oidc/v1/authorize",
    "token_endpoint": f"https://{os.getenv('DATABRICKS_WORKSPACE_URL')}/oidc/v1/token",
    "registration_endpoint": "https://databricks-mcp.onrender.com/oauth/register",
    "response_types_supported": ["code"],
    "grant_types_supported": ["authorization_code", "refresh_token"],
    "token_endpoint_auth_methods_supported": ["client_secret_post"],
    "token_endpoint_auth_method": "client_secret_post",
    "code_challenge_methods_supported": ["S256"],
    "custom_redirect_url_params": {
        "scopes": ["all-apis"],
        "scope": "all-apis"
    }
}

@router.get("/.well-known/oauth-authorization-server")
async def oauth_metadata():
    return AZ_METADATA

# -------------------- Client Registration --------------------
class RegReq(BaseModel):
    client_name: str
    redirect_uris: list[str] = Field(min_length=1)
    grant_types: list[str] | None = None
    response_types: list[str] | None = None
    token_endpoint_auth_method: str | None = None

class RegResp(BaseModel):
    client_id: str
    client_secret: SecretStr
    scopes: list[str]
    token_endpoint_auth_method: str

@router.post("/oauth/register", response_model=RegResp, status_code=201)
async def register(req: RegReq):
    # log the request
    logging.info(req)

    # Always return the Databricks-supported static public client
    return RegResp(
        client_id=os.getenv("DATABRICKS_CLIENT_ID"),
        client_secret=SecretStr(os.getenv("DATABRICKS_CLIENT_SECRET")),
        scopes=["all-apis"],
        token_endpoint_auth_method="client_secret_post"
    )

# -------------------- Token Exchange Proxy --------------------
@router.post("/oauth/token")
async def proxy_token(request: Request):
    """
    ChatGPT will POST here with an authorization code and PKCE verifier.
    We simply proxy the token request to Databricks without basic auth.
    """
    async with httpx.AsyncClient() as client:
        resp = await client.post(
            f"https://{os.getenv('DATABRICKS_WORKSPACE_URL')}/oidc/v1/token",
            data=await request.body(),
            headers={
                "Content-Type": request.headers.get("content-type", "application/x-www-form-urlencoded")
            },
            timeout=30,
        )
    return resp.json()
