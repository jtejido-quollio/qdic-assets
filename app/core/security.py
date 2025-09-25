from datetime import datetime, timedelta

from fastapi import Request
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from jose import jwt, jwk
import httpx
from app.core.exceptions import Unauthorized

_jwks_cache = {}


async def get_jwks(issuer: str) -> dict:
    if issuer in _jwks_cache:
        return _jwks_cache[issuer]

    jwks_url = issuer.rstrip("/") + "/.well-known/jwks.json"
    async with httpx.AsyncClient() as client:
        resp = await client.get(jwks_url)
        if resp.status_code != 200:
            raise Unauthorized(detail=f"Could not retrieve JWKS: {resp.text}")
        jwks = resp.json()
        _jwks_cache[issuer] = jwks
        return jwks


class JWTBearer(HTTPBearer):
    def __init__(self, auto_error: bool = True):
        super(JWTBearer, self).__init__(auto_error=auto_error)

    async def __call__(self, request: Request):
        credentials: HTTPAuthorizationCredentials = await super(
            JWTBearer, self
        ).__call__(request)
        if credentials:
            if not credentials.scheme == "Bearer":
                raise Unauthorized(detail="Invalid authentication scheme.")
            if not await self.verify_jwt(credentials.credentials, request):
                raise Unauthorized(detail="Invalid token or expired token.")
            return credentials.credentials
        else:
            raise Unauthorized(detail="Invalid authorization code.")

    async def verify_jwt(self, jwt_token: str, request: Request) -> bool:
        is_token_valid: bool = False
        try:
            unverified_header = jwt.get_unverified_header(jwt_token)
            unverified_payload = jwt.get_unverified_claims(jwt_token)
            issuer = unverified_payload.get("iss")
            request.state.client_id = unverified_payload.get("client_id")

            if not issuer:
                raise Unauthorized(detail="Missing issuer in token")

            jwks = await get_jwks(issuer)
            key_dict = next(
                (k for k in jwks["keys"] if k["kid"] == unverified_header["kid"]), None
            )
            if key_dict is None:
                raise Unauthorized(detail="Signing key not found")
            key = jwk.construct(key_dict)

            decoded = jwt.decode(
                jwt_token,
                key,
                algorithms=[key_dict["alg"]],
                issuer=issuer,
            )

            payload = (
                decoded
                if decoded["exp"] >= int(round(datetime.utcnow().timestamp()))
                else None
            )
        except Exception as e:
            payload = None
        if payload:
            is_token_valid = True
        return is_token_valid
