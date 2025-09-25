from fastapi import FastAPI, Request, HTTPException, status
from typing import Any, Dict, Optional
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException


class InternalServer(HTTPException):
    def __init__(
        self, detail: Any = None, headers: Optional[Dict[str, Any]] = None
    ) -> None:
        super().__init__(status.HTTP_500_INTERNAL_SERVER_ERROR, detail, headers)


class NotFound(HTTPException):
    def __init__(
        self, detail: Any = None, headers: Optional[Dict[str, Any]] = None
    ) -> None:
        super().__init__(status.HTTP_404_NOT_FOUND, detail, headers)


class BadRequest(HTTPException):
    def __init__(
        self, detail: Any = None, headers: Optional[Dict[str, Any]] = None
    ) -> None:
        super().__init__(status.HTTP_400_BAD_REQUEST, detail, headers)


class Forbidden(HTTPException):
    def __init__(
        self, detail: Any = None, headers: Optional[Dict[str, Any]] = None
    ) -> None:
        super().__init__(status.HTTP_403_FORBIDDEN, detail, headers)


class Unauthorized(HTTPException):
    def __init__(
        self, detail: Any = None, headers: Optional[Dict[str, Any]] = None
    ) -> None:
        super().__init__(status.HTTP_401_UNAUTHORIZED, detail, headers)


# exceptions
class BadRequestError(Exception):
    def __init__(self, detail: str):
        super().__init__(detail)


class NotFoundError(Exception):
    def __init__(self, detail: str):
        super().__init__(detail)


class AssetNotFoundError(Exception):
    def __init__(self, asset_id: str):
        super().__init__(f"Asset not found: {asset_id}")


def exception_container(app: FastAPI) -> None:
    @app.exception_handler(HTTPException)
    async def common_exception_handler(request: Request, exc: HTTPException):
        return JSONResponse(
            status_code=exc.status_code,
            content={
                "code": exc.status_code,
                "http_error": exc.__class__.__name__,
                "message": exc.detail,
            },
        )
