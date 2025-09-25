from typing import Annotated
from dependency_injector.wiring import Provide
from fastapi import APIRouter, Depends, Header, Request
from app.core.security import JWTBearer
from app.core.container import Container
from app.core.middleware import inject
from app.domain.schemas.asset import Asset
from app.domain.schemas.common import CommonResponse
from app.services.asset import AssetService
from app.core.exceptions import (
    NotFound,
    AssetNotFoundError,
    BadRequest,
    BadRequestError,
)


router = APIRouter(
    prefix="/assets", tags=["assets"], dependencies=[Depends(JWTBearer())]
)


@router.get("/{asset_id}", response_model=CommonResponse[Asset])
@inject
def get_asset(
    asset_id: str,
    service: AssetService = Depends(Provide[Container.asset_service]),
):
    try:
        return service.get_asset_details(asset_id)
    except AssetNotFoundError as e:
        raise NotFound(detail=str(e))


@router.delete("/{asset_id}")
@inject
def delete_asset(
    asset_id: str,
    request: Request,
    service: AssetService = Depends(Provide[Container.asset_service]),
):
    try:
        return service.delete_asset(asset_id, request.state.client_id)
    except AssetNotFoundError as e:
        raise NotFound(detail=str(e))
    except BadRequestError as e:
        raise BadRequest(detail=str(e))
