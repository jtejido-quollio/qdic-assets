from dependency_injector.wiring import Provide
from fastapi import APIRouter, Depends
from app.core.security import JWTBearer
from app.core.container import Container
from app.core.middleware import inject
from app.domain.schemas.search import SearchResponse, SearchRequest
from app.domain.schemas.common import CommonResponse
from app.services.search import SearchService
from app.core.exceptions import NotFound, AssetNotFoundError

router = APIRouter(
    prefix="/search", tags=["search"], dependencies=[Depends(JWTBearer())]
)


@router.post("/assets", response_model=CommonResponse[SearchResponse])
@inject
def search_asset(
    body: SearchRequest,
    service: SearchService = Depends(Provide[Container.search_service]),
):
    try:
        return service.search(body)
    except AssetNotFoundError as e:
        raise NotFound(detail=str(e))
