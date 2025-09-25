from fastapi import APIRouter
from app.api.v1.endpoint.asset import router as asset_router
from app.api.v1.endpoint.search import router as search_router

routers = APIRouter()
router_list = [
    asset_router,
    search_router,
]

for router in router_list:
    routers.include_router(router)
