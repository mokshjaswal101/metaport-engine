from fastapi import APIRouter, Depends
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials

from context_manager.context import build_request_context

security = HTTPBearer()

# utils
from utils.jwt_token_handler import JWTHandler

# routers
from modules.client import client_router
from modules.company import company_router
from modules.user import user_router
from modules.returns import return_router
from modules.orders import order_router
from modules.pickup_location import pickup_router
from modules.shipment import shipment_router
from modules.wallet import wallet_router
from modules.serviceability import serviceability_router
from modules.dashboard import dashboard_router
from modules.courier_priority import courier_allocation_router
from modules.orders.order_controller import special_orders_router
from modules.byoc.byoc_controller import byoc_router
from modules.returns.return_controller import special_returns_router

from modules.order_tags.order_tags_controller import order_tags_router

from modules.documents.billing_invoice.billing_invoice_controller import (
    billing_invoice_router,
)

# from modules.dashboard import dashboard_router
from modules.discrepancie import discrepancie_router

from modules.ndr import ndr_router

from modules.shipping_notifications.shipping_notifications_controller import (
    notifications_router,
)

from modules.channels.channel_controller import router as channel_router


# settings
from modules.documents.shipping_label.shipping_label_controller import label_router


async def get_current_user(
    credentials: HTTPAuthorizationCredentials = Depends(security),
):
    token = credentials.credentials
    payload = JWTHandler.decode_access_token(token)
    return payload


# create a comming master router for all the routes in the service
CommonRouter = APIRouter(
    prefix="/api/v1",
    dependencies=[Depends(get_current_user), Depends(build_request_context)],
)


# add all the routes to the master router
CommonRouter.include_router(company_router)
CommonRouter.include_router(client_router)
CommonRouter.include_router(user_router)
CommonRouter.include_router(order_router)
# CommonRouter.include_router(return_router)
CommonRouter.include_router(pickup_router)
CommonRouter.include_router(shipment_router)
CommonRouter.include_router(wallet_router)
CommonRouter.include_router(serviceability_router)
CommonRouter.include_router(dashboard_router)
# CommonRouter.include_router(discrepancie_router)
CommonRouter.include_router(special_orders_router)
CommonRouter.include_router(byoc_router)

CommonRouter.include_router(special_returns_router)

CommonRouter.include_router(order_tags_router)

CommonRouter.include_router(notifications_router)

CommonRouter.include_router(label_router)
CommonRouter.include_router(ndr_router)

CommonRouter.include_router(courier_allocation_router)
CommonRouter.include_router(billing_invoice_router)
CommonRouter.include_router(channel_router)
