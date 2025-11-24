from fastapi import APIRouter, Depends

from context_manager.context import build_request_context

# utils
from utils.jwt_token_handler import JWTHandler

# routers
from modules.razorpay import razorpay_router
from modules.payu.payu_controller import payu_router
from marketplace.woocommerce.woocommerce_controller import woocommerce_router
from marketplace.shopify.shopify_controller import shopify_router
from marketplace.easyecom.easyecom_controller import easyecom_router
from marketplace.magento.magento_controller import magento_router

from shipping_partner.logistify.logistify_controller import logistify_router
from shipping_partner.shiprocket.shiprocket_controller import shiprocket_router
from shipping_partner.delhivery.delhivery_controller import delhivery_router
from shipping_partner.shipmozo.shipmozo_controller import shipmozo_router
from shipping_partner.ecom.ecom_controller import ecom_router
from shipping_partner.xpressbees.xpressbees_controller import xpressbees_router
from shipping_partner.dtdc.dtdc_controller import dtdc_router
from shipping_partner.ekart.ekart_controller import ekart_router
from shipping_partner.ats.ats_controller import ats_router
from shipping_partner.shadowfax.shadowfax_controller import shadowfax_router
from shipping_partner.blitz.blitz_controller import blitz_router
from shipping_partner.bluedart.bluedart_controller import bluedart_router
from shipping_partner.zippyy.zippyy_controller import zippyy_router

from modules.shipment.shipment_controller import track_router

from modules.user.user_controller import user_router


# create a comming master router for all the routes in the service
OpenRouter = APIRouter(prefix="/api/v1", dependencies=[Depends(build_request_context)])


# add all the routes to the master router
OpenRouter.include_router(razorpay_router)
OpenRouter.include_router(payu_router)

OpenRouter.include_router(woocommerce_router)
OpenRouter.include_router(shopify_router)
OpenRouter.include_router(easyecom_router)
OpenRouter.include_router(magento_router)

OpenRouter.include_router(track_router)

OpenRouter.include_router(shiprocket_router)
OpenRouter.include_router(logistify_router)
OpenRouter.include_router(delhivery_router)
OpenRouter.include_router(shipmozo_router)
OpenRouter.include_router(ecom_router)
OpenRouter.include_router(xpressbees_router)
OpenRouter.include_router(dtdc_router)
OpenRouter.include_router(ekart_router)
OpenRouter.include_router(ats_router)
OpenRouter.include_router(shadowfax_router)
OpenRouter.include_router(blitz_router)
OpenRouter.include_router(bluedart_router)
OpenRouter.include_router(zippyy_router)

OpenRouter.include_router(user_router)  # Includes OTP endpoints: /user/otp/*
