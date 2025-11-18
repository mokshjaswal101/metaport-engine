import http
from fastapi import APIRouter, Depends, HTTPException, Header, Request, Query
from fastapi import APIRouter, Depends
from fastapi.security import HTTPBearer
import hmac, hashlib, requests, urllib.parse, os
from fastapi.responses import RedirectResponse
from fastapi.responses import JSONResponse
import base64
import asyncio
import secrets
import string
from urllib.parse import urlencode
from fastapi.responses import HTMLResponse
from context_manager.context import context_user_data, get_db_session
from fastapi.encoders import jsonable_encoder
from urllib.parse import quote
from datetime import datetime, timezone
import httpx
import time
from datetime import datetime
from urllib.parse import quote_plus

# schema
from schema.base import GenericResponseModel

# utils
from utils.response_handler import build_api_response
from utils.jwt_token_handler import JWTHandler

# service
from .shopify_service import Shopify

# creating a client router
shopify_router = APIRouter(tags=["shopify"])
import secrets

security = HTTPBearer()
SHOPIFY_API_KEY = "a2a2db72d3f5da969329f56c2cca68ac"
SHOPIFY_API_SECRET = os.getenv("SHOPIFY_API_SECRET", "2e3c3e453c65bf4f2cc290af738bf550")
# SCOPES = "read_orders, write_orders, read_fulfillments, write_fulfillments"
SCOPES = "read_orders, write_orders, read_fulfillments, write_fulfillments, read_customers, write_customers"
SERVER_NGROK_LINK = "https://api.lastmiles.co"
CALLBACK_PATH = f"{SERVER_NGROK_LINK}/api/v1/market-place/shopify/auth"


REDIRECT_URI = CALLBACK_PATH

from models import (
    Market_Place,
)


def generate_state():
    return secrets.token_urlsafe(16)


def uninstall_verify_webhook(data: bytes, hmac_header: str) -> bool:
    digest = hmac.new(SHOPIFY_API_SECRET.encode("utf-8"), data, hashlib.sha256).digest()
    computed_hmac = base64.b64encode(digest).decode()
    return hmac.compare_digest(computed_hmac, hmac_header)


@shopify_router.get("/market-place/shopify/front", response_class=HTMLResponse)
def shopify_app_landing(request: Request):
    shop = request.query_params.get("shop")
    host = request.query_params.get("host")
    server_url = SERVER_NGROK_LINK
    print(shop, "welcome to shopify front action", host)
    if not shop or not host:
        print("Mission shop and host")
        return HTMLResponse("<h3>Missing shop or host</h3>", status_code=400)

    # Fake redirect logic — replace with your DB call
    with get_db_session() as db:
        # print(shop, "dddddddddddddddd")
        store = (
            db.query(Market_Place)
            .filter(Market_Place.which_market_place == shop)
            .first()
        )
        print(jsonable_encoder(store))
        if store is not None and store.access_token is None:
            print("welcome to none section")
            return "Successfully install"
            timestamp = int(time.time())
            start_dt = datetime.now()
            print(
                "Start if first is not able to rerect then again hit :",
                start_dt.strftime("%Y-%m-%d %H:%M:%S"),
            )
            redirect_uri = f"{SERVER_NGROK_LINK}/api/v1/market-place/shopify/success"
            encoded_redirect_uri = quote(redirect_uri, safe="")
            install_uri = (
                f"https://{shop}/admin/oauth/authorize"
                f"?client_id={SHOPIFY_API_KEY}"
                f"&scope=read_orders,write_orders,read_fulfillments,write_fulfillments"
                f"&redirect_uri={encoded_redirect_uri}"
                f"&state={store.oauth_state}"
                f"&nonce={secrets.token_urlsafe(8)}"
                f"&timestamp={timestamp}"
                f"&grant_options[]=per-user"
            )
            print("Install URL again hit:", install_uri)
            print(
                "redirect hit at this time again hit",
                start_dt.strftime("%Y-%m-%d %H:%M:%S"),
            )
            return RedirectResponse(install_uri)

    # return "ssssss"
    if not store:
        print(f"welcome to {shop},  name is not exist into db ")
        return RedirectResponse(
            f"{server_url}/api/v1/market-place/shopify/auth?shop={shop}&host={host}"
        )

    html_content = f"""
    <!DOCTYPE html>
    <html>
    <head>
      <title>Warehousity App</title>
      <script src="https://unpkg.com/@shopify/app-bridge@3"></script>
      <style>       
        #app {{        
            padding: 0px 0px;
            border-radius: 16px;
            text-align: center;
            max-width: 600px;
        }}
        h1 {{
            font-size: 32px;
            margin-bottom: 20px;
            /* Colorful gradient text */
            background: linear-gradient(90deg, #FF6B6B, #FFD93D, #6BCB77, #4D96FF);
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
            font-weight: 700;
        }}
         p {{
            font-size: 18px;
            color: #4a4a4a;
            line-height: 1.6;
            margin: 0 auto;
            max-width: 900px; 
        }}
        h1 {{
            margin: 0 auto;
            max-width: 700px;
        }}
      </style>
    </head>
    <body>
      <script>
        // Force iframe redirect if not already embedded
        if (window.top === window.self) {{
          var params = new URLSearchParams(window.location.search);
          var redirectUri = "/market-place/shopify/front?" + params.toString();
          window.top.location.href = redirectUri;
        }}
      </script>
        <div>
            <h1>Warehousity App Installed Successfully!</h1>
            <p>Fulfilment is an important spoke in the journey of the product from Manufacturing to the consumer. It has come up in many studies that on an average, organisations spend about 30-90 days from ‘end of planning’ to ‘executing a warehousing project’. That’s a lot of time, especially in the Internet Era.

The process starts with looking for a raw warehouse. And you know the archaic ways of finding warehouses, and how cumbersome they are. And then lots of time is spent in manual activities to do the due diligence of warehouse.

Putting up a team together and training them to ensure desired Operations Quality largely remain expectations despite investment of money & time by organisations. Realtime Inventory visibility and Compliances at Warehouse are another aspects where acute challenges exist as 90% of the warehousing in India are said to Un-organised.

Considering the above, can you deduce the ultimate speed an organisation can attain for scaling the network & business? It is nowhere near to match up the speed of the world. No wonder why the Logistics cost in India is a whopping 14% of the GDP!

We at WAREHOUSITY, aim to solve these challenges for you ~ Fulfilment Warehousing Digitally Delivered !.</p>
<h1>Last Mile!</h1>
<p>Last mile logistics, also known as last-mile delivery, is the final stage of the supply chain, encompassing the transportation of goods from a distribution center or warehouse to the customer's doorstep or final destination. It's a critical and often challenging part of the logistics process, impacting customer satisfaction and brand perception.
<br />
CORPORATE OFFICE
<br />
New Delhi, India
<br />
sales@warehousity.com 
<br />
</p>
        </div>
      <script>
        var AppBridge = window['app-bridge'];
        var createApp = AppBridge.default;
        var app = createApp({{
          apiKey: '{SHOPIFY_API_KEY}',
          host: '{host}',
          forceRedirect: true,
        }});
      </script>
    </body>
    </html>
    """

    return HTMLResponse(
        content=html_content,
        headers={
            "Content-Security-Policy": f"frame-ancestors https://{shop} https://admin.shopify.com;",
            "X-Frame-Options": "ALLOWALL",
        },
    )
    # return RedirectResponse("https://cn.omneelab.com/success.php?0")


@shopify_router.post("/market-place/shopify/webhook/app_uninstalled")
async def for_all_webhook(
    request: Request,
    x_shopify_hmac_sha256: str = Header(None),
    x_shopify_shop_domain: str = Header(None),
):
    raw_body = await request.body()

    # Debug - print the incoming headers and body length
    print("Received webhook from:", x_shopify_shop_domain)
    print("HMAC header:", x_shopify_hmac_sha256)
    print("Payload length:", len(raw_body))

    if not uninstall_verify_webhook(raw_body, x_shopify_hmac_sha256):
        print("Invalid HMAC")
        return JSONResponse(status_code=401, content={"error": "Invalid webhook"})

    print(f"App uninstalled from shop: {x_shopify_shop_domain}")
    with get_db_session() as db:
        # user_data = context_user_data.get()
        # print(user_data, "**user_data**")
        # client_id = str(user_data.client_id)
        # company_id = str(user_data.company_id)
        query = (
            db.query(Market_Place)
            .filter(Market_Place.which_market_place == x_shopify_shop_domain)
            .delete()
        )
        db.commit()

    # TODO: Mark the store as uninstalled in your DB here

    return JSONResponse(status_code=200, content={"ok": True})


# Compliance webhooks Verifies webhooks with HMAC signatures
@shopify_router.post("/compliance/webhooks")
async def compliance_webhook(
    request: Request, x_shopify_hmac_sha256: str = Header(...)
):
    # 1. Read raw body
    raw_body = await request.body()

    print(raw_body, "**raw_body**", x_shopify_hmac_sha256)

    # 2. Compute expected HMAC
    computed_hmac = base64.b64encode(
        hmac.new(SHOPIFY_API_SECRET.encode("utf-8"), raw_body, hashlib.sha256).digest()
    ).decode()
    print(computed_hmac, "computed_hmac")
    # 3. Verify HMAC
    if not hmac.compare_digest(computed_hmac, x_shopify_hmac_sha256):
        print("HMAC verification failed")
        raise HTTPException(status_code=401, detail="HMAC verification failed")
    print("HMAC PASS")
    # 4. Parse JSON and handle webhook
    data = await request.json()
    print("Webhook received:", data)

    return {"success": True}


def validate_shopify_hmac(params: dict, hmac_to_validate: str, secret: str) -> bool:
    sorted_params = {k: v for k, v in params.items() if k != "hmac"}
    sorted_items = sorted(sorted_params.items())
    query_string = "&".join([f"{k}={v}" for k, v in sorted_items])

    calculated_hmac = hmac.new(
        secret.encode("utf-8"), query_string.encode("utf-8"), hashlib.sha256
    ).hexdigest()

    return hmac.compare_digest(calculated_hmac, hmac_to_validate)


def success_verify_hmac(params, secret):
    # Convert params to dict and remove the 'hmac' key
    params = dict(params)
    hmac_received = params.pop("hmac", None)

    # Sort params lexicographically and build the message string
    message = urlencode(sorted(params.items()))

    # Create HMAC with SHA256 using secret key
    computed_hmac = hmac.new(
        secret.encode("utf-8"), message.encode("utf-8"), hashlib.sha256
    ).hexdigest()

    # Compare HMACs securely
    return hmac.compare_digest(computed_hmac, hmac_received)


@shopify_router.get("/market-place/shopify/ping")
def ping(request: Request):
    shop = request.query_params.get("shop")

    if not shop:
        raise HTTPException(status_code=400, detail="Missing 'shop' parameter")

    with get_db_session() as db:
        store = (
            db.query(Market_Place)
            .filter(Market_Place.which_market_place == shop)
            .first()
        )
        print(store, "<<store>>")
        start_time = time.time()
        start_dt = datetime.now()
        end_time = time.time()
        print("Start DateTime:", start_dt.strftime("%Y-%m-%d %H:%M:%S"))
        if store is not None:
            print(f"[{shop}] Access token found. Store is authorized.")
            # return {"success": True, "message": "Shop is authorized."}
            return "true"
        else:
            print(f"[{shop}] Access token missing. Store not authorized.")
            # return {"success": False, "message": "Shop is not authorized."}
            return "false"


@shopify_router.get("/market-place/shopify/auth")
def shopify_auth(request: Request, shop: str):
    state = generate_state()
    if not shop:
        raise HTTPException(status_code=400, detail="Missing shop param")

    with get_db_session() as db:
        store = (
            db.query(Market_Place)
            .filter(Market_Place.which_market_place == shop)
            .first()
        )
        if store:
            print("UNDER AUTH STATE NEED TO UPDATE ")
            store.oauth_state = state
        else:
            print("UNDER AUTH STATE INSERT FIRST TIME")
            store = Market_Place(which_market_place=shop, oauth_state=state)
            db.add(store)
        db.commit()
    timestamp = int(time.time())
    start_time = time.time()
    start_dt = datetime.now()
    end_time = time.time()
    print("Start auth action DateTime:", start_dt.strftime("%Y-%m-%d %H:%M:%S"))
    redirect_uri = f"{SERVER_NGROK_LINK}/api/v1/market-place/shopify/success"
    encoded_redirect_uri = quote(redirect_uri, safe="")

    install_uri = (
        f"https://{shop}/admin/oauth/authorize"
        f"?client_id={SHOPIFY_API_KEY}"
        f"&scope={SCOPES}"
        f"&redirect_uri={encoded_redirect_uri}"
        f"&state={state}"
        f"&nonce={secrets.token_urlsafe(8)}"
        f"&timestamp={timestamp}"
        f"&grant_options[]=per-user"
    )
    print("Install URL:", install_uri)
    print("REDIRECT HIT TIME", start_dt.strftime("%Y-%m-%d %H:%M:%S"))
    return RedirectResponse(install_uri)


@shopify_router.get("/market-place/shopify/success")
async def shopify_token(
    request: Request,
    shop: str,
    code: str,
    hmac: str,
    timestamp: str,
    state: str,
):
    print("App install successful into store new")

    # Validate HMAC
    if not success_verify_hmac(request.query_params, SHOPIFY_API_SECRET):
        raise HTTPException(status_code=400, detail="Invalid HMAC")

    with get_db_session() as db:
        store = (
            db.query(Market_Place)
            .filter(Market_Place.which_market_place == shop)
            .first()
        )

        if not store:
            raise HTTPException(status_code=400, detail="Shop not registered")

        print(f"Stored oauth_state: {store.oauth_state}, Incoming state: {state}")
        if store.oauth_state != state:
            print(f"Invalid state! Expected {store.oauth_state}, got {state}")
            # Redirect to login or error page
            # return RedirectResponse(url=f"https://app.lastmiles.co/login")
            return "Error !"
            # return RedirectResponse(
            # url=f"{SERVER_NGROK_LINK}/api/v1/market-place/shopify/front"
            # )

    token_response = requests.post(
        f"https://{shop}/admin/oauth/access_token",
        data={
            "client_id": SHOPIFY_API_KEY,
            "client_secret": SHOPIFY_API_SECRET,
            "code": code,
        },
    )

    if token_response.status_code != 200:
        print(token_response.text)
        raise HTTPException(status_code=400, detail="Failed to get access token")

    access_token = token_response.json().get("access_token")
    print(access_token, "<<access_token>>")

    with get_db_session() as db:
        store = (
            db.query(Market_Place)
            .filter(Market_Place.which_market_place == shop)
            .first()
        )
        if store:
            store.oauth_state = None  # clear state after validation
            store.access_token = access_token
        db.commit()

    # Register webhook
    GRAPHQL_URL = f"https://{shop}/admin/api/2025-01/graphql.json"
    headers = {
        "Content-Type": "application/json",
        "X-Shopify-Access-Token": access_token,
    }

    mutation = """
    mutation webhookSubscriptionCreate($topic: WebhookSubscriptionTopic!, $callbackUrl: URL!) {
    webhookSubscriptionCreate(
        topic: $topic
        webhookSubscription: {
        callbackUrl: $callbackUrl
        format: JSON
        }
    ) {
        userErrors {
        field
        message
        }
        webhookSubscription {
        id
        }
    }
    }
    """

    topics = [
        "ORDERS_CREATE",
        "ORDERS_UPDATED",
        "FULFILLMENTS_CREATE",
        "APP_UNINSTALLED",
    ]
    callback_base = f"{SERVER_NGROK_LINK}/api/v1/market-place/shopify/webhook"

    for topic in topics:
        variables = {
            "topic": topic,
            "callbackUrl": f"{callback_base}/{topic.lower()}",
        }

        response = requests.post(
            GRAPHQL_URL,
            headers=headers,
            json={"query": mutation, "variables": variables},
        )
        resp_json = response.json()

        if "errors" in resp_json:
            print(f"[ERROR] {topic} GraphQL error: {resp_json['errors']}")
            continue

        user_errors = resp_json["data"]["webhookSubscriptionCreate"]["userErrors"]
        if user_errors:
            print(f"[ERROR] {topic} userErrors: {user_errors}")
        else:
            sub = resp_json["data"]["webhookSubscriptionCreate"]["webhookSubscription"]
            print(f"[OK] Subscribed {topic}: {sub['id']}")

    print("Webhook subscriptions complete.")

    # Redirect to your embedded app inside Shopify admin
    host = request.query_params.get("host", "")
    host_encoded = quote_plus(host)

    embedded_app_url = (
        f"https://{shop}/admin/apps/warehousity?shop={shop}&host={host_encoded}"
    )
    print(f"Redirecting to embedded app URL: {embedded_app_url}")
    return RedirectResponse(
        f"{SERVER_NGROK_LINK}/api/v1/market-place/shopify/front?shop={shop}"
    )
    # print("i am waiting 30 second")
    # await asyncio.sleep(50)  # non-blocking sleep
    return RedirectResponse(url=embedded_app_url)


@shopify_router.post(
    "/shopify/order-create",
)
async def webhook(request: Request):
    try:
        orders = await request.json()
        client_id = request.query_params.get("client_id")
        store_id = request.query_params.get("store_id")

        if store_id in [None, "", "0", 0]:
            store_id = None
        else:
            store_id = int(store_id)

        if not client_id:
            return

        response = await Shopify.create_or_update_order(orders, client_id, store_id)
        return response

    except Exception as e:
        return build_api_response(
            GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                data=str(e),
                message="An error occurred while creating the order.",
            )
        )


@shopify_router.post(
    "/shopify/temp",
)
async def webhook():
    try:

        response = await Shopify.get_open_orders()
        return response

    except Exception as e:
        return build_api_response(
            GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                data=str(e),
                message="An error occurred while creating the order.",
            )
        )


@shopify_router.post(
    "/shopify/update_order_ids",
)
async def webhook(request: Request):
    try:

        orders = await request.json()

        response = await Shopify.update_shopify_order_id(orders=orders)
        return response

    except Exception as e:
        return build_api_response(
            GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                data=str(e),
                message="An error occurred while creating the order.",
            )
        )
