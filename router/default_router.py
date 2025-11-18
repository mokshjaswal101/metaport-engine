from fastapi import APIRouter

# Create a default router for api landing page
DefaultRouter = APIRouter()


@DefaultRouter.get("/")
async def helloWorld():
    return "Welcome to the Navigator Service"
