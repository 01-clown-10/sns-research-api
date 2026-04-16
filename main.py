from fastapi import FastAPI
from sns_research import router as sns_router

app = FastAPI(title="SNS Research API")
app.include_router(sns_router)
