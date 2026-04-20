"""FastAPI application entry point. Phase 3 implementation — stub only."""

from fastapi import FastAPI

app = FastAPI(title="Enterprise Data Product Planner", version="0.1.0")


@app.get("/health")
def health() -> dict:
    return {"status": "ok"}
