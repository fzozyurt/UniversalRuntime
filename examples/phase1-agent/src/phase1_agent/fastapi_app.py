from fastapi import FastAPI

app = FastAPI(title="Phase 1 deterministic agent")


@app.get("/hello")
async def hello() -> dict[str, str]:
    return {"message": "phase1-agent"}
