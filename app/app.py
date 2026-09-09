"""DR Reconciliation operator console — FastAPI entrypoint.

Serves the React SPA plus JSON APIs for monitoring, operator actions, and
Ask-Genie, all against the dr_recon.control tables via the app service principal.
"""
import os

from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

from server import queries
from server.routes import actions, genie_routes, recon


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Ensure the acknowledgement table exists so findings/exports never 500.
    try:
        queries.ensure_ack_table()
    except Exception as e:  # non-fatal; acks simply unavailable until table exists
        print(f"ensure_ack_table failed at startup: {e}")
    yield


app = FastAPI(title="DR Reconciliation Console", lifespan=lifespan)

app.include_router(recon.router, prefix="/api")
app.include_router(actions.router, prefix="/api")
app.include_router(genie_routes.router, prefix="/api")


@app.get("/api/health")
def health():
    return {"status": "healthy"}


# ---- Static frontend ------------------------------------------------------
_DIST = os.path.join(os.path.dirname(__file__), "frontend", "dist")

if os.path.isdir(_DIST):
    _ASSETS = os.path.join(_DIST, "assets")
    if os.path.isdir(_ASSETS):
        app.mount("/assets", StaticFiles(directory=_ASSETS), name="assets")

    @app.get("/")
    def index():
        return FileResponse(os.path.join(_DIST, "index.html"))

    @app.get("/{full_path:path}")
    def spa(full_path: str):
        if full_path.startswith("api/"):
            return JSONResponse({"detail": "Not found"}, status_code=404)
        candidate = os.path.join(_DIST, full_path)
        if os.path.isfile(candidate):
            return FileResponse(candidate)
        return FileResponse(os.path.join(_DIST, "index.html"))
