"""
GeoGenie Network Atlas — Databricks App backend (FastAPI).

Three data paths, two engines:
  • the MAP (features + lasso analysis) → Lakebase Postgres/PostGIS, directly (ms)
  • Genie natural-language questions     → UC-federated copy of the same tables

Routes:
  GET  /                      -> ESRI map UI (static/index.html)
  GET  /api/features/{layer}  -> viewport bbox -> GeoJSON FeatureCollection per layer
  POST /api/analyze           -> drawn polygon -> expansion ROI (uncovered demand)
  POST /api/genie             -> natural-language question (optionally H3-scoped)
  GET  /healthz

Env vars:
  PGHOST / PGPORT / PGDATABASE / PGUSER / LAKEBASE_ENDPOINT  (injected by the postgres resource)
  GENIE_SPACE_ID                                            (set in app.yaml)
"""
import json
import os

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

import lakebase
import genie

app = FastAPI(title="GeoGenie Network Atlas")


@app.get("/api/features/{layer}")
def features(layer: str, north: float, south: float, east: float, west: float):
    if layer not in lakebase.LAYER_SQL:
        raise HTTPException(404, f"Unknown layer '{layer}'")
    try:
        return lakebase.features_in_bbox(layer, north, south, east, west)
    except Exception as e:
        raise HTTPException(502, f"Lakebase query failed: {e}")


class AnalyzeRequest(BaseModel):
    geojson: dict  # a GeoJSON Polygon geometry


@app.post("/api/analyze")
def analyze(req: AnalyzeRequest):
    geom = req.geojson
    if geom.get("type") != "Polygon" or not geom.get("coordinates"):
        raise HTTPException(400, "Expected a GeoJSON Polygon geometry")

    # Simplify absurdly detailed freehand polygons before shipping to PG.
    ring = geom["coordinates"][0]
    if len(ring) > 500:
        step = len(ring) // 400 + 1
        simplified = ring[::step]
        if simplified[-1] != ring[-1]:
            simplified.append(ring[-1])
        geom = {"type": "Polygon", "coordinates": [simplified]}

    try:
        result = lakebase.analyze_expansion(json.dumps(geom))
    except Exception as e:
        raise HTTPException(502, f"Lakebase query failed: {e}")

    return result or {"roi_tier": "NO_DATA"}


class GenieRequest(BaseModel):
    prompt: str
    h3_cells: list[str] | None = None  # optional region scope from a drawn polygon
    scope_mode: str = "h3"             # "h3" (approx) or "st" (exact point-in-polygon)
    polygon_wkt: str | None = None     # drawn polygon as WKT, used when scope_mode="st"


@app.post("/api/genie")
def ask_genie(req: GenieRequest):
    space_id = os.environ.get("GENIE_SPACE_ID")
    if not space_id:
        raise HTTPException(500, "GENIE_SPACE_ID is not configured")
    if not req.prompt.strip():
        raise HTTPException(400, "Empty prompt")
    try:
        answer, sql, table, chart, is_map = genie.send_genie_query(
            space_id, req.prompt, req.h3_cells,
            scope_mode=req.scope_mode, polygon_wkt=req.polygon_wkt,
        )
    except Exception as e:
        raise HTTPException(502, f"Genie query failed: {e}")
    return {"answer": answer, "sql": sql, "table": table, "chart": chart,
            "is_map": is_map}


@app.get("/healthz")
def healthz():
    return {"ok": True, "lakebase": lakebase.ping()}


app.mount("/static", StaticFiles(directory="static"), name="static")


@app.get("/")
def index():
    return FileResponse("static/index.html")


if __name__ == "__main__":
    import uvicorn

    # Databricks Apps assigns the port via DATABRICKS_APP_PORT; 8000 is a
    # local-dev fallback. Bind to 0.0.0.0 so the platform proxy can reach it.
    port = int(os.environ.get("DATABRICKS_APP_PORT", "8000"))
    uvicorn.run(app, host="0.0.0.0", port=port)
