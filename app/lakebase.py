"""Lakebase (managed Postgres + PostGIS) access layer — the map's runtime data path.

Lakebase is the SINGLE SOURCE OF TRUTH for the network data. The same tables are
ALSO exposed read-only to Unity Catalog via Lakehouse Federation so Genie can
query them through a SQL warehouse (see the provisioning notebook). This module
is only the *map* path: fast viewport (bbox) queries and the lasso analysis,
answered directly by PostGIS.

Databricks Apps with a `postgres` resource get PGHOST/PGPORT/PGDATABASE/PGUSER
and LAKEBASE_ENDPOINT env vars. Auth uses the app service principal's OAuth
token as the Postgres password; tokens are short-lived, so we refresh
proactively. Connections are per-request (OAuth passwords rotate ~hourly, which
makes static pools awkward; intra-workspace connect latency is negligible).
"""
import json
import os
import time

import psycopg
from databricks.sdk import WorkspaceClient

_TOKEN = {"value": None, "exp": 0.0}


def _password() -> str:
    now = time.time()
    if _TOKEN["value"] and now < _TOKEN["exp"] - 120:
        return _TOKEN["value"]
    w = WorkspaceClient()
    endpoint = os.getenv("LAKEBASE_ENDPOINT")
    if endpoint:
        # Deployed: mint a credential scoped to this Lakebase endpoint.
        token = w.postgres.generate_database_credential(endpoint=endpoint).token
    else:
        # Local dev fallback: a plain workspace OAuth token as the PG password.
        token = w.config.oauth_token().access_token
    _TOKEN["value"] = token
    _TOKEN["exp"] = now + 3000  # ~1h tokens; refresh early
    return _TOKEN["value"]


def _conn():
    return psycopg.connect(
        host=os.environ["PGHOST"],
        dbname=os.getenv("PGDATABASE", "databricks_postgres"),
        user=os.environ["PGUSER"],
        password=_password(),
        port=int(os.getenv("PGPORT", "5432")),
        sslmode="require",
        connect_timeout=5,
    )


# One allowlisted query per asset layer. The {layer} path segment is validated
# against this dict before anything runs, so the path can only ever select a
# pre-written query — never reach raw SQL. The bbox filter uses the GiST index
# via the && operator; geometry is returned as GeoJSON text for direct ESRI
# FeatureLayer ingestion. `props` names the attribute columns to expose.
LAYER_SQL = {
    "antennas": {
        "sql": """
            SELECT ST_AsGeoJSON(geom) AS geometry, ST_AsGeoJSON(sector_geom) AS sector,
                   sys_id, structure_sys_id, radio_band, carrier_number,
                   azimuth, h_beamwidth, mech_tilt, elec_tilt
            FROM public.antennas
            WHERE geom && ST_MakeEnvelope(%(west)s, %(south)s, %(east)s, %(north)s, 4326)
            LIMIT 5000
        """,
        "props": ["sector", "sys_id", "structure_sys_id", "radio_band",
                  "carrier_number", "azimuth", "h_beamwidth", "mech_tilt", "elec_tilt"],
    },
    "cables": {
        "sql": """
            SELECT ST_AsGeoJSON(geom) AS geometry,
                   cable_id, cable_type, fiber_count, length_m
            FROM public.cables
            WHERE geom && ST_MakeEnvelope(%(west)s, %(south)s, %(east)s, %(north)s, 4326)
            LIMIT 5000
        """,
        "props": ["cable_id", "cable_type", "fiber_count", "length_m"],
    },
    "nodes": {
        "sql": """
            SELECT ST_AsGeoJSON(geom) AS geometry,
                   node_id, node_type, status
            FROM public.nodes
            WHERE geom && ST_MakeEnvelope(%(west)s, %(south)s, %(east)s, %(north)s, 4326)
            LIMIT 5000
        """,
        "props": ["node_id", "node_type", "status"],
    },
    "sites": {
        "sql": """
            SELECT ST_AsGeoJSON(geom) AS geometry,
                   structure_sys_id, site_type, height_m
            FROM public.sites
            WHERE geom && ST_MakeEnvelope(%(west)s, %(south)s, %(east)s, %(north)s, 4326)
            LIMIT 5000
        """,
        "props": ["structure_sys_id", "site_type", "height_m"],
    },
    "demand": {
        # H3-aggregated market demand, one point per hex centroid. Weighted by
        # homes_passed for the map's heatmap renderer.
        "sql": """
            SELECT ST_AsGeoJSON(geom) AS geometry,
                   h3_cell, homes_passed, biz_count, avg_arpu, est_build_cost_usd
            FROM public.demand_h3
            WHERE geom && ST_MakeEnvelope(%(west)s, %(south)s, %(east)s, %(north)s, 4326)
            LIMIT 20000
        """,
        "props": ["h3_cell", "homes_passed", "biz_count", "avg_arpu", "est_build_cost_usd"],
    },
}


def features_in_bbox(layer: str, north: float, south: float,
                     east: float, west: float) -> dict:
    """Return a GeoJSON FeatureCollection for one asset layer within the bbox.

    Raises KeyError if `layer` is not an allowlisted layer (caller maps to 404).
    """
    spec = LAYER_SQL[layer]  # KeyError → 404 at the route
    params = {"north": north, "south": south, "east": east, "west": west}
    with _conn() as conn, conn.cursor() as cur:
        cur.execute(spec["sql"], params)
        cols = [d.name for d in cur.description]
        features = []
        for row in cur.fetchall():
            rec = dict(zip(cols, row))
            geometry = json.loads(rec.pop("geometry"))
            props = {}
            for p in spec["props"]:
                v = rec.get(p)
                # `sector` is GeoJSON text → parse to an object for the frontend
                if p == "sector" and v is not None:
                    v = json.loads(v)
                props[p] = v
            features.append({"type": "Feature", "geometry": geometry, "properties": props})
    return {"type": "FeatureCollection", "features": features}


def analyze_expansion(polygon_geojson: str) -> dict | None:
    """Run the in-database expansion-ROI model on a drawn polygon."""
    with _conn() as conn, conn.cursor() as cur:
        cur.execute("SELECT * FROM public.analyze_expansion_roi(%s)", (polygon_geojson,))
        row = cur.fetchone()
        if row is None:
            return None
        cols = [d.name for d in cur.description]
        out = dict(zip(cols, row))
        return {k: (float(v) if type(v).__name__ == "Decimal" else v) for k, v in out.items()}


def ping() -> bool:
    try:
        with _conn() as conn, conn.cursor() as cur:
            cur.execute("SELECT 1")
            return cur.fetchone()[0] == 1
    except Exception:
        return False
