# Databricks notebook source
# MAGIC %md
# MAGIC # Network Atlas — Provision & Setup
# MAGIC
# MAGIC One notebook to stand up everything the app needs. **Idempotent** — safe to re-run.
# MAGIC
# MAGIC **Architecture this builds:**
# MAGIC ```
# MAGIC   Lakebase Postgres + PostGIS   ← SINGLE SOURCE OF TRUTH (synthetic network)
# MAGIC        │                                   │
# MAGIC   the MAP queries directly            registered read-only into Unity Catalog
# MAGIC   (FastAPI /api/features, /analyze)   via Lakehouse Federation → Genie queries it
# MAGIC ```
# MAGIC
# MAGIC **Steps**
# MAGIC 0. Parameters
# MAGIC 1. Create the Lakebase (Postgres) instance
# MAGIC 2. Connect, enable PostGIS + H3-friendly schema
# MAGIC 3. Generate the synthetic Dallas network **into Postgres** (sites, antennas+sectors, cables, nodes, demand_h3)
# MAGIC 4. Install the `analyze_expansion_roi()` PostGIS function
# MAGIC 5. Register Lakebase into Unity Catalog as a read-only foreign catalog (for Genie)
# MAGIC 6. Print next steps (Genie Space + app deploy)
# MAGIC
# MAGIC > **Data is 100% synthetic**, generated deterministically (numpy seed 42). There is no
# MAGIC > real network, subscriber, or revenue data for any actual carrier.

# COMMAND ----------

# MAGIC %md ## Install dependencies

# COMMAND ----------

# On the Databricks corporate network, PyPI is proxied. If a plain pip install
# fails, use: %pip install --index-url https://pypi-proxy.cloud.databricks.com/simple ...
%pip install "psycopg[binary]==3.2.3" "h3==4.1.0" "databricks-sdk>=0.118.0" numpy --quiet
dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md ## 0 · Parameters

# COMMAND ----------

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

# ── Basic text widgets ────────────────────────────────────────────────────────
dbutils.widgets.text("lakebase_instance", "geogenie-network-atlas-db", "Lakebase instance name")
dbutils.widgets.text("federated_catalog", "geogenie_network_atlas",    "UC catalog name for federation (Genie)")
dbutils.widgets.text("capacity",          "CU_1",                 "Lakebase capacity")
dbutils.widgets.text("seed",              "42",                   "Random seed")

# ── Warehouse dropdown (serverless first, then running, then others) ──────────
_warehouses = sorted(
    list(w.warehouses.list()),
    key=lambda wh: (
        not getattr(wh, "enable_serverless_compute", False),   # serverless first
        getattr(wh.state, "value", "Z") != "RUNNING",          # running second
        wh.name or "",                                          # then alphabetical
    ),
)

if not _warehouses:
    raise RuntimeError("No SQL warehouses found. Please create one in the Databricks UI first.")

def _wh_label(wh):
    prefix    = "⚡" if getattr(wh, "enable_serverless_compute", False) else "🏢"
    state_val = getattr(wh.state, "value", "") if wh.state else ""
    state_tag = f" [{state_val}]" if state_val else ""
    return f"{prefix} {wh.name}{state_tag}  |  {wh.id}"

_wh_choices = [_wh_label(wh) for wh in _warehouses]
_wh_default = _wh_choices[0]

dbutils.widgets.dropdown("warehouse", _wh_default, _wh_choices, "SQL Warehouse")

# ── Read all widget values ────────────────────────────────────────────────────
LAKEBASE_INSTANCE = dbutils.widgets.get("lakebase_instance")
FEDERATED_CATALOG = dbutils.widgets.get("federated_catalog")
CAPACITY          = dbutils.widgets.get("capacity")
SEED              = int(dbutils.widgets.get("seed"))
WAREHOUSE_ID      = dbutils.widgets.get("warehouse").split("  |  ")[-1].strip()

PG_DATABASE = "databricks_postgres"
PG_SCHEMA   = "public"

# ── Summary ───────────────────────────────────────────────────────────────────
print(f"Lakebase instance : {LAKEBASE_INSTANCE}")
print(f"UC catalog (Genie): {FEDERATED_CATALOG}")
print(f"SQL Warehouse     : {dbutils.widgets.get('warehouse')}")
print(f"Warehouse ID      : {WAREHOUSE_ID}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 · Create the Lakebase instance
# MAGIC
# MAGIC Creates a managed Postgres instance if one by this name doesn't already exist,
# MAGIC then waits for it to become available. (API surface: `w.database`.)

# COMMAND ----------

import time
import itertools
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.postgres import Project, ProjectSpec

w = WorkspaceClient()

PROJECT_ID = LAKEBASE_INSTANCE  # reuse widget value as the Autoscaling project id

def get_project(pid):
    """Return project if it exists AND is usable (not soft-deleted)."""
    try:
        proj = w.postgres.get_project(name=f"projects/{pid}")
        # Soft-deleted projects still appear via get_project during the retention window
        if proj.delete_time is not None:
            return None
        return proj
    except Exception:
        return None

project = get_project(PROJECT_ID)
if project is None:
    # Project is either soft-deleted (slug reserved) or truly non-existent.
    # Try undelete first, then create.
    try:
        probe = w.postgres.get_project(name=f"projects/{PROJECT_ID}")
        if probe.delete_time is not None:
            # Soft-deleted — restore it
            print(f"Project '{PROJECT_ID}' is soft-deleted — restoring...")
            w.postgres.undelete_project(name=f"projects/{PROJECT_ID}")
            project = probe
        else:
            # get_project returned None (delete_time race) but probe shows it's live
            project = probe
            print(f"Project '{PROJECT_ID}' already exists — reusing.")
    except Exception as e:
        err_msg = str(e).lower()
        if "not found" in err_msg:
            pass  # project truly doesn't exist — fall through to create
        elif "not deleted" in err_msg or "already exists" in err_msg:
            # Project recovered between calls — just use it
            project = w.postgres.get_project(name=f"projects/{PROJECT_ID}")
            print(f"Project '{PROJECT_ID}' already exists — reusing.")
        else:
            raise  # unexpected error — surface it

    if project is None:
        print(f"Creating Lakebase Autoscaling project '{PROJECT_ID}' ...")
        op = w.postgres.create_project(
            project=Project(spec=ProjectSpec(display_name=PROJECT_ID, pg_version=17)),
            project_id=PROJECT_ID,
        )
        project = op.wait()
        print(f"Project created: {project.name}")
else:
    print(f"Project '{PROJECT_ID}' already exists — reusing.")

# The project auto-creates a 'production' branch + 'primary' read-write endpoint.
BRANCH_NAME = f"projects/{PROJECT_ID}/branches/production"
ENDPOINT_NAME = f"{BRANCH_NAME}/endpoints/primary"

# Wait for endpoint host to be available
PG_HOST = None
for _ in range(30):
    try:
        ep = w.postgres.get_endpoint(name=ENDPOINT_NAME)
        if ep.status and ep.status.hosts:
            PG_HOST = ep.status.hosts.host
            break
    except Exception:
        pass
    time.sleep(5)

print(f"\nLakebase host: {PG_HOST}")
print(f"Branch: {BRANCH_NAME}")
print(f"Endpoint: {ENDPOINT_NAME}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 · Connect to Postgres + create schema
# MAGIC
# MAGIC Auth: the current user mints a short-lived database credential (OAuth token used
# MAGIC as the Postgres password). PostGIS is enabled and the five tables are (re)created
# MAGIC with both a `geom` geometry column (for the map) and an `h3_cell` text column
# MAGIC (for Genie's H3 scoping — geometry does not federate to a usable UC type).

# COMMAND ----------

import psycopg

def pg_password():
    """Mint a short-lived Lakebase OAuth token (valid 1 hour)."""
    cred = w.postgres.generate_database_credential(endpoint=ENDPOINT_NAME)
    return cred.token

PG_USER = w.current_user.me().user_name

def connect():
    return psycopg.connect(
        host=PG_HOST, dbname=PG_DATABASE, user=PG_USER,
        password=pg_password(), port=5432, sslmode="require", autocommit=True,
    )

DDL = """
CREATE EXTENSION IF NOT EXISTS postgis;

DROP TABLE IF EXISTS public.sites CASCADE;
CREATE TABLE public.sites (
  structure_sys_id text PRIMARY KEY,
  site_type text, height_m double precision,
  lat double precision, lon double precision,
  h3_cell text,
  geom geometry(Point, 4326)
);

DROP TABLE IF EXISTS public.antennas CASCADE;
CREATE TABLE public.antennas (
  sys_id text PRIMARY KEY,
  structure_sys_id text,
  radio_band text, carrier_number int,
  azimuth double precision, h_beamwidth double precision,
  mech_tilt double precision, elec_tilt double precision,
  lat double precision, lon double precision,
  h3_cell text,
  geom geometry(Point, 4326),
  sector_geom geometry(Polygon, 4326)
);

DROP TABLE IF EXISTS public.cables CASCADE;
CREATE TABLE public.cables (
  cable_id text PRIMARY KEY,
  cable_type text, fiber_count int, length_m double precision,
  geom geometry(LineString, 4326)
);

DROP TABLE IF EXISTS public.nodes CASCADE;
CREATE TABLE public.nodes (
  node_id text PRIMARY KEY,
  node_type text, status text,
  lat double precision, lon double precision,
  h3_cell text,
  geom geometry(Point, 4326)
);

DROP TABLE IF EXISTS public.demand_h3 CASCADE;
CREATE TABLE public.demand_h3 (
  h3_cell text PRIMARY KEY,
  homes_passed int, biz_count int, avg_arpu double precision,
  est_build_cost_usd double precision,
  cell_lat double precision, cell_lon double precision,
  geom geometry(Point, 4326)
);
"""

with connect() as conn, conn.cursor() as cur:
    cur.execute(DDL)
print("PostGIS enabled and tables created.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 · Generate the synthetic Dallas network (into Postgres)
# MAGIC
# MAGIC Deterministic numpy generator (seed 42). Produces the same asset types shown in
# MAGIC the customer's Network Atlas: sites, antennas with computed coverage sectors,
# MAGIC cables (trunk/distribution), equipment nodes, and demand aggregated to H3 res-9.

# COMMAND ----------

import numpy as np
import h3
from psycopg.rows import tuple_row

rng = np.random.default_rng(SEED)

# Dallas, TX — a large, flat metro with a dense, regular street grid.
CENTER_LAT, CENTER_LON = 32.78, -96.80
COSLAT = np.cos(np.radians(CENTER_LAT))
BANDS = ["600MHz", "700MHz", "AWS", "PCS", "C-Band"]
BAND_RADIUS_M = {"600MHz": 850, "700MHz": 780, "AWS": 680, "PCS": 640, "C-Band": 560}

def h3_of(lat, lon):
    return h3.latlng_to_cell(lat, lon, 9)

# ---- sites: ~200 structures clustered on 6 corridors ----
# corridor_centers is reused below so DEMAND is placed on the same corridors as the
# network — otherwise demand and antennas wouldn't spatially coincide and lassoing a
# populated area would find no demand to score.
sites = []
corridor_centers = []
n = 0
# CORRIDOR_R controls how far the 6 corridors spread from the metro center.
# ~0.045deg keeps the whole network within a ~15 km area so it frames at a
# readable zoom instead of being scattered across the metro with empty gaps.
CORRIDOR_R = 0.045
for cid in range(6):
    c_lat = CENTER_LAT + CORRIDOR_R * np.sin(rng.random() * 2 * np.pi)
    c_lon = CENTER_LON + CORRIDOR_R * np.cos(rng.random() * 2 * np.pi) / COSLAT
    corridor_centers.append((c_lat, c_lon))
    for _ in range(34):
        lat = c_lat + (rng.random() - 0.5) * 0.05
        lon = c_lon + (rng.random() - 0.5) * 0.055
        is_tower = rng.random() < 0.4
        height = round((15 + rng.random() * 45) if is_tower else (8 + rng.random() * 22), 1)
        sid = f"STR-{n:05d}"
        sites.append((sid, "tower" if is_tower else "rooftop", height,
                      round(lat, 6), round(lon, 6), h3_of(lat, lon)))
        n += 1

# ---- antennas: 1-7 per site, each with a pie-slice coverage sector ----
def sector_wkt(lat, lon, azimuth, beamwidth, radius_m):
    pts = [f"{lon:.6f} {lat:.6f}"]
    for i in range(13):
        ang = np.radians(azimuth - beamwidth / 2 + beamwidth * i / 12)
        dlat = np.cos(ang) * radius_m / 111320.0
        dlon = np.sin(ang) * radius_m / (111320.0 * COSLAT)
        pts.append(f"{lon + dlon:.6f} {lat + dlat:.6f}")
    pts.append(f"{lon:.6f} {lat:.6f}")
    return f"POLYGON(({','.join(pts)}))"

antennas = []
aid = 0
for sid, _st, _h, lat, lon, _hc in sites:
    n_ant = int(rng.integers(1, 8))
    base_az = rng.integers(0, 360)
    for k in range(n_ant):
        band = BANDS[int(rng.integers(0, 5))]
        az = round((base_az + k * (360.0 / n_ant) + (rng.random() - 0.5) * 16) % 360, 1)
        bw = float([33, 45, 60, 65, 90][int(rng.integers(0, 5))])
        antennas.append((
            f"Antenna-{aid:05d}", sid, band, int(rng.integers(1, 40)),
            az, bw, round(rng.random() * 6, 1), round(rng.random() * 8, 1),
            round(lat, 6), round(lon, 6), h3_of(lat, lon),
            sector_wkt(lat, lon, az, bw, BAND_RADIUS_M[band]),
        ))
        aid += 1

# ---- demand_h3: premises aggregated to H3 res-9 ----
# Demand blobs are anchored to the SAME corridors as the network (plus a few
# standalone "greenfield" pockets), so a lasso over a populated area reliably finds
# demand to score. Blobs are offset from each corridor center by up to ~0.04deg — so
# some demand sits under existing antenna sectors (→ covered) and some spills just
# outside them (→ uncovered, the real expansion opportunity that drives BUILD/PASS).
demand_centers = []
for (c_lat, c_lon) in corridor_centers:
    # 3 demand pockets around each corridor, offset from the network core
    for _ in range(3):
        d_lat = c_lat + (rng.random() - 0.5) * 0.05
        d_lon = c_lon + (rng.random() - 0.5) * 0.055
        demand_centers.append((d_lat, d_lon))
# a few greenfield pockets just beyond the network (unserved demand → strong ROI)
for _ in range(4):
    ang = rng.random() * 2 * np.pi
    r = CORRIDOR_R + 0.02 + rng.random() * 0.03
    demand_centers.append((CENTER_LAT + r * np.sin(ang),
                           CENTER_LON + r * np.cos(ang) / COSLAT))

cell_acc = {}
for (b_lat, b_lon) in demand_centers:
    intensity = 0.7 + rng.random() * 0.6  # relative ARPU/density of this pocket
    for _ in range(1500):
        lat = b_lat + (rng.random() - 0.5) * 0.03
        lon = b_lon + (rng.random() - 0.5) * 0.033
        is_biz = 1 if rng.random() < 0.15 else 0
        arpu = (140.0 if is_biz else 75.0) * (0.7 + 0.5 * intensity)
        cell = h3_of(lat, lon)
        acc = cell_acc.setdefault(cell, [0, 0, 0.0])
        acc[0] += 1
        acc[1] += is_biz
        acc[2] += arpu

demand = []
for cell, (homes, biz, arpu_sum) in cell_acc.items():
    clat, clon = h3.cell_to_latlng(cell)
    avg_arpu = round(arpu_sum / homes, 2)
    build_cost = max(1500.0, round(3000 + 55 * homes, 0))
    demand.append((cell, homes, biz, avg_arpu, build_cost,
                   round(clat, 6), round(clon, 6)))

# ---- cables: trunk (nearest-neighbor) + a distribution spur off each midpoint ----
pts = np.array([[s[3], s[4]] for s in sites])  # (lat, lon)
cables = []
for i in range(len(pts)):
    d = np.sqrt((pts[:, 0] - pts[i, 0]) ** 2 + ((pts[:, 1] - pts[i, 1]) * COSLAT) ** 2)
    d[i] = np.inf
    j = int(np.argmin(d))
    (alat, alon), (blat, blon) = pts[i], pts[j]
    length_m = round(float(d[j]) * 111320.0, 1)
    cables.append((f"CBL-T-{i:05d}", "trunk",
                   int([48, 96, 144, 288][int(rng.integers(0, 4))]), length_m,
                   f"LINESTRING({alon:.6f} {alat:.6f},{blon:.6f} {blat:.6f})"))
    mlat, mlon = (alat + blat) / 2, (alon + blon) / 2
    slat = mlat + (rng.random() - 0.5) * 0.008
    slon = mlon + (rng.random() - 0.5) * 0.008
    cables.append((f"CBL-D-{i:05d}", "distribution",
                   int([12, 24, 48][int(rng.integers(0, 3))]), 400.0,
                   f"LINESTRING({mlon:.6f} {mlat:.6f},{slon:.6f} {slat:.6f})"))

# ---- nodes: 1-3 equipment points along each cable ----
NODE_TYPES = ["cabinet", "splitter", "amplifier", "power_supply"]
nodes = []
nid = 0
for cid_str, _ct, _fc, _lm, wkt in cables:
    coords = wkt.replace("LINESTRING(", "").replace(")", "").split(",")
    (x0, y0) = map(float, coords[0].split())
    (x1, y1) = map(float, coords[1].split())
    for _ in range(int(rng.integers(1, 4))):
        t = rng.random()
        lon = x0 + (x1 - x0) * t
        lat = y0 + (y1 - y0) * t
        status = "fault" if rng.random() < 0.08 else "active"
        nodes.append((f"ND-{nid:05d}", NODE_TYPES[int(rng.integers(0, 4))], status,
                      round(lat, 6), round(lon, 6), h3_of(lat, lon)))
        nid += 1

print(f"Generated: {len(sites)} sites, {len(antennas)} antennas, "
      f"{len(demand)} demand hexes, {len(cables)} cables, {len(nodes)} nodes")

# COMMAND ----------

# MAGIC %md ### Insert into Postgres (geometry built with PostGIS ST_ functions)

# COMMAND ----------

from psycopg import sql

with connect() as conn, conn.cursor() as cur:
    cur.executemany(
        "INSERT INTO public.sites (structure_sys_id, site_type, height_m, lat, lon, h3_cell, geom) "
        "VALUES (%s,%s,%s,%s,%s,%s, ST_SetSRID(ST_MakePoint(%s,%s),4326))",
        [(s[0], s[1], s[2], s[3], s[4], s[5], s[4], s[3]) for s in sites],
    )
    cur.executemany(
        "INSERT INTO public.antennas (sys_id, structure_sys_id, radio_band, carrier_number, "
        "azimuth, h_beamwidth, mech_tilt, elec_tilt, lat, lon, h3_cell, geom, sector_geom) "
        "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s, ST_SetSRID(ST_MakePoint(%s,%s),4326), "
        "ST_SetSRID(ST_GeomFromText(%s),4326))",
        [(a[0], a[1], a[2], a[3], a[4], a[5], a[6], a[7], a[8], a[9], a[10],
          a[9], a[8], a[11]) for a in antennas],
    )
    cur.executemany(
        "INSERT INTO public.demand_h3 (h3_cell, homes_passed, biz_count, avg_arpu, "
        "est_build_cost_usd, cell_lat, cell_lon, geom) "
        "VALUES (%s,%s,%s,%s,%s,%s,%s, ST_SetSRID(ST_MakePoint(%s,%s),4326))",
        [(d[0], d[1], d[2], d[3], d[4], d[5], d[6], d[6], d[5]) for d in demand],
    )
    cur.executemany(
        "INSERT INTO public.cables (cable_id, cable_type, fiber_count, length_m, geom) "
        "VALUES (%s,%s,%s,%s, ST_SetSRID(ST_GeomFromText(%s),4326))",
        cables,
    )
    cur.executemany(
        "INSERT INTO public.nodes (node_id, node_type, status, lat, lon, h3_cell, geom) "
        "VALUES (%s,%s,%s,%s,%s,%s, ST_SetSRID(ST_MakePoint(%s,%s),4326))",
        [(nd[0], nd[1], nd[2], nd[3], nd[4], nd[5], nd[4], nd[3]) for nd in nodes],
    )

    # Spatial indexes for the map's bbox queries, btree for the ROI bbox prefilter.
    cur.execute("CREATE INDEX IF NOT EXISTS sites_gix    ON public.sites    USING GIST (geom)")
    cur.execute("CREATE INDEX IF NOT EXISTS ant_gix      ON public.antennas USING GIST (geom)")
    cur.execute("CREATE INDEX IF NOT EXISTS ant_sect_gix ON public.antennas USING GIST (sector_geom)")
    cur.execute("CREATE INDEX IF NOT EXISTS cables_gix   ON public.cables   USING GIST (geom)")
    cur.execute("CREATE INDEX IF NOT EXISTS nodes_gix    ON public.nodes    USING GIST (geom)")
    cur.execute("CREATE INDEX IF NOT EXISTS demand_gix   ON public.demand_h3 USING GIST (geom)")
    cur.execute("CREATE INDEX IF NOT EXISTS demand_ll    ON public.demand_h3 (cell_lat, cell_lon)")

print("Inserted all rows and built indexes.")

# COMMAND ----------

# MAGIC %md ## 4 · Install the `analyze_expansion_roi()` PostGIS function

# COMMAND ----------

# Mirrors sql/functions_postgis.sql — kept inline so the notebook is self-contained.
ANALYZE_FN = r"""
CREATE OR REPLACE FUNCTION public.analyze_expansion_roi(polygon_geojson text)
RETURNS TABLE (
  total_hexes integer, covered_hexes integer, uncovered_hexes integer,
  homes_passed_uncovered bigint, biz_count bigint, avg_arpu numeric,
  projected_take_rate numeric, projected_subs bigint, total_capex_usd numeric,
  annual_revenue_usd numeric, five_yr_revenue_usd numeric,
  five_yr_roi_pct numeric, payback_months numeric, roi_tier text
)
LANGUAGE sql STABLE AS
$$
WITH poly AS (SELECT ST_SetSRID(ST_GeomFromGeoJSON(polygon_geojson), 4326) AS g),
scoped AS (
  SELECT d.* FROM public.demand_h3 d, poly p
  WHERE d.cell_lat BETWEEN ST_YMin(p.g) AND ST_YMax(p.g)
    AND d.cell_lon BETWEEN ST_XMin(p.g) AND ST_XMax(p.g)
    AND ST_Contains(p.g, d.geom)
),
flagged AS (
  SELECT s.*, EXISTS (
    SELECT 1 FROM public.antennas a
    WHERE a.sector_geom && s.geom AND ST_Intersects(a.sector_geom, s.geom)
  ) AS covered FROM scoped s
),
agg AS (
  SELECT COUNT(*)::int AS total_hexes,
    COUNT(*) FILTER (WHERE covered)::int AS covered_hexes,
    COUNT(*) FILTER (WHERE NOT covered)::int AS uncovered_hexes,
    COALESCE(SUM(homes_passed) FILTER (WHERE NOT covered),0) AS homes_unc,
    COALESCE(SUM(biz_count) FILTER (WHERE NOT covered),0) AS biz_unc,
    AVG(avg_arpu) FILTER (WHERE NOT covered) AS arpu_unc,
    COALESCE(SUM(est_build_cost_usd) FILTER (WHERE NOT covered),0) AS build_unc
  FROM flagged
),
model AS (SELECT *, LEAST(0.55, GREATEST(0.12, 0.38 + 0.06*(COALESCE(arpu_unc,80)/100.0 - 0.8))) AS take_rate FROM agg),
calc AS (SELECT *, ROUND(homes_unc*take_rate)::bigint AS subs FROM model),
fin AS (SELECT *, build_unc + subs*650 AS total_capex, subs*COALESCE(arpu_unc,80)*12 AS annual_rev,
        subs*COALESCE(arpu_unc,80)*12*5*0.62 AS five_yr_margin FROM calc)
SELECT total_hexes, covered_hexes, uncovered_hexes, homes_unc, biz_unc,
  ROUND(COALESCE(arpu_unc,0)::numeric,2), ROUND(take_rate::numeric,3), subs,
  ROUND(total_capex::numeric,0), ROUND(annual_rev::numeric,0), ROUND(five_yr_margin::numeric,0),
  CASE WHEN total_capex>0 THEN ROUND((100.0*(five_yr_margin-total_capex)/total_capex)::numeric,1) ELSE 0 END,
  CASE WHEN annual_rev*0.62>0 THEN ROUND((total_capex/(annual_rev*0.62/12.0))::numeric,1) ELSE NULL END,
  CASE WHEN homes_unc=0 THEN 'NO_DATA'
       WHEN total_capex>0 AND five_yr_margin/total_capex>=2.0 THEN 'BUILD'
       WHEN total_capex>0 AND five_yr_margin/total_capex>=1.2 THEN 'MARGINAL'
       ELSE 'PASS' END
FROM fin;
$$;
"""

with connect() as conn, conn.cursor() as cur:
    cur.execute(ANALYZE_FN)
    cur.execute(
        "SELECT * FROM public.analyze_expansion_roi(%s)",
        ('{"type":"Polygon","coordinates":[[[-96.90,32.70],[-96.70,32.70],'
         '[-96.70,32.86],[-96.90,32.86],[-96.90,32.70]]]}',),
    )
    print("Smoke test row:", cur.fetchone())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5 · Register Lakebase into Unity Catalog (read-only) for Genie
# MAGIC
# MAGIC Genie can't reach Postgres directly — it queries UC tables via a SQL warehouse.
# MAGIC Registering the Lakebase database as a UC catalog exposes `public.*` as UC tables.
# MAGIC The `h3_cell` and plain attribute columns federate cleanly; the PostGIS `geom`
# MAGIC columns come across as unsupported (that's fine — Genie scopes by H3, not geometry).
# MAGIC
# MAGIC > Native Lakebase→UC registration is a recent feature. If `create_database_catalog`
# MAGIC > isn't available in your workspace, fall back to a standard **PostgreSQL Lakehouse
# MAGIC > Federation** connection + foreign catalog (see SETUP.md).

# COMMAND ----------

from databricks.sdk.service.postgres import Catalog, CatalogCatalogSpec

try:
    catalog = w.postgres.create_catalog(
        catalog=Catalog(spec=CatalogCatalogSpec(
            postgres_database=PG_DATABASE,
            branch=BRANCH_NAME,
        )),
        catalog_id=FEDERATED_CATALOG,
    ).wait()
    print(f"Registered Lakebase as UC catalog '{FEDERATED_CATALOG}'.")
except Exception as e:
    # If it already exists or the call fails, handle gracefully
    if "ALREADY_EXISTS" in str(e) or "already" in str(e).lower():
        print(f"UC catalog '{FEDERATED_CATALOG}' already registered.")
    else:
        print(f"Native registration failed: {e}")
        print("\n→ Fallback: use Catalog Explorer → External Data → Connections → PostgreSQL")
        print("  to create a Lakehouse Federation foreign catalog (see SETUP.md).")
        raise

print(f"\nGenie should point at tables under: {FEDERATED_CATALOG}.{PG_SCHEMA}.*")
print("  e.g. antennas, sites, cables, nodes, demand_h3")

# COMMAND ----------

# MAGIC %sql
# MAGIC REFRESH FOREIGN SCHEMA geogenie_network_atlas.public;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6 · Create Genie Space
# MAGIC
# MAGIC Creates a Genie Space over the federated tables with full schema documentation,
# MAGIC terminology, and geographic scoping instructions (H3 + point-in-polygon).

# COMMAND ----------

# DBTITLE 1,Create Genie Space
import json, uuid

# Tables exposed via the federated UC catalog (must be sorted for the API)
tables = sorted([
    f"{FEDERATED_CATALOG}.{PG_SCHEMA}.sites",
    f"{FEDERATED_CATALOG}.{PG_SCHEMA}.antennas",
    f"{FEDERATED_CATALOG}.{PG_SCHEMA}.cables",
    f"{FEDERATED_CATALOG}.{PG_SCHEMA}.nodes",
    f"{FEDERATED_CATALOG}.{PG_SCHEMA}.demand_h3",
])

instruction_text = (
    "This Space answers questions about a synthetic wireless network for a regional carrier around Dallas, TX. "
    "All data is fictional. There are five tables in network_atlas.public:\n\n"
    "- antennas: individual radios. Columns: sys_id (PK), structure_sys_id (the site it's mounted on), "
    "radio_band (one of 600MHz, 700MHz, AWS, PCS, C-Band), carrier_number, azimuth (degrees the coverage sector points), "
    "h_beamwidth (sector width in degrees), mech_tilt, elec_tilt, lat, lon, h3_cell.\n"
    "- sites: the towers and rooftops antennas mount on. Columns: structure_sys_id (PK), site_type ('tower' or 'rooftop'), "
    "height_m, lat, lon, h3_cell.\n"
    "- cables: fiber lines. Columns: cable_id (PK), cable_type ('trunk' or 'distribution'), fiber_count, length_m. "
    "(No h3_cell or lat/lon \u2014 cables are lines, not points; do not geographically scope cable questions.)\n"
    "- nodes: outside-plant equipment along the cables. Columns: node_id (PK), node_type ('cabinet', 'splitter', "
    "'amplifier', 'power_supply'), status ('active' or 'fault'), lat, lon, h3_cell.\n"
    "- demand_h3: market demand aggregated to H3 res-9 hexagons. Columns: h3_cell (PK), homes_passed "
    "(residential+business premises the network could serve in that hex), biz_count (subset that are businesses), "
    "avg_arpu (avg monthly revenue per user, USD), est_build_cost_usd (estimated fiber build-out cost), cell_lat, cell_lon.\n\n"
    "TERMINOLOGY\n"
    "- 'Homes passed' = premises the network could connect (addressable market), not current subscribers.\n"
    "- A 'sector' is one antenna's coverage wedge, defined by azimuth + h_beamwidth. One antenna = one sector.\n"
    "- Join antennas to sites on antennas.structure_sys_id = sites.structure_sys_id.\n\n"
    "GEOGRAPHIC SCOPING\n"
    "Questions may include a filter hint that restricts results to a drawn area. Use whichever method the hint specifies:\n\n"
    "1) H3 cells (approximate): when the hint lists H3 cells, filter on the `h3_cell` text column, "
    "e.g. `WHERE h3_cell IN ('...', '...')`. antennas, sites, nodes, and demand_h3 all have an h3_cell column.\n\n"
    "2) Exact point-in-polygon: when the hint provides a polygon in WKT, reconstruct each asset's location from its "
    "`lat` and `lon` columns and test containment with native Databricks spatial functions:\n"
    "   `WHERE ST_Contains(ST_GeomFromText('<POLYGON WKT>', 4326), ST_SetSRID(ST_Point(lon, lat), 4326))`\n"
    "   ST_Point returns SRID 0, so it MUST be wrapped in ST_SetSRID(..., 4326) to match the polygon's SRID. "
    "antennas, sites, and nodes have lat/lon columns.\n\n"
    "If a question has no geographic filter hint, query across the whole dataset (no spatial predicate).\n\n"
    "The raw PostGIS `geom` and `sector_geom` columns federate only as opaque strings \u2014 do NOT query them directly. "
    "Use `h3_cell`, or reconstruct geometry from `lat`/`lon` as shown above."
)

serialized_space = json.dumps({
    "version": 1,
    "data_sources": {
        "tables": [{"identifier": t} for t in tables]
    },
    "instructions": {
        "text_instructions": [{
            "id": uuid.uuid4().hex,
            "content": [instruction_text]
        }]
    },
    "config": {
        "sample_questions": [
            {"id": uuid.uuid4().hex, "question": ["How many cell sites are towers vs rooftop?"]},
            {"id": uuid.uuid4().hex, "question": ["Which H3 cells have the highest avg ARPU?"]},
            {"id": uuid.uuid4().hex, "question": ["Show uncovered demand hexes with more than 50 homes passed"]},
        ]
    }
})

# Use the warehouse selected in the widget
print(f"Using SQL warehouse: {WAREHOUSE_ID}")

SPACE_TITLE = "GeoGenie Network Atlas"

payload = {
    "title": SPACE_TITLE,
    "description": "Synthetic Dallas telecom network: sites, antennas, cables, nodes, and demand (H3). Use h3_cell for geographic scoping.",
    "warehouse_id": WAREHOUSE_ID,
    "serialized_space": serialized_space,
}

# Check if a Genie Space with this name already exists
existing_space = None
try:
    for sp in (w.genie.list_spaces().spaces or []):
        if sp.title and sp.title.startswith(SPACE_TITLE):
            existing_space = sp
            break
except Exception:
    pass  # list failed — fall through to create

if existing_space:
    # Update the existing space instead of creating a duplicate
    space_id = existing_space.space_id
    try:
        w.api_client.do("PUT", f"/api/2.0/genie/spaces/{space_id}", body=payload)
        print(f"Genie Space '{existing_space.title}' updated (ID: {space_id}).")
    except Exception as e:
        print(f"⚠️  Update failed: {e}")
        print(f"  Reusing existing Space ID: {space_id}")
else:
    # Create a new Genie Space
    try:
        response = w.api_client.do("POST", "/api/2.0/genie/spaces", body=payload)
        space_id = response.get("space_id", response.get("id", "UNKNOWN"))
        print(f"Genie Space created!")
    except Exception as e:
        print(f"Failed to create Genie Space: {e}")
        print("\nYou can create it manually in the Genie UI with these tables:")
        for t in tables:
            print(f"  • {t}")
        raise

print(f"  Space ID: {space_id}")
print(f"  Tables:   {', '.join(tables)}")
print(f"\nSet this in app/app.yaml → GENIE_SPACE_ID: {space_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7 · Deploy App & Configure SP Resources
# MAGIC
# MAGIC The next cell handles everything end-to-end:
# MAGIC 1. Declares all app resources (SQL warehouse, Genie Space, Lakebase DB, UC tables)
# MAGIC 2. Creates or updates the app with resources — platform auto-grants the SP
# MAGIC 3. Deploys from the `app/` source directory
# MAGIC 4. Applies Postgres table-level grants (`SELECT` + `EXECUTE`) for the SP
# MAGIC
# MAGIC `app.yaml` uses `valueFrom: genie-space` so the Space ID is injected automatically
# MAGIC from the declared resource — no manual copy-paste needed.

# COMMAND ----------

# DBTITLE 1,Deploy App and Configure SP Resources
from databricks.sdk.service.apps import (
    App, AppResource,
    AppResourceSqlWarehouse, AppResourceSqlWarehouseSqlWarehousePermission,
    AppResourceGenieSpace, AppResourceGenieSpaceGenieSpacePermission,
    AppResourcePostgres, AppResourcePostgresPostgresPermission,
    AppResourceUcSecurable, AppResourceUcSecurableUcSecurablePermission,
    AppResourceUcSecurableUcSecurableType,
)

APP_NAME = "geogenie-network-atlas"
# Path to the app/ source in the workspace. Defaults to the current user's home;
notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
APP_SOURCE = f"/Workspace{"/".join(notebook_path.split("/")[:-2])}/app"

# ── 1. Declare ALL resources upfront ─────────────────────────────────────────
# The platform auto-grants the SP the declared permission for each resource.
resources = [
    AppResource(
        name="sql-warehouse",
        sql_warehouse=AppResourceSqlWarehouse(
            id=WAREHOUSE_ID,
            permission=AppResourceSqlWarehouseSqlWarehousePermission.CAN_USE,
        ),
    ),
    AppResource(
        name="genie-space",
        genie_space=AppResourceGenieSpace(
            name="GeoGenie Network Atlas",
            space_id=space_id,
            permission=AppResourceGenieSpaceGenieSpacePermission.CAN_RUN,
        ),
    ),
    AppResource(
        name="database",
        postgres=AppResourcePostgres(
            branch=BRANCH_NAME,
            database=f"{BRANCH_NAME}/databases/databricks-postgres",
            permission=AppResourcePostgresPostgresPermission.CAN_CONNECT_AND_CREATE,
        ),
    ),
] + [
    AppResource(
        name=f"table-{tbl.split('.')[-1]}",
        uc_securable=AppResourceUcSecurable(
            securable_full_name=tbl,
            securable_type=AppResourceUcSecurableUcSecurableType.TABLE,
            permission=AppResourceUcSecurableUcSecurablePermission.SELECT,
        ),
    )
    for tbl in tables  # from the Genie Space cell above
]

print(f"Declaring {len(resources)} resources: sql-warehouse, genie-space, database, + {len(tables)} UC tables")

# ── 2. Create or update the app with resources ───────────────────────────────
import time as _time

try:
    app_info = w.apps.get(APP_NAME)
    # App exists — wait for stable compute state, then update
    for _attempt in range(18):
        try:
            app_info = w.apps.update(
                name=APP_NAME,
                app=App(name=APP_NAME, resources=resources),
            )
            print(f"\u2139\ufe0f  App '{APP_NAME}' updated with resources.")
            break
        except Exception as update_err:
            if "STARTING" in str(update_err) or "STOPPING" in str(update_err):
                print(f"  App compute not ready, retrying in 10s...")
                _time.sleep(10)
            else:
                raise update_err
    else:
        print(f"\u26a0\ufe0f  App compute did not stabilize after 3 min. Re-run this cell later.")
except Exception as e:
    if "NOT_FOUND" in str(e) or "does not exist" in str(e).lower():
        app_info = w.apps.create_and_wait(
            app=App(
                name=APP_NAME,
                description="GeoGenie Network Atlas \u2014 telecom network map + Genie NL",
                resources=resources,
            ),
        )
        print(f"\u2705 App '{APP_NAME}' created with all resources.")
    else:
        raise

# ── 3. Deploy the app ────────────────────────────────────────────────────────
from databricks.sdk.service.apps import AppDeployment

try:
    deployment = w.apps.deploy_and_wait(
        app_name=APP_NAME,
        app_deployment=AppDeployment(source_code_path=APP_SOURCE),
    )
    print(f"\u2705 Deployment succeeded: {deployment.deployment_id}")
except Exception as e:
    print(f"\u26a0\ufe0f  Deployment: {e}")

# ── 4. Get the app's service principal ───────────────────────────────────────
app_info = w.apps.get(APP_NAME)
SP_ID = app_info.service_principal_client_id
SP_NAME = app_info.service_principal_name
print(f"\n  App SP: {SP_NAME} (client_id: {SP_ID})")

# ── 5. Postgres table-level grants (resource only gives CONNECT + CREATE) ────
# The postgres resource auto-creates the role, but explicit SELECT/EXECUTE is
# still needed for existing tables.
# Note: Lakebase resource ID uses hyphens (databricks-postgres) but the actual
# Postgres database name uses underscores (databricks_postgres).
try:
    import psycopg as _pg
    with _pg.connect(
        host=PG_HOST, dbname="databricks_postgres",
        user=w.current_user.me().user_name,
        password=w.postgres.generate_database_credential(endpoint=ENDPOINT_NAME).token,
        port=5432, sslmode="require",
    ) as conn, conn.cursor() as cur:
        cur.execute(f'GRANT USAGE ON SCHEMA public TO "{SP_ID}"')
        cur.execute(f'GRANT SELECT ON ALL TABLES IN SCHEMA public TO "{SP_ID}"')
        cur.execute(f'GRANT EXECUTE ON FUNCTION public.analyze_expansion_roi(text) TO "{SP_ID}"')
    print(f"\u2705 Postgres table/function grants applied.")
except Exception as e:
    print(f"\u26a0\ufe0f  Postgres grants: {e}")

# ── Summary ──────────────────────────────────────────────────────────────────
print(f"\n{'='*60}")
print(f"App URL: https://{w.config.host.replace('https://','')}/apps-v2/app/{APP_NAME}/overview")
print(f"SP resources (platform-managed):")
print(f"  \u2022 SQL Warehouse: {WAREHOUSE_ID} (CAN_USE)")
print(f"  \u2022 Genie Space: {space_id} (CAN_RUN)")
print(f"  \u2022 Lakebase: {BRANCH_NAME} (CAN_CONNECT_AND_CREATE)")
print(f"  \u2022 UC tables: {len(tables)}x SELECT on {FEDERATED_CATALOG}.{PG_SCHEMA}.*")
print(f"  \u2022 Postgres grants: SELECT + EXECUTE (manual, one-time)")

# COMMAND ----------

print("Done. Lakebase instance:", LAKEBASE_INSTANCE)
print("Host:", PG_HOST)
print("UC catalog for Genie:", FEDERATED_CATALOG)
