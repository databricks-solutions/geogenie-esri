# GeoGenie Network Atlas — an ArcGIS-style network map on Databricks

An interactive network map of a synthetic **wireless telecom network** (a regional
carrier around Dallas, TX), built entirely on Databricks. It shows the same kind of
view a telco runs in an **ArcGIS Experience Builder** app — antennas with coverage
**sectors**, fiber **cables**, equipment **nodes**, and mount **sites**, all
clickable — plus two things a typical Experience Builder app can't do:

1. **Expansion ROI** — lasso an area and get a **Build / Marginal / Pass** verdict,
   scored on the demand *not already covered* by existing antenna sectors.
2. **Ask Genie** — natural-language questions about the network, automatically
   **scoped to the area you drew** — either approximately (via H3) or with an
   **exact point-in-polygon** filter using the SQL warehouse's native `ST_*`
   functions. Genie answers come back with the prose summary, the **result table**,
   and any **chart** Genie generates.

A **demand heatmap** overlay (toggle) shows where the addressable market
concentrates, so you can see where an expansion lasso is worth drawing.

> **The data is 100% synthetic**, generated deterministically inside the provisioning
> notebook. It does not represent any real carrier's network, subscribers, or revenue.

---

## Video Overview

A short walkthrough of the app — the map, sectors and demand heatmap, the Expansion ROI
lasso, and Ask Genie with region scoping:

https://github.com/databricks-solutions/geogenie-esri/raw/main/media/geogenie_esri.mp4

_(GitHub renders the link above as an inline player. Direct file:
[`media/geogenie_esri.mp4`](media/geogenie_esri.mp4).)_

---

## The pitch: "Can ArcGIS be done in Databricks?"

Yes — and this repo is the proof. A telco's "Network Atlas" is typically an **ArcGIS
Experience Builder** app backed by **ArcGIS Enterprise**. This delivers the same
experience as a single **Databricks App** you open at one login-gated URL. ESRI is
reduced to a *browser drawing library*; every data question is answered by Databricks.

| Concern | How it's solved here | The "usual" ArcGIS way it replaces |
|---|---|---|
| Draw a background map | ESRI hosted basemap (streamed to the browser) | Hosting your own tiles |
| Click a feature / draw a shape | ESRI `FeatureLayer` popups + `SketchViewModel` | Same, but on ArcGIS Server |
| Store the network data | **Lakebase Postgres** (single source of truth) | A hosted feature service |
| Fast "what's in this box / polygon" | **PostGIS `ST_*`, one call** | **ArcGIS Server / GeoAnalytics** |
| Natural-language questions | **Genie** over UC-federated tables (region-scoped via H3 or native `ST_*`) | (not possible) |
| Auth / secrets | Service principal + runtime OAuth token | ArcGIS Enterprise identity |

**No ArcGIS Server, no hosted feature service, no ArcGIS Enterprise portal.** The only
runtime ESRI dependency is the browser fetching the basemap tiles.

---

## Architecture — one source of truth, two consumers

```
                     Provisioning notebook (numpy seed 42)
                                  │
                                  ▼
                 ┌──────────────────────────────────────┐
                 │  Lakebase Postgres + PostGIS          │  ← SINGLE SOURCE OF TRUTH
                 │  sites · antennas(+sector) · cables    │
                 │  nodes · demand_h3                     │
                 │  each row: geom (PostGIS) + h3_cell    │
                 └──────────────────────────────────────┘
                     │                              │
   geom columns      │                              │  registered read-only into
   (PostGIS)         ▼                              ▼  Unity Catalog (federation)
   ┌───────────────────────────────┐      ┌──────────────────────────────────┐
   │ FastAPI (Databricks App)       │      │ UC foreign catalog                │
   │  GET  /api/features/{layer}    │      │  geogenie_network_atlas.public.*  │
   │  POST /api/analyze             │      │        │                          │
   │  POST /api/genie ──────────────┼──────┼──►  Genie Space (H3 or ST_* scope) │
   └───────────────────────────────┘      └──────────────────────────────────┘
                     │                              ▲
   the MAP hits Postgres directly (ms)   Genie queries the federated tables via a
   for viewport features + lasso ROI     SQL warehouse; scopes the drawn region by
                                         h3_cell OR native ST_* on lat/lon
            ▲
            │  Browser: ESRI ArcGIS Maps SDK for JavaScript 4.29
            │   • dark-gray-vector basemap (from ESRI's CDN)
            │   • FeatureLayers (click to inspect), sector wedges, freehand lasso
            │   • demand heatmap toggle; Genie chat with table + chart results
            │   • drawn polygon → H3 cells OR polygon WKT → region-scoped questions
```

**Why this split:** the map pans/zooms/lassos constantly — that needs millisecond
queries, so it hits **Lakebase/PostGIS directly**. Genie only queries occasionally and
needs **Unity Catalog** tables, so the same Lakebase data is exposed to UC read-only via
Lakehouse Federation. One authoritative copy, no sync job.

**How Genie scopes to a drawn region — two ways, both on Databricks:**

- **H3 (approximate).** Every row carries an `h3_cell` text column. The frontend
  converts the drawn polygon to res-9 cells and Genie filters `WHERE h3_cell IN (...)`.
  Plain-text set membership — the SQL an LLM generates most reliably.
- **`ST_*` (exact point-in-polygon).** Databricks SQL has native geospatial functions,
  so Genie can reconstruct each asset's point from its federated `lat`/`lon` and test
  `ST_Contains(ST_GeomFromText('POLYGON(...)', 4326), ST_SetSRID(ST_Point(lon, lat), 4326))`
  against the drawn polygon — exact to the boundary, no hexagon approximation.

The raw PostGIS `geom` column **does** federate, but only as an opaque WKB string with
no spatial meaning — you can't run `ST_*` on it directly. That's why the two usable
paths above exist: `h3_cell` (a string that federates cleanly) and geometry
**reconstructed** from the `lat`/`lon` doubles. The map still uses real PostGIS
`geom` directly inside Lakebase for its millisecond spatial ops.

---

## Repo layout

```
app/
  app.py                    FastAPI: / · /api/features/{layer} · /api/analyze · /api/genie · /healthz
  lakebase.py               PostGIS access, OAuth-token auth, allowlisted per-layer bbox queries
                            (incl. the demand layer for the heatmap)
  genie.py                  Genie NL query + region-scope injection (H3 or exact ST_*), plus
                            result-table and chart-PNG retrieval
  static/index.html         ESRI ArcGIS JS 4.29 map — layers, sectors, demand heatmap, popups,
                            lasso, Genie chat (tables + charts, H3/ST_* sample questions)
  app.yaml                  start command + GENIE_SPACE_ID (from a genie-space app resource)
  requirements.txt          fastapi, uvicorn, databricks-sdk (>=0.120), psycopg[binary], pydantic

sql/
  functions_postgis.sql     the analyze_expansion_roi() PostGIS model (also inlined in the notebook)

notebooks/
  provision_and_setup.py    ONE notebook, end to end: create Lakebase, enable PostGIS, generate
                            synthetic network into Postgres, install ROI fn, register UC catalog,
                            create the Genie Space, and create + deploy the app with its resources
  teardown.py               Reverses all provisioned resources (run cells individually)

databricks.yml              Bundle config — declares a provisioning job that runs the notebook
                            (optional; for CI/CD or headless runs)
SETUP.md                    step-by-step deploy on a fresh workspace
```

---

## Installation

Open `notebooks/provision_and_setup` and **Run All**. The notebook provisions
everything end-to-end:

1. Creates the **Lakebase** project (`geogenie-network-atlas-db`), enables PostGIS,
   and generates the synthetic Dallas network into Postgres (each row with both
   `geom` and `h3_cell`).
2. Installs the `analyze_expansion_roi()` ROI function.
3. Registers the Lakebase DB as a **UC foreign catalog** (`geogenie_network_atlas`)
   for Genie.
4. Creates the **Genie Space** ("GeoGenie Network Atlas") over
   `geogenie_network_atlas.public.*` (with the schema + dual-scoping instruction
   baked in) — no manual space creation, no ID to copy.
5. Creates and deploys the **app** (`geogenie-network-atlas`) with its resources
   (SQL warehouse, Genie Space, Lakebase, UC table SELECTs) and applies the
   Postgres GRANTs.

Alternatively, deploy via the bundle for CI/CD or headless runs:

```bash
databricks bundle deploy -t dev
databricks bundle run geogenie_provision -t dev
```

The Genie Space ID reaches the app via a `genie-space` **app resource** (`app.yaml`
reads it with `valueFrom: genie-space`) — you don't paste an ID anywhere.

Open the app URL and you'll see the Dallas network. Click assets to inspect, toggle
**Sectors** and the **Demand** heatmap, lasso an area for an ROI verdict, and click
**Ask Genie** to ask questions scoped to the drawn area (the sample-question chips show
which use the H3 vs. exact `ST_*` path). See **[SETUP.md](SETUP.md)** for the full
sequence and the federation fallback.

### Local development

```bash
cd app
pip install -r requirements.txt
export DATABRICKS_HOST=https://<workspace>.cloud.databricks.com
export DATABRICKS_TOKEN=<pat>
export PGHOST=<lakebase-host> PGDATABASE=databricks_postgres PGUSER=<you>
export GENIE_SPACE_ID=<your-space-id>
# LAKEBASE_ENDPOINT unset locally → lakebase.py falls back to a workspace OAuth token.
uvicorn app:app --reload --port 8000
```

---

## Notes & upgrade paths

- **ESRI SDK load order.** `h3-js` **must** load before the ArcGIS SDK in
  `static/index.html` — the SDK installs a global AMD `define()` that h3-js's UMD
  wrapper otherwise collides with (blank map). The keyless `dark-gray-vector` basemap
  works as-is; if it ever renders blank, set `esriConfig.apiKey` or switch styles.
- **UC federation of Lakebase is recent.** If native registration isn't enabled in your
  workspace, use standard PostgreSQL Lakehouse Federation (SETUP.md). Either way Genie
  reads the same tables.
- **Genie region scoping — H3 vs. `ST_*`.** H3 is approximate (res-9 hexagons; the
  injected cell list is capped for large lassos — tune in `genie.py`/`index.html`).
  The `ST_*` path is exact but relies on Genie reliably emitting the
  `ST_Contains(...)` SQL; the Genie Space instruction describes both paths so it does.
  Raw PostGIS `geom` federates only as an opaque WKB string — don't query it directly.
- **`databricks-sdk >= 0.120`.** Required for the Genie **visualization download**
  endpoint (`download_message_attachment_visualization`) used to render chart PNGs;
  also gives the attachment-scoped query-result call that returns table rows when a
  chart is present.
- **Redeploying.** Re-run the provisioning notebook to update the app. The notebook
  re-attaches resources and re-applies Postgres GRANTs idempotently — no manual
  steps needed.
- **Related work.** The Genie natural-language layer (dual H3 / `ST_*` scoping,
  result tables, chart rendering) follows the pattern in
  [GeoGenie](https://github.com/databricks-solutions/genie-geo-chat), a sibling
  Databricks demo that pairs a Cesium/Leaflet map with Genie.

---

## How to get help

Databricks support doesn't cover this content. For questions or bugs, please open a
GitHub issue and the team will help on a best effort basis.

---

## License

&copy; 2025 Databricks, Inc. All rights reserved. The source in this project is provided
subject to the Databricks License [https://databricks.com/db-license-source]. All
included or referenced third party libraries are subject to the licenses set forth below.

| library | description | license | source |
|---|---|---|---|
| FastAPI | ASGI web framework for the app API | MIT | https://github.com/fastapi/fastapi |
| Uvicorn | ASGI server that runs the app | BSD-3-Clause | https://github.com/encode/uvicorn |
| databricks-sdk | Databricks SDK for Python (Genie, workspace APIs) | Apache-2.0 | https://github.com/databricks/databricks-sdk-py |
| psycopg (psycopg 3) | PostgreSQL / PostGIS driver | LGPL-3.0 | https://github.com/psycopg/psycopg |
| pydantic | Request/response data validation | MIT | https://github.com/pydantic/pydantic |
| ArcGIS Maps SDK for JavaScript 4.29 | Browser map rendering, feature layers, sketch/lasso | Esri Master Agreement (proprietary) | https://developers.arcgis.com/javascript/ |
| h3-js 4.1.0 | Polygon → H3 cell conversion in the browser | Apache-2.0 | https://github.com/uber/h3-js |
| PostGIS | Spatial functions in Lakebase Postgres | GPL-2.0 | https://postgis.net |
