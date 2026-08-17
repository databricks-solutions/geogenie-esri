# SETUP — deploy GeoGenie Network Atlas on a fresh workspace

End-to-end steps. Assumes Unity Catalog, a SQL warehouse, and permission to create
Lakebase instances, Databricks Apps, and Genie Spaces.

---

## Quick start

Open `notebooks/provision_and_setup` and **Run All**. That's it — the notebook
provisions every resource end-to-end.

### Alternative: via bundle (for CI/CD or headless runs)

```bash
databricks bundle deploy -t dev                    # registers the provisioning job
databricks bundle run geogenie_provision -t dev     # runs the notebook as a job
```

The bundle is optional scaffolding — useful for automation, team triggers, or job
monitoring, but not required for interactive use.

---

## What the provisioning notebook does

The notebook is the single source of truth — it creates and configures ALL resources:

1. **Lakebase project** (`geogenie-network-atlas-db`) — Postgres + PostGIS
2. **Schema & tables** — `sites`, `antennas`, `cables`, `nodes`, `demand_h3` (with `geom` + `h3_cell`)
3. **Synthetic data** — deterministic Dallas network generated into Postgres
4. **PostGIS function** — `analyze_expansion_roi()` with smoke test
5. **UC federated catalog** (`geogenie_network_atlas`) — registers Lakebase for Genie
6. **Genie Space** ("GeoGenie Network Atlas") — with full schema docs + geographic scoping instructions
7. **Databricks App** (`geogenie-network-atlas`) — creates/updates with all resources attached, deploys from `app/`, and grants SP access to Postgres tables

All steps are **idempotent** — safe to re-run.

### Parameters (widgets)

| Widget | Default | Description |
| --- | --- | --- |
| `lakebase_instance` | `geogenie-network-atlas-db` | Lakebase Autoscaling project name |
| `federated_catalog` | `geogenie_network_atlas` | UC catalog name for Genie |
| `capacity` | `CU_1` | Lakebase capacity |
| `warehouse` | (auto-detected) | SQL warehouse for Genie queries |
| `seed` | `42` | Random seed for data generation |

---

## Bundle structure

```
geogenie-esri/
├── databricks.yml              # Bundle config — declares provisioning job
├── notebooks/
│   ├── provision_and_setup.py  # Creates everything (single source of truth)
│   └── teardown.py             # Reverses all provisioned resources
├── app/                        # App source (FastAPI + ESRI map)
│   ├── app.py
│   └── app.yaml
└── SETUP.md                    # This file
```

### Deployment model

- `databricks.yml` declares a **job** (`geogenie_provision`) that runs the notebook.
- The **app is NOT declared in the bundle** — the notebook handles app creation,
  resource attachment (Genie Space, Lakebase, UC tables), SP grants, and deployment.
  This avoids conflicts between bundle reconciliation and imperative resource management.

---

## Fallback: PostgreSQL Lakehouse Federation

If the notebook's step 5 (native UC registration) reports it isn't available:

1. Catalog Explorer → **External Data → Connections → Create connection** → type
   **PostgreSQL**. Host = the Lakebase host; auth = OAuth / the DB credential.
2. Create a **foreign catalog** from that connection named `geogenie_network_atlas`.
3. The `public.*` tables now appear in UC. (Geometry columns show as unsupported —
   ignore them; Genie uses `h3_cell`.)

---

## Teardown

To reverse all provisioned resources, open `notebooks/teardown` and run cells step by
step. Each step has safety checks and the most destructive action (project removal)
requires manual uncommenting. See the notebook header for the full sequence.

---

## Verify

- `GET /healthz` → `{"ok": true, "lakebase": true}`
- Open the app URL → the Dallas network draws; panning refetches the viewport.
- Toggle **Sectors** → purple azimuth wedges appear.
- **Draw area** → an ROI verdict panel fills in.
- **Ask Genie** → after drawing, questions are scoped to that area; a SQL query is shown.

---

## Auth model (summary)

- **User → app:** Databricks Apps login gate (your Databricks identity).
- **App → Lakebase:** the app's **service principal** mints a short-lived OAuth token and
  uses it as the Postgres password (no stored secret); refreshed proactively.
- **App → Genie:** the same SP calls the Genie API via the Databricks SDK.
- **Genie → data:** the SQL warehouse reads the UC-federated Lakebase tables.
