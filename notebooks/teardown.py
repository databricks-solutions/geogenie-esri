# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Teardown Header
# MAGIC %md
# MAGIC # Network Atlas — Teardown
# MAGIC
# MAGIC Reverses everything created by `provision_and_setup`. Executes in reverse dependency order:
# MAGIC
# MAGIC 1. Stop & delete the **Databricks App** (`geogenie-network-atlas`)
# MAGIC 2. Delete the **Genie Space** ("GeoGenie Network Atlas")
# MAGIC 3. Drop the **UC federated catalog** (`geogenie_network_atlas`)
# MAGIC 4. Drop **Postgres tables, function, extension** (optional — project deletion handles this)
# MAGIC 5. Delete the **Lakebase project** (`geogenie-network-atlas-db`) — removes all branches, endpoints, databases, roles
# MAGIC
# MAGIC > **⚠️ DESTRUCTIVE** — Run cells individually and confirm each step. All data will be permanently lost.

# COMMAND ----------

# DBTITLE 1,Install dependencies
# MAGIC %pip install "psycopg[binary]==3.2.3" "databricks-sdk>=0.118.0" --quiet
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Parameters & SDK setup
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

# ── Parameters (must match provision_and_setup) ───────────────────────────────
dbutils.widgets.text("lakebase_instance", "geogenie-network-atlas-db", "Lakebase instance name")
dbutils.widgets.text("federated_catalog", "geogenie_network_atlas",    "UC catalog name for federation")

LAKEBASE_INSTANCE = dbutils.widgets.get("lakebase_instance")
FEDERATED_CATALOG = dbutils.widgets.get("federated_catalog")

PROJECT_ID   = LAKEBASE_INSTANCE
BRANCH_NAME  = f"projects/{PROJECT_ID}/branches/production"
ENDPOINT_NAME = f"{BRANCH_NAME}/endpoints/primary"
APP_NAME     = "geogenie-network-atlas"

print(f"Teardown targets:")
print(f"  App:             {APP_NAME}")
print(f"  UC Catalog:      {FEDERATED_CATALOG}")
print(f"  Lakebase project: {LAKEBASE_INSTANCE}")

# COMMAND ----------

# MAGIC %md ## 1 · Stop & Delete the Databricks App

# COMMAND ----------

# DBTITLE 1,Delete App
# Stop the app first (if running), then delete it.
try:
    app_info = w.apps.get(APP_NAME)
    print(f"Found app '{APP_NAME}' (status: {app_info.app_status})")

    # Stop if active
    if app_info.app_status and "RUNNING" in str(app_info.app_status.state):
        print("Stopping app...")
        w.apps.stop(APP_NAME)
        import time
        # Wait for app to reach terminal state before deleting
        for _ in range(60):
            time.sleep(10)
            app_info = w.apps.get(APP_NAME)
            state = str(app_info.app_status.state) if app_info.app_status else ""
            if "RUNNING" not in state and "STOPPING" not in state:
                break
        print(f"App state after stop: {app_info.app_status}")

    # Delete
    w.apps.delete(APP_NAME)
    print(f"✅ App '{APP_NAME}' deleted.")
except Exception as e:
    if "NOT_FOUND" in str(e) or "does not exist" in str(e).lower():
        print(f"App '{APP_NAME}' not found — already deleted or never created.")
    else:
        print(f"⚠️ Error deleting app: {e}")
        raise

# COMMAND ----------

# MAGIC %md ## 2 · Delete the Genie Space

# COMMAND ----------

# DBTITLE 1,Delete Genie Space
# Find and delete the Genie Space named "GeoGenie Network Atlas"
try:
    from databricks.sdk.service.dashboards import GenieSpace
    spaces = w.genie.list_spaces().spaces or []
    target_space = None
    for space in spaces:
        if space.title == "GeoGenie Network Atlas":
            target_space = space
            break

    if target_space:
        w.genie.trash_space(target_space.space_id)
        print(f"✅ Genie Space '{target_space.title}' (ID: {target_space.space_id}) deleted.")
    else:
        print("Genie Space 'GeoGenie Network Atlas' not found — already deleted or never created.")
except Exception as e:
    if "NOT_FOUND" in str(e):
        print("Genie Space not found — already deleted.")
    else:
        print(f"⚠️ Error deleting Genie Space: {e}")
        raise

# COMMAND ----------

# MAGIC %md ## 3 · Drop the UC Federated Catalog

# COMMAND ----------

# DBTITLE 1,Drop UC Catalog
# Drop the UC catalog that was registered via w.postgres.create_catalog()
# This only removes the UC registration — not the underlying Postgres data.
try:
    w.postgres.delete_catalog(name=f"catalogs/{FEDERATED_CATALOG}")
    print(f"✅ UC catalog '{FEDERATED_CATALOG}' deregistered (Lakebase native).")
except Exception as e:
    if "NOT_FOUND" in str(e) or "does not exist" in str(e).lower():
        print(f"UC catalog '{FEDERATED_CATALOG}' not found — already removed.")
    else:
        # Fallback: try SQL DROP CATALOG
        print(f"Native deletion failed ({e}), trying SQL fallback...")
        try:
            spark.sql(f"DROP CATALOG IF EXISTS {FEDERATED_CATALOG} CASCADE")
            print(f"✅ UC catalog '{FEDERATED_CATALOG}' dropped via SQL.")
        except Exception as e2:
            print(f"⚠️ Could not drop catalog: {e2}")
            raise

# COMMAND ----------

# MAGIC %md ## 4 · Drop Postgres Tables & Functions (optional)
# MAGIC
# MAGIC This step is **optional** — deleting the Lakebase project (Step 5) removes everything.
# MAGIC Run this only if you want to clean Postgres without destroying the project.

# COMMAND ----------

# DBTITLE 1,Drop Postgres objects
import psycopg

PG_DATABASE = "databricks_postgres"
PG_USER = w.current_user.me().user_name

def pg_password():
    cred = w.postgres.generate_database_credential(endpoint=ENDPOINT_NAME)
    return cred.token

def connect():
    ep = w.postgres.get_endpoint(name=ENDPOINT_NAME)
    host = ep.status.hosts.host
    return psycopg.connect(
        host=host, dbname=PG_DATABASE, user=PG_USER,
        password=pg_password(), port=5432, sslmode="require",
        autocommit=True,
    )

try:
    with connect() as conn, conn.cursor() as cur:
        # Drop function
        cur.execute("DROP FUNCTION IF EXISTS public.analyze_expansion_roi(text) CASCADE")
        # Drop tables
        for tbl in ["antennas", "sites", "cables", "nodes", "demand_h3"]:
            cur.execute(f"DROP TABLE IF EXISTS public.{tbl} CASCADE")
        # Drop extension
        cur.execute("DROP EXTENSION IF EXISTS postgis CASCADE")
    print("✅ All Postgres objects dropped (tables, function, PostGIS extension).")
except Exception as e:
    if "does not exist" in str(e).lower() or "could not connect" in str(e).lower():
        print(f"Skipped — Lakebase endpoint not reachable (project may already be deleted): {e}")
    else:
        print(f"⚠️ Error: {e}")
        raise

# COMMAND ----------

# MAGIC %md ## 5 · Delete the Lakebase Project
# MAGIC
# MAGIC > **⚠️ IRREVERSIBLE** — This permanently deletes the project, all branches, endpoints,
# MAGIC > databases, roles, and all data. There is no undo.

# COMMAND ----------

# DBTITLE 1,Delete Lakebase project
# ⚠️ DESTRUCTIVE: Permanently deletes the Lakebase project and ALL its data.

print(f"To delete Lakebase project '{PROJECT_ID}', uncomment and run:")
print(f"")
print(f"  w.postgres.delete_project(name='projects/{PROJECT_ID}')")
print(f"")
print(f"This will destroy ALL data, branches, endpoints, and roles.")

try:
    op = w.postgres.delete_project(name=f"projects/{PROJECT_ID}")
    op.wait()  # Block until deletion fully completes (releases the project slug)
    print(f"✅ Lakebase project '{PROJECT_ID}' fully deleted.")
except Exception as e:
    if "not found" in str(e).lower() or "NOT_FOUND" in str(e):
        print(f"Lakebase project '{PROJECT_ID}' not found — already deleted.")
    else:
        print(f"⚠️ Error deleting project: {e}")
        raise

# COMMAND ----------

# MAGIC %md ## ✅ Teardown Complete
# MAGIC
# MAGIC All resources created by `provision_and_setup` have been removed:
# MAGIC * Databricks App (`network-atlas`)
# MAGIC * Genie Space ("Network Atlas")
# MAGIC * UC federated catalog (`network_atlas`)
# MAGIC * Postgres objects (tables, function, PostGIS extension)
# MAGIC * Lakebase project (`network-atlas-db`)
