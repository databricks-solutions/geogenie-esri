"""Genie natural-language layer.

Genie cannot talk to Lakebase/PostGIS directly — it only queries Unity Catalog
tables through a SQL warehouse. So the Lakebase network tables are exposed to UC
read-only via Lakehouse Federation (see the provisioning notebook), and this
Genie Space is pointed at that federated catalog.

Spatial scoping of a drawn region is done with H3, NOT PostGIS: every row in the
federated tables carries an `h3_cell` column, and a drawn polygon is converted
to the set of H3 cells it covers, injected into the prompt as a filter hint.
PostGIS geometry does not federate to a usable Databricks type, which is exactly
why the H3 column exists.

Adapted from GeoGenie's services/genie.py (same start_conversation_and_wait +
polygon-injection pattern).
"""
import base64

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()


def format_h3_scope_for_query(h3_cells: list[str]) -> str:
    """Build a natural-language filter hint that scopes results to H3 cells.

    The frontend converts the drawn polygon to H3 res-9 cells (client-side, via
    h3-js) and passes them here. We inject them as an explicit filter instruction
    so Genie's generated SQL narrows to `h3_cell IN (...)`.
    """
    if not h3_cells:
        return ""
    # Cap the injected list so a huge lasso can't blow up the prompt; Genie still
    # gets a representative filter. (Tune as needed.)
    capped = h3_cells[:2000]
    cell_list = ", ".join(f"'{c}'" for c in capped)
    return (
        "\n\n[Geographic filter: restrict all results to network assets whose "
        f"h3_cell is one of the following {len(capped)} H3 res-9 cells. "
        f"Add `WHERE h3_cell IN ({cell_list})` (or the equivalent join) to the query.]"
    )


def format_st_scope_for_query(polygon_wkt: str) -> str:
    """Build a filter hint that scopes results with exact point-in-polygon SQL.

    The frontend sends the drawn polygon as WKT. Databricks SQL has native ST_*
    functions, so we instruct Genie to reconstruct each asset's point from its
    lat/lon columns and test containment against the drawn polygon. This is exact
    (unlike H3's hexagon approximation). Note ST_Point yields SRID 0, so it must
    be wrapped in ST_SetSRID(..., 4326) to match the polygon's SRID.
    """
    if not polygon_wkt:
        return ""
    return (
        "\n\n[Geographic filter: restrict all results to assets located inside "
        "this polygon, using an EXACT point-in-polygon test. Every asset table "
        "has `lat` and `lon` columns. Add this predicate to the query: "
        "`WHERE ST_Contains(ST_GeomFromText('" + polygon_wkt + "', 4326), "
        "ST_SetSRID(ST_Point(lon, lat), 4326))`. Do NOT use h3_cell for this "
        "query; use the ST_Contains predicate above.]"
    )


def _extract_table(space_id: str, conversation_id: str, message_id: str,
                   attachment_id: str) -> dict | None:
    """Fetch the tabular result behind a query attachment.

    Genie's text attachment is only a prose summary; the actual rows live in the
    query attachment's execution result, retrieved separately. The message-level
    result endpoint returns only the manifest (no rows) when a viz attachment is
    also present, so we fetch by the query's attachment_id. Returns
    {"columns": [...], "rows": [[...], ...], "truncated": bool} or None.
    """
    resp = None
    for fetch in (
        lambda: w.genie.get_message_attachment_query_result(
            space_id=space_id, conversation_id=conversation_id,
            message_id=message_id, attachment_id=attachment_id),
        lambda: w.genie.get_message_query_result_by_attachment(
            space_id=space_id, conversation_id=conversation_id,
            message_id=message_id, attachment_id=attachment_id),
        lambda: w.genie.get_message_query_result(
            space_id=space_id, conversation_id=conversation_id,
            message_id=message_id),
    ):
        try:
            resp = fetch()
            if resp and getattr(resp, "statement_response", None):
                break
        except Exception:
            continue
    if resp is None:
        return None

    stmt = getattr(resp, "statement_response", None)
    if not stmt:
        return None

    manifest = getattr(stmt, "manifest", None)
    result = getattr(stmt, "result", None)
    schema = getattr(manifest, "schema", None) if manifest else None
    columns = [c.name for c in schema.columns] if schema and schema.columns else []
    rows = getattr(result, "data_array", None) if result else None
    if not columns and not rows:
        return None

    return {
        "columns": columns,
        "rows": rows or [],
        "truncated": bool(getattr(manifest, "truncated", False)) if manifest else False,
    }


# Column-name candidates for detecting a *geographic* result — one that should
# be drawn natively on the ESRI map instead of shown as Genie's server-rendered
# PNG. Genie's server-side map export drops the Mapbox basemap tiles (it renders
# the data points on a blank gray canvas), so for geographic answers we hand the
# rows to the browser and let the ArcGIS map draw them. Kept in sync with the
# frontend's detection in static/index.html.
_LAT_COLS = ("lat", "latitude", "cell_lat", "site_lat")
_LON_COLS = ("lon", "lng", "long", "longitude", "cell_lon", "site_lon")


def _result_is_geographic(table: dict | None) -> bool:
    """True when the query result carries columns we can plot on the map.

    Either an explicit lat/lon column pair, or an H3 cell column (which the
    frontend expands to a hexagon via h3-js). Non-geographic results (e.g. a
    count by radio band) fall through to Genie's rendered PNG, which is fine for
    bar/pie charts — only the map viz type loses its basemap on server export.
    """
    if not table or not table.get("columns"):
        return False
    cols = {c.lower() for c in table["columns"]}
    has_latlon = (any(c in cols for c in _LAT_COLS)
                  and any(c in cols for c in _LON_COLS))
    has_h3 = any("h3" in c or "hex" in c for c in cols)
    return has_latlon or has_h3


def _download_viz_png(space_id: str, conversation_id: str, message_id: str,
                      attachment_id: str) -> str | None:
    """Download a rendered PNG for a visualization attachment, base64-encoded.

    The Genie viz-download endpoint (Beta, requires enable_visualization=True on
    the conversation) returns a raw PNG. We base64-encode it into a data URI so it
    rides over JSON to the browser as an <img> src. Returns None if unavailable.
    """
    name = (f"spaces/{space_id}/conversations/{conversation_id}"
            f"/messages/{message_id}/attachments/{attachment_id}")
    try:
        resp = w.genie.download_message_attachment_visualization(name=name)
    except Exception:
        return None

    contents = getattr(resp, "contents", None)
    if contents is None:
        return None
    raw = contents.read() if hasattr(contents, "read") else contents
    if not raw:
        return None
    return "data:image/png;base64," + base64.b64encode(raw).decode("ascii")


def send_genie_query(genie_space_id: str, prompt: str,
                     h3_cells: list[str] | None = None,
                     scope_mode: str = "h3",
                     polygon_wkt: str | None = None):
    """Send a query to the Genie Space, optionally scoped to a drawn region.

    Two scoping paths demonstrate that Databricks supports both approaches:
      - scope_mode="h3":  approximate, via `WHERE h3_cell IN (...)` (federates as
        plain text; the SQL an LLM generates most reliably).
      - scope_mode="st":  EXACT point-in-polygon, via the warehouse's native ST_*
        functions on the federated lat/lon columns.

    Returns:
        tuple: (answer_text, sql_query, table, chart, is_map) — sql_query, table,
        and chart may each be None. `table` is {"columns", "rows", "truncated"}
        when Genie ran a query; `chart` is a base64 PNG data URI when Genie
        produced a NON-map visualization (requires enable_visualization). `is_map`
        is True when the result is geographic — in that case `chart` is left None
        and the frontend renders the rows natively on the ESRI map (Genie's
        server-side map PNG loses its basemap tiles, so we skip it).
    """
    full_prompt = prompt
    if scope_mode == "st" and polygon_wkt:
        full_prompt += format_st_scope_for_query(polygon_wkt)
    elif h3_cells:
        full_prompt += format_h3_scope_for_query(h3_cells)

    genie_message = w.genie.start_conversation_and_wait(
        space_id=genie_space_id,
        content=full_prompt,
        enable_visualization=True,
    )

    response_parts = []
    sql_query = None
    query_attachment_id = None
    viz_attachment_id = None
    if genie_message.attachments:
        for attachment in genie_message.attachments:
            if attachment.text and attachment.text.content:
                response_parts.append(attachment.text.content)
            if attachment.query and attachment.query.query:
                sql_query = attachment.query.query
                query_attachment_id = attachment.attachment_id
            # A viz attachment carries its own attachment_id and points at the
            # query it was generated from.
            if getattr(attachment, "viz", None):
                viz_attachment_id = attachment.attachment_id

    # If Genie generated a query, fetch its tabular result so the app can render
    # the actual rows (not just the prose summary).
    table = None
    if query_attachment_id is not None:
        table = _extract_table(
            genie_space_id,
            genie_message.conversation_id,
            genie_message.id,
            query_attachment_id,
        )

    # If the result is geographic, the frontend draws it on the ESRI map, so we
    # skip Genie's rendered PNG (its server-side map export loses the basemap).
    # Otherwise, download the rendered PNG for non-map charts (bar/pie/line).
    is_map = _result_is_geographic(table)
    chart = None
    if viz_attachment_id and not is_map:
        chart = _download_viz_png(
            genie_space_id,
            genie_message.conversation_id,
            genie_message.id,
            viz_attachment_id,
        )

    if not response_parts:
        response_parts.append("Query completed successfully!")

    return "\n".join(response_parts), sql_query, table, chart, is_map
