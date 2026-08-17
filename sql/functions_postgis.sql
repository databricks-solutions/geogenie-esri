-- ============================================================
-- functions_postgis.sql — the expansion-ROI model, run in Lakebase Postgres.
--
-- analyze_expansion_roi(polygon_geojson): clip demand hexes to the drawn polygon,
-- mark each hex covered if its centroid falls inside ANY existing antenna sector,
-- and score the ROI of building only the UNCOVERED demand. The antenna sectors
-- shown on the map are exactly what shrinks the opportunity here.
--
-- This runs in PostGIS (the map's fast path). It is NOT visible to Genie —
-- Genie uses the UC-federated copy of the tables with H3 scoping instead.
--
-- The provisioning notebook installs this automatically; this file is kept so
-- the model is reviewable and re-appliable on its own.
-- ============================================================

CREATE EXTENSION IF NOT EXISTS postgis;

CREATE OR REPLACE FUNCTION public.analyze_expansion_roi(polygon_geojson text)
RETURNS TABLE (
  total_hexes            integer,
  covered_hexes          integer,
  uncovered_hexes        integer,
  homes_passed_uncovered bigint,
  biz_count              bigint,
  avg_arpu               numeric,
  projected_take_rate    numeric,
  projected_subs         bigint,
  total_capex_usd        numeric,
  annual_revenue_usd     numeric,
  five_yr_revenue_usd    numeric,
  five_yr_roi_pct        numeric,
  payback_months         numeric,
  roi_tier               text
)
LANGUAGE sql STABLE AS
$$
WITH poly AS (
  SELECT ST_SetSRID(ST_GeomFromGeoJSON(polygon_geojson), 4326) AS g
),
scoped AS (
  -- bbox prefilter on the (cell_lat, cell_lon) btree, then exact point-in-polygon
  SELECT d.*
  FROM public.demand_h3 d, poly p
  WHERE d.cell_lat BETWEEN ST_YMin(p.g) AND ST_YMax(p.g)
    AND d.cell_lon BETWEEN ST_XMin(p.g) AND ST_XMax(p.g)
    AND ST_Contains(p.g, d.geom)
),
flagged AS (
  SELECT s.*,
    EXISTS (
      SELECT 1 FROM public.antennas a
      WHERE a.sector_geom && s.geom AND ST_Intersects(a.sector_geom, s.geom)
    ) AS covered
  FROM scoped s
),
agg AS (
  SELECT
    COUNT(*)::int                                              AS total_hexes,
    COUNT(*) FILTER (WHERE covered)::int                       AS covered_hexes,
    COUNT(*) FILTER (WHERE NOT covered)::int                   AS uncovered_hexes,
    COALESCE(SUM(homes_passed) FILTER (WHERE NOT covered), 0)  AS homes_unc,
    COALESCE(SUM(biz_count)    FILTER (WHERE NOT covered), 0)  AS biz_unc,
    AVG(avg_arpu)              FILTER (WHERE NOT covered)       AS arpu_unc,
    COALESCE(SUM(est_build_cost_usd) FILTER (WHERE NOT covered), 0) AS build_unc
  FROM flagged
),
model AS (
  SELECT *,
    -- take rate: 38% base + ARPU-driven uplift, clamped to [12%, 55%]
    LEAST(0.55, GREATEST(0.12,
      0.38 + 0.06 * (COALESCE(arpu_unc, 80) / 100.0 - 0.8)
    )) AS take_rate
  FROM agg
),
calc AS (
  SELECT *,
    ROUND(homes_unc * take_rate)::bigint AS subs
  FROM model
),
fin AS (
  SELECT *,
    build_unc + subs * 650                          AS total_capex,
    subs * COALESCE(arpu_unc, 80) * 12              AS annual_rev,
    subs * COALESCE(arpu_unc, 80) * 12 * 5 * 0.62   AS five_yr_margin  -- 62% margin
  FROM calc
)
SELECT
  total_hexes,
  covered_hexes,
  uncovered_hexes,
  homes_unc,
  biz_unc,
  ROUND(COALESCE(arpu_unc,0)::numeric, 2),
  ROUND(take_rate::numeric, 3),
  subs,
  ROUND(total_capex::numeric, 0),
  ROUND(annual_rev::numeric, 0),
  ROUND(five_yr_margin::numeric, 0),
  CASE WHEN total_capex > 0
       THEN ROUND((100.0 * (five_yr_margin - total_capex) / total_capex)::numeric, 1)
       ELSE 0 END,
  CASE WHEN annual_rev * 0.62 > 0
       THEN ROUND((total_capex / (annual_rev * 0.62 / 12.0))::numeric, 1)
       ELSE NULL END,
  CASE
    WHEN homes_unc = 0 THEN 'NO_DATA'
    WHEN total_capex > 0 AND five_yr_margin / total_capex >= 2.0 THEN 'BUILD'
    WHEN total_capex > 0 AND five_yr_margin / total_capex >= 1.2 THEN 'MARGINAL'
    ELSE 'PASS'
  END
FROM fin;
$$;

-- Smoke test (a rectangle over central Dallas) — expect one row with a
-- sensible covered/uncovered split:
-- SELECT * FROM public.analyze_expansion_roi('{"type":"Polygon","coordinates":[[[-96.90,32.70],[-96.70,32.70],[-96.70,32.86],[-96.90,32.86],[-96.90,32.70]]]}');
