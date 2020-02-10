-- Start Postgres app or server

pgcli -p5432 "postgres"

-- Line below can be duplicated by just double clicking on the server in the Postgres.app
/Applications/Postgres.app/Contents/Versions/10/bin/psql -p5432 "postgres"

-- You may need to add a PostGIS extention to your Postgres DB
CREATE EXTENSION postgis;



/**********************
 PART ONE: IMPORTING DATA
**********************/

-- Uploading a shapefile to the database
-- Using shp2pgsql (part of PostGIS install)
shp2pgsql -s 4326 ~/Projects/IntroToPostGIS/data/us_counties/countyp010g public.counties | psql -h postgres -d postgres -U postgres

-- Using psql \copy (with no geom represented as text)

CREATE TABLE public.ethanol_plants (
  id serial,
  long numeric,
  lat numeric,
  name text,
  address text,
  address_state text,
  capacity numeric
);

\copy public.ethanol_plants FROM '~/Projects/IntroToPostGIS/data/ethanol_plants.csv' WITH CSV HEADER;

-- Uploading regular old tabular data
CREATE TABLE usda_corn_stats (
  id serial,
  crop text,
  variable text,
  state text,
  county text,
  fips_code text,
  year integer,
  value numeric
);

-- Note: we do not include id as a column to upload and the serial type will automatically populate a unique ID
\copy usda_corn_stats (crop, variable, state, county, fips_code, year, value) FROM '~/Projects/IntroToPostGIS/data/usda_corn_stats.csv' WITH CSV HEADER;

-- dealing with unexpected data types
ALTER TABLE usda_corn_stats ALTER COLUMN value TYPE text;

-- upload again now that value accepts text
\copy usda_corn_stats (crop, variable, state, county, fips_code, year, value) FROM '~/Projects/IntroToPostGIS/data/usda_corn_stats.csv' WITH CSV HEADER;

-- Remove commas and try to cast as numeric
UPDATE usda_corn_stats SET value = replace(value, ',', '');
SELECT value::numeric FROM usda_corn_stats;

-- Now change column type back to numeric
ALTER TABLE usda_corn_stats ALTER COLUMN value TYPE numeric USING value::numeric;

-- adding a geometry column to the table
SELECT AddGeometryColumn('public', 'ethanol_plants', 'geom', 4326, 'POINT', 2);
UPDATE ethanol_plants SET geom = ST_SetSRID(ST_MakePoint(long, lat), 4326);

/**********************
 PART TWO: ANALYSIS
**********************/

-- QUESTION 1:  What are the top three corn producing states each year?

-- Format data into wide view and add production
CREATE TABLE usda_corn_wide AS
SELECT a.fips_code, a.state, a.county, a.year, a.value AS area_harvested, b.value AS yield, a.value * b.value AS production
FROM usda_corn_stats a INNER JOIN usda_corn_stats b ON (a.year = b.year AND a.fips_code = b.fips_code)
WHERE a.variable = 'AREA HARVESTED' AND b.variable = 'YIELD' AND a.fips_code NOT LIKE '%998' ORDER BY 1,2;

WITH state_agg AS (SELECT state, year, sum(production) as production FROM usda_corn_wide GROUP BY 1,2),
ranked AS (SELECT *, rank() OVER (PARTITION BY year ORDER BY production DESC) as rank FROM state_agg)
SELECT * FROM ranked WHERE rank <= 3;

-- QUESTION 2: How many ethanol plants are in the top 10 corn producing counties?

-- First select the top 10 counties averaged across the last 2 years
SELECT fips_code, county, state, avg(production) as production
FROM usda_corn_wide WHERE year in (2017,2018) GROUP BY 1,2,3 ORDER BY production DESC LIMIT 10;

-- select the geometry table from our shape table filtering using our production query above
WITH top_ten AS (SELECT fips_code, county, state, avg(production) as production
FROM usda_corn_wide WHERE year in (2017,2018) GROUP BY 1,2,3 ORDER BY production DESC LIMIT 10)
SELECT * FROM counties WHERE admin_fips IN (SELECT fips_code FROM top_ten);

-- next we can intersect the ethanol plants with our counties layer
WITH top_ten AS (SELECT fips_code, county, state, avg(production) as production
FROM usda_corn_wide WHERE year in (2017,2018) GROUP BY 1,2,3 ORDER BY production DESC LIMIT 10),
top_ten_counties AS (SELECT * FROM counties WHERE admin_fips IN (SELECT fips_code FROM top_ten))
SELECT ethanol_plants.* FROM ethanol_plants, top_ten_counties WHERE ST_Intersects(ethanol_plants.geom, top_ten_counties.geom)

-- QUESTION 3: What is the total capacity of all ethanol plants within 200 miles of the center of the highest corn producing county?
CREATE VIEW top_ethanol_100mi AS
WITH top_county AS (SELECT fips_code, county, state, avg(production) as production
FROM usda_corn_wide WHERE year in (2017,2018) GROUP BY 1,2,3 ORDER BY production DESC LIMIT 1),
top_cnty_geom AS (SELECT a.* FROM counties a, top_county b WHERE a.admin_fips = b.fips_code),
buffer AS (SELECT ST_Buffer(ST_Centroid(geom)::geography, 1609 * 100) as buffer_geom FROM top_cnty_geom)
SELECT a.* FROM ethanol_plants a, buffer b WHERE ST_Intersects(a.geom, b.buffer_geom);

-- select actual capacity from the view
SELECT count(*), sum(capacity) FROM top_ethanol_100mi

-- QUESTION 4: Which ethanol plants have another ethanol plant within 20 miles?
SELECT a.name, count(b.*) - 1 as count FROM ethanol_plants a, ethanol_plants b
WHERE ST_Intersects(ST_Buffer(ST_Centroid(a.geom)::geography, 1609 * 20), b.geom)
GROUP BY 1 ORDER BY 2 DESC

-- QUESTION 5: How many acres of corn is within the average catchment area of an ethanol plant (simplify to county centroids within 50 miles)?

CREATE TABLE corn_acreage AS
SELECT fips_code, state, county, avg(area_harvested) as area_harvested
FROM usda_corn_wide WHERE year IN (2017,2018) GROUP BY 1,2,3;

-- First generate a set of centroids for the counties
CREATE TABLE us_acreage_county_centroids AS
SELECT a.admin_fips, a.admin_name as county, a.state, b.area_harvested, ST_Centroid(a.geom) AS geom
FROM counties a JOIN corn_acreage b ON (a.admin_fips = b.fips_code);

-- Intersect all points with buffers from the plants
SELECT a.name, avg(area_harvested) as avg_acreage
FROM ethanol_plants a, us_acreage_county_centroids b
WHERE ST_Within(b.geom, ST_Buffer(ST_Centroid(a.geom)::geography, 1609 * 50)::geometry)
GROUP BY 1 ORDER BY 2 DESC


/*************************
 PART THREE: VISUALIZATION
*************************/

-- Map of corn yields by county
CREATE VIEW corn_stats_2018 AS
SELECT a.admin_fips AS fips_code, b.area_harvested, b.yield, b.production, a.geom
FROM counties a, usda_corn_wide b
WHERE a.admin_fips = b.fips_code AND b.year = 2018

-- Change in area harvested by county from 2016-2018
CREATE VIEW area_harvest_delta AS
SELECT a.fips_code, a.area_harvested - b.area_harvested AS change_area_harvested, c.geom
FROM usda_corn_wide a
LEFT JOIN usda_corn_wide b ON (a.fips_code = b.fips_code)
LEFT JOIN counties c ON (a.fips_code = c.admin_fips)
WHERE a.year = 2018 AND b.year = 2016

-- Capacity of plants and production by county


/*************************
 PART FOUR: EXPORTING DATA
*************************/

-- dump as lat long
\copy (SELECT admin_fips as fips_code, area_harvested, ST_X(geom) as long, ST_Y(geom) as lat FROM us_acreage_county_centroids) TO '~/Downloads/us_acreage_points.csv' CSV HEADER;

-- dump as WKT
\copy (SELECT admin_fips as fips_code, area_harvested, ST_AsText(geom) as wkt FROM us_acreage_county_centroids) TO '~/Downloads/us_acreage_wkt.csv' CSV HEADER;

-- export to shp
pgsql2shp -f ~/Downloads/us_acreage_shp -h localhost -u postgres postgres 'SELECT * FROM us_acreage_county_centroids'
