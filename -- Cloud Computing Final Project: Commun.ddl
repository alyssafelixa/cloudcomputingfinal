-- Cloud Computing Final Project: Community Amenities in Philadelphia
---Big Query Code
CREATE TABLE turnkey-aleph-422118.my_dataset.nearesttt AS
WITH philadelphia AS (
    SELECT * 
    FROM `bigquery-public-data.geo_us_census_places.places_pennsylvania` 
    WHERE place_name = 'Philadelphia'
),
amenities AS (
    SELECT *, 
           (
               SELECT tags.value 
               FROM UNNEST(all_tags) AS tags 
               WHERE tags.key = 'amenity'
           ) AS amenity
    FROM `bigquery-public-data.geo_openstreetmap.planet_features_points` AS features
    CROSS JOIN philadelphia
    WHERE ST_CONTAINS(philadelphia.place_geom, features.geometry)
      AND (
          EXISTS (SELECT 1 FROM UNNEST(all_tags) AS tags WHERE tags.key = 'amenity' AND tags.value IN ('library', 'place_of_worship', 'community_centre'))
      )
),
joiin AS (
SELECT 
  a1.*, 
  a2.osm_id AS nearest_osm_id, 
  ST_DISTANCE(a1.geometry, a2.geometry) AS distance, 
  ROW_NUMBER() OVER (PARTITION BY a1.osm_id ORDER BY ST_Distance(a1.geometry, a2.geometry)) AS row_num
FROM amenities a1
CROSS JOIN amenities a2
WHERE a1.osm_id <> a2.osm_id -- Exclude self-joins
ORDER BY a1.osm_id, distance
) 
SELECT *
FROM joiin  
WHERE row_num = 1;

--Carto Code 
WITH clustered_points AS (
    SELECT
        `carto-un`.carto.ST_CLUSTERKMEANS(ARRAY_AGG(geometry IGNORE NULLS), 6) AS cluster_arr
    FROM `turnkey-aleph-422118.my_dataset.nearesttt`
),

unioned_clusters AS (
    SELECT
        cluster_element.cluster,
        CAST(cluster_element.cluster AS STRING) AS cluster_str,
        ST_UNION_AGG(cluster_element.geom) AS geom
    FROM clustered_points, UNNEST(cluster_arr) AS cluster_element
    GROUP BY cluster_element.cluster
),

extracted_points AS (
    SELECT
        cluster,
        cluster_str,
        point_geom
    FROM unioned_clusters,
        UNNEST(ST_DUMP(geom)) AS point_geom
)

SELECT
    e.cluster,
    e.cluster_str,
    e.point_geom AS geom,
    t.osm_id,
    t.place_name,
    t.amenity,
    t.distance
FROM extracted_points e
JOIN `turnkey-aleph-422118.my_dataset.nearesttt` t
ON ST_WITHIN(e.point_geom, t.geometry); 