{{ config(materialized='table') }}

{% set years = range(2012, 2024) %}

WITH all_crashes_with_locations AS (
    {% for year in years %}
    SELECT
        latitude,
        longitude,
        on_street_name,
        cross_street_name
    FROM {{ source('public', 'MVC_C_' ~ year) }}
    WHERE latitude IS NOT NULL AND longitude IS NOT NULL AND on_street_name IS NOT NULL AND cross_street_name IS NOT NULL
    {% if not loop.last %}UNION ALL{% endif %}
    {% endfor %}
),

intersection_counts AS (
    SELECT
        -- Using street names is more reliable than coordinate clustering for this dataset
        on_street_name,
        cross_street_name,
        COUNT(*) as number_of_crashes
    FROM all_crashes_with_locations
    GROUP BY on_street_name, cross_street_name
)

SELECT
    on_street_name AS primary_street,
    cross_street_name AS secondary_street,
    number_of_crashes
FROM intersection_counts
ORDER BY number_of_crashes DESC
LIMIT 10