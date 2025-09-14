{{ config(materialized='table') }}

{% set years = range(2012, 2024) %}

WITH all_crashes AS (
    {% for year in years %}
    SELECT
        crash_date,
        crash_time,
        injured,
        killed
    FROM {{ source('public', 'MVC_C_' ~ year) }}
    {% if not loop.last %}UNION ALL{% endif %}
    {% endfor %}
)
SELECT
    CAST(date_part('year', crash_date) AS INTEGER) AS "year",
    CAST(date_part('month', crash_date) AS INTEGER) AS "month",
    CAST(date_part('hour', crash_time) AS INTEGER) AS "hour",
    CONCAT(
        TO_CHAR(date_part('hour', crash_time), 'fm00'), ':00 - ',
        TO_CHAR(date_part('hour', crash_time), 'fm00'), ':59'
    ) AS time_interval,
    COUNT(*) AS all_amount,
    SUM(COALESCE(injured, 0)) AS injured_am,
    SUM(COALESCE(killed, 0)) AS killed_am
FROM all_crashes
GROUP BY 1, 2, 3, 4
ORDER BY 1, 2, 3