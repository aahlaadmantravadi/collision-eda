{{ config(materialized='table') }}

{% set years = range(2012, 2024) %}

WITH crashes AS (
    {% for year in years %}
    SELECT collision_id, date_part('year', crash_date) as "year", borough, contr_f_vhc_1 FROM {{ source('public', 'MVC_C_' ~ year) }}
    {% if not loop.last %}UNION ALL{% endif %}
    {% endfor %}
),
vehicles AS (
    {% for year in years %}
    SELECT collision_id, date_part('year', crash_date) as "year", vhc_type, vhc_year, dr_lic_status FROM {{ source('public', 'MVC_V_' ~ year) }}
    {% if not loop.last %}UNION ALL{% endif %}
    {% endfor %}
),
persons AS (
    {% for year in years %}
    SELECT collision_id, date_part('year', crash_date) as "year", age, sex FROM {{ source('public', 'MVC_P_' ~ year) }}
    {% if not loop.last %}UNION ALL{% endif %}
    {% endfor %}
),
yearly_stats AS (
    SELECT
        "year",
        COUNT(DISTINCT collision_id) AS total_crashes_am,
        COUNT(collision_id) AS total_records_c,
        SUM(CASE WHEN borough IS NULL THEN 1 ELSE 0 END) AS no_borough_data,
        SUM(CASE WHEN contr_f_vhc_1 IS NULL THEN 1 ELSE 0 END) AS no_contr_f_data
    FROM crashes
    GROUP BY "year"
),
vehicle_stats AS (
    SELECT
        "year",
        COUNT(DISTINCT collision_id) AS crashes_with_vehicles_am,
        COUNT(collision_id) AS total_vhc_am,
        SUM(CASE WHEN vhc_type IS NULL THEN 1 ELSE 0 END) AS no_vhc_type_data
    FROM vehicles
    GROUP BY "year"
),
person_stats AS (
    SELECT
        "year",
        COUNT(DISTINCT collision_id) AS crashes_with_persons_am,
        COUNT(collision_id) AS total_person_am,
        SUM(CASE WHEN age IS NULL THEN 1 ELSE 0 END) AS no_age_data,
        SUM(CASE WHEN sex IS NULL OR sex = 'U' THEN 1 ELSE 0 END) AS no_sex_data
    FROM persons
    GROUP BY "year"
)
SELECT
    y."year",
    y.total_crashes_am,
    COALESCE(v.total_vhc_am, 0) as total_vhc_am,
    COALESCE(p.total_person_am, 0) as total_person_am,
    (y.total_crashes_am - COALESCE(v.crashes_with_vehicles_am, 0)) AS no_vhc_data_am,
    ROUND(100.0 * (y.total_crashes_am - COALESCE(v.crashes_with_vehicles_am, 0)) / y.total_crashes_am, 2) AS "no_vhc_data_%",
    (y.total_crashes_am - COALESCE(p.crashes_with_persons_am, 0)) AS no_person_data_am,
    ROUND(100.0 * (y.total_crashes_am - COALESCE(p.crashes_with_persons_am, 0)) / y.total_crashes_am, 2) AS "no_person_data_%",
    y.no_borough_data,
    ROUND(100.0 * y.no_borough_data / y.total_records_c, 2) AS "no_borough_data_%",
    y.no_contr_f_data,
    ROUND(100.0 * y.no_contr_f_data / y.total_records_c, 2) AS "no_contr_f_data_%",
    COALESCE(v.no_vhc_type_data, 0) AS no_vhc_type_data,
    ROUND(CASE WHEN v.total_vhc_am > 0 THEN 100.0 * v.no_vhc_type_data / v.total_vhc_am ELSE 0 END, 2) AS "no_vhc_type_data_%",
    COALESCE(p.no_age_data, 0) AS no_age_data,
    ROUND(CASE WHEN p.total_person_am > 0 THEN 100.0 * p.no_age_data / p.total_person_am ELSE 0 END, 2) AS "no_age_data_%",
    COALESCE(p.no_sex_data, 0) AS no_sex_data,
    ROUND(CASE WHEN p.total_person_am > 0 THEN 100.0 * p.no_sex_data / p.total_person_am ELSE 0 END, 2) AS "no_sex_data_%"
FROM yearly_stats y
LEFT JOIN vehicle_stats v ON y."year" = v."year"
LEFT JOIN person_stats p ON y."year" = p."year"
ORDER BY y."year"