{{ config(materialized='table') }}

{% set years = range(2012, 2024) %}

WITH all_data AS (
    {% for year in years %}
    (SELECT 
        {{ year }} AS "year",
        injured, 
        COUNT(*) AS inj_am,
        NULL AS killed, NULL AS killed_am,
        borough, COUNT(*) AS borough_am,
        vhc_1_code AS vhc_type, COUNT(*) AS vhc_type_am,
        contr_f_vhc_1 AS contr_f, COUNT(*) as contr_f_am
     FROM {{ source('public', 'MVC_C_' ~ year) }}
     GROUP BY injured, borough, vhc_1_code, contr_f_vhc_1)
    {% if not loop.last %}UNION ALL{% endif %}
    {% endfor %}
)
SELECT 
    "year",
    injured, inj_am,
    killed, killed_am,
    borough, borough_am,
    vhc_type, vhc_type_am,
    contr_f, contr_f_am
FROM all_data