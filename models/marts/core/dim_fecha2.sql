WITH

src_fecha AS (
    SELECT * 
    FROM {{ ref('dim_fecha') }}
    ),


fecha_completa AS (
    SELECT
        date_day
        , day (date_day) as day
        , month (date_day) as month
        , year (date_day) as year
        , quarter (date_day) as QUARTER
        
    FROM src_fecha
    )

SELECT * FROM fecha_completa