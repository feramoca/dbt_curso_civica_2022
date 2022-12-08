WITH src_budget AS (
    SELECT * 
    FROM {{ source('google_sheets', 'budget') }}
    ),

budget AS (
    SELECT
          _row
        , product_id
        , quantity
        , month
        
    FROM src_budget
    )

SELECT * FROM budget