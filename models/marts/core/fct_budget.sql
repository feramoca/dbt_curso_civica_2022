WITH

src_budget AS (
    SELECT * 
    FROM {{ ref('stg_google_sheets_budget') }}
    ),

budget AS (
    SELECT
          product_id
        , quantity
        , month    

    FROM src_budget

    )

SELECT * FROM budget