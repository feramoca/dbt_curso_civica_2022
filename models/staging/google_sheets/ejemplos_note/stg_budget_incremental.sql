
{{ config(
    materialized='incremental',
    unique_key = '_row'
    ) 
    }}


WITH stg_budget_products AS (
    SELECT * 
    FROM {{ source('google_sheets','budget_fro') }}
    ),

renamed_casted AS (
    SELECT
          _row
        , month
        , quantity 
        , _fivetran_synced
    FROM stg_budget_products
    )

SELECT * FROM renamed_casted

{% if is_incremental() %}

  where _fivetran_synced > (select max(_fivetran_synced) from {{ this }})

{% endif %}