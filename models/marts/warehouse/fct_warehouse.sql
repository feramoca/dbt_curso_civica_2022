WITH

src_almacen AS (
    SELECT * 
    FROM {{ ref('stg_sql_server_dbo_products') }}
    ),


fct_warehouse AS (
    SELECT
          product_id
        , inventory
        , price
        , (inventory * price) as Total_Warehouse
        , case 
            when inventory > 75 then 'High'
            when inventory > 50 then 'Optimus'
            else 'Low'
          end as Stock
        
    FROM src_almacen

    )

SELECT * FROM fct_warehouse