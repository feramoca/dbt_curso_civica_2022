WITH


src_ace AS (
    SELECT * 
    FROM {{ ref('stg_seeds_acs') }}
    ), 



src_pedido_agregado AS (
    SELECT * 
    FROM {{ ref('int_pedido_agregado') }}
    ), 

comp_shipping_service  AS (
    SELECT
          order_id 
        , src_ace.state
        , ((fijo) + (variable * Total_Unidades_Pedido)) as euros
        , shipping_cost        
        , Total_Unidades_Pedido
        , fijo
        , variable
    FROM src_pedido_agregado
    join src_ace on src_ace.state_id = src_pedido_agregado.state_id
    )

SELECT * FROM comp_shipping_service