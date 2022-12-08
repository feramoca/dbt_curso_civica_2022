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
        , cast (((fijo) + (variable * Total_Unidades_Pedido)) as numeric (8,2)) as Porte_ACE
        , shipping_cost as Porte_pagado 
        , case
            when Porte_ACE < Porte_pagado Then 'Si'
            else 'No'
            end as Favorable_ACE
        , Cast ((Porte_ACE - Porte_pagado) as numeric (8,2)) as Ahorro_Porte_USD
        , Total_Unidades_Pedido
        , fijo
        , variable
    FROM src_pedido_agregado
    join src_ace on src_ace.state_id = src_pedido_agregado.state_id
    )

SELECT * FROM comp_shipping_service
