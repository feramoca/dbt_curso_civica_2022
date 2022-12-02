WITH

src_clientes_pedidos AS (
    SELECT * 
    FROM {{ ref('int_clientes_agregados_pedidos') }}
    ),

src_clientes_articulos AS (
    SELECT * 
    FROM {{ ref('int_clientes_agregados_articulos') }}
    ),

fct_users AS (
    SELECT
        src_clientes_pedidos.user_id
        , Numero_Pedidos_Cliente
        , Importe_Total_Cliente
        , Importe_Medio_Pedido_Cliente
        , Importe_Pedido_Max
        , Importe_Pedido_Min
        , Direcciones_Distintas_Cliente
        , Total_Unidades_Compradas_Cliente
        , Articulos_Cliente
        , Articulos_Distintos_Cliente
        , Fecha_ultimo_pedido
        
    FROM src_clientes_pedidos
    join src_clientes_articulos on src_clientes_articulos.user_id = src_clientes_pedidos.user_id
    -- quitar el where, es para hacer pruebas
    --where src_clientes_pedidos.user_id = '93908a68-20b7-44ba-b6c1-6f4cc57d7726'
    )

SELECT * FROM fct_users