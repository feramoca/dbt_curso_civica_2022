WITH

stg_users AS (
    SELECT * 
    FROM {{ ref('stg_sql_server_dbo_users') }}
    ),

dim_fecha as (
    select *
    from {{ ref('dim_fecha')}}
),

dim_usuario AS (
    SELECT
          user_id
        , phone_number
        , first_name
        , last_name
        --, cast (created_at as date) as created_at
        , fechacreacion.id_date as created_at
        , address_id
        --, cast (updated_at as date) as updated_at
        , fechaactualizacion.id_date as updated_at        
        , email
    FROM stg_users
    join dim_fecha fechacreacion on fechacreacion.fecha = cast (stg_users.created_at as date)
    join dim_fecha fechaactualizacion on fechaactualizacion.fecha = cast (stg_users.updated_at as date)    
    )

SELECT * FROM dim_usuario
